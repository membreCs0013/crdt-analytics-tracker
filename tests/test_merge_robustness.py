"""Regression tests for /merge endpoint robustness (Phase 0 Check C).

Covers: non-JSON body, non-dict body, missing sender/state, negative counts,
float counts, bool counts, numeric strings, int > 2^53, empty sender,
oversized sender (>64 chars), unknown metric (whitelist enforcement),
oversized body (413), and state-unchanged invariant after all rejections.
"""

from __future__ import annotations

import shutil
import tempfile
from typing import Generator

import pytest
from flask.testing import FlaskClient

from node.app import create_app
from node.config import Config


@pytest.fixture
def client() -> Generator[FlaskClient, None, None]:
    """Provide Flask test client for a node with page_views and clicks metrics."""
    tmp = tempfile.mkdtemp(prefix="crdt_merge_rob_")
    cfg = Config(
        node_id="robustness-node",
        port=5099,
        host="127.0.0.1",
        peers={},
        gossip_interval=1.0,
        fanout=1,
        data_dir=tmp,
        metrics=["page_views", "clicks"],
        request_timeout=1.0,
    )
    app = create_app(cfg)
    try:
        yield app.test_client()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _get_totals(client: FlaskClient) -> dict:
    return client.get("/state").json["totals"]


class TestMergeRobustness:
    """All bad /merge payloads must return 4xx and leave state unchanged."""

    def test_non_json_body(self, client: FlaskClient) -> None:
        resp = client.post("/merge", data=b"not-json", content_type="text/plain")
        assert resp.status_code == 400

    def test_non_dict_json_body(self, client: FlaskClient) -> None:
        resp = client.post("/merge", json=[1, 2, 3])
        assert resp.status_code == 400

    def test_missing_sender(self, client: FlaskClient) -> None:
        resp = client.post("/merge", json={"state": {"page_views": {"p1": 1}}})
        assert resp.status_code == 400

    def test_missing_state(self, client: FlaskClient) -> None:
        resp = client.post("/merge", json={"sender": "peer1"})
        assert resp.status_code == 400

    def test_negative_counts(self, client: FlaskClient) -> None:
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": -5}}},
        )
        assert resp.status_code == 400

    def test_float_counts(self, client: FlaskClient) -> None:
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": 1.5}}},
        )
        assert resp.status_code == 400

    def test_bool_counts(self, client: FlaskClient) -> None:
        # In Python, bool is a subclass of int — must be explicitly rejected
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": True}}},
        )
        assert resp.status_code == 400

    def test_numeric_string_counts(self, client: FlaskClient) -> None:
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": "100"}}},
        )
        assert resp.status_code == 400

    def test_int_exceeds_2_53(self, client: FlaskClient) -> None:
        big = 2 ** 53 + 1
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": big}}},
        )
        assert resp.status_code == 400

    def test_empty_sender(self, client: FlaskClient) -> None:
        resp = client.post(
            "/merge",
            json={"sender": "", "state": {"page_views": {"peer1": 1}}},
        )
        assert resp.status_code == 400

    def test_oversized_sender(self, client: FlaskClient) -> None:
        """sender node_id longer than 64 characters must be rejected with 400."""
        long_id = "x" * 65
        resp = client.post(
            "/merge",
            json={"sender": long_id, "state": {"page_views": {long_id: 1}}},
        )
        assert resp.status_code == 400

    def test_exactly_64_char_sender_accepted(self, client: FlaskClient) -> None:
        """sender node_id of exactly 64 characters must be accepted."""
        ok_id = "a" * 64
        resp = client.post(
            "/merge",
            json={"sender": ok_id, "state": {"page_views": {ok_id: 1}}},
        )
        assert resp.status_code == 200

    def test_unknown_metric_rejected(self, client: FlaskClient) -> None:
        """Metrics NOT in the whitelist must be rejected (rogue peer protection)."""
        resp = client.post(
            "/merge",
            json={
                "sender": "rogue-peer",
                "state": {"evil_metric": {"rogue-peer": 999}},
            },
        )
        assert resp.status_code == 400
        assert "whitelist" in resp.json["error"].lower() or "unknown" in resp.json["error"].lower()

    def test_known_plus_unknown_metric_rejected(self, client: FlaskClient) -> None:
        """Even if one metric is valid, an unknown one in the same payload must be rejected."""
        resp = client.post(
            "/merge",
            json={
                "sender": "rogue-peer",
                "state": {
                    "page_views": {"rogue-peer": 1},
                    "evil_metric": {"rogue-peer": 999},
                },
            },
        )
        assert resp.status_code == 400

    def test_oversized_body_returns_413(self, client: FlaskClient) -> None:
        """Bodies over MAX_CONTENT_LENGTH (1 MB) must return 413."""
        # Send 1 MB + 1 byte of raw data
        big_body = b"x" * (1024 * 1024 + 1)
        resp = client.post(
            "/merge",
            data=big_body,
            content_type="application/json",
        )
        assert resp.status_code == 413

    def test_state_unchanged_after_all_rejections(self, client: FlaskClient) -> None:
        """State must remain unchanged after a sequence of rejected /merge requests."""
        # Establish known state
        client.post("/visit?metric=page_views&n=77")
        baseline_totals = _get_totals(client)
        assert baseline_totals["page_views"] == 77

        bad_payloads = [
            (b"garbage", "text/plain", None),  # non-JSON
        ]
        # Non-JSON
        client.post("/merge", data=b"garbage", content_type="text/plain")
        # Non-dict
        client.post("/merge", json=[1, 2, 3])
        # Missing fields
        client.post("/merge", json={"state": {"page_views": {"p": 1}}})
        client.post("/merge", json={"sender": "peer1"})
        # Bad values
        client.post("/merge", json={"sender": "p1", "state": {"page_views": {"p1": -1}}})
        client.post("/merge", json={"sender": "p1", "state": {"page_views": {"p1": 1.5}}})
        client.post("/merge", json={"sender": "p1", "state": {"page_views": {"p1": True}}})
        client.post("/merge", json={"sender": "p1", "state": {"page_views": {"p1": "100"}}})
        client.post("/merge", json={"sender": "p1", "state": {"page_views": {"p1": 2**53 + 1}}})
        # Bad sender
        client.post("/merge", json={"sender": "", "state": {}})
        client.post("/merge", json={"sender": "x" * 65, "state": {}})
        # Unknown metric
        client.post("/merge", json={"sender": "p1", "state": {"evil": {"p1": 1}}})

        # State must still be the baseline
        assert _get_totals(client) == baseline_totals
