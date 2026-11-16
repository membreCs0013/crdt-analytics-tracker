"""Unit and API tests for node Flask server using the Flask test client."""

import hashlib
import json
import shutil
import tempfile
from typing import Generator

import pytest
from flask.testing import FlaskClient

from node.app import create_app
from node.config import Config


@pytest.fixture
def temp_data_dir() -> Generator[str, None, None]:
    """Provide isolated temporary directory for state persistence."""
    tmp = tempfile.mkdtemp(prefix="crdt_node_test_")
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def client(temp_data_dir: str) -> FlaskClient:
    """Provide Flask test client for a standalone node."""
    cfg = Config(
        node_id="node-test-1",
        port=5001,
        host="127.0.0.1",
        peers={"node-peer": "http://127.0.0.1:5002"},
        gossip_interval=1.0,
        fanout=2,
        data_dir=temp_data_dir,
        metrics=["page_views", "clicks"],
        request_timeout=1.0,
    )
    app = create_app(cfg)
    # Background threads not started in API tests to avoid unmanaged threads
    return app.test_client()


class TestNodeAPI:
    """Test suite for HTTP API endpoints and validations."""

    def test_health_check(self, client: FlaskClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json == {"status": "ok", "node": "node-test-1"}
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"

    def test_visit_validation(self, client: FlaskClient) -> None:
        # 1. Valid visit with query parameters
        resp = client.post("/visit?metric=page_views&n=5")
        assert resp.status_code == 200
        assert resp.json == {
            "node": "node-test-1",
            "metric": "page_views",
            "local_value": 5,
            "global_value": 5,
        }

        # 2. Cumulative visit
        resp = client.post("/visit?metric=page_views&n=10")
        assert resp.status_code == 200
        assert resp.json["local_value"] == 15
        assert resp.json["global_value"] == 15

        # 3. Whitelist enforcement: unlisted metric rejected with 400
        resp = client.post("/visit?metric=unlisted_metric&n=1")
        assert resp.status_code == 400
        assert "Invalid metric" in resp.json["error"]

        # 4. Out of bounds or invalid n
        assert client.post("/visit?metric=page_views&n=0").status_code == 400
        assert client.post("/visit?metric=page_views&n=-5").status_code == 400
        assert client.post("/visit?metric=page_views&n=1000001").status_code == 400
        assert client.post("/visit?metric=page_views&n=not_an_int").status_code == 400

        # 5. JSON body fallback for /visit
        resp = client.post("/visit", json={"metric": "clicks", "n": 3})
        assert resp.status_code == 200
        assert resp.json["metric"] == "clicks"
        assert resp.json["local_value"] == 3

    def test_state_shape_and_hash_stability(self, client: FlaskClient) -> None:
        # Initially empty
        resp = client.get("/state")
        assert resp.status_code == 200
        data = resp.json
        assert data["node"] == "node-test-1"
        assert "per_node" in data
        assert "totals" in data
        assert "state_hash" in data
        assert "timestamp" in data
        assert data["blocked"] == []

        hash1 = data["state_hash"]

        # Canonical hash check: sha256 of sorted state dict
        # State dict has all whitelisted metrics
        expected_canonical = hashlib.sha256(
            json.dumps({"clicks": {}, "page_views": {}}, sort_keys=True).encode("utf-8")
        ).hexdigest()
        assert hash1 == expected_canonical

        # After increment, hash must change deterministically
        client.post("/visit?metric=page_views&n=7")
        resp2 = client.get("/state")
        hash2 = resp2.json["state_hash"]
        assert hash2 != hash1

        expected_state_after = {"clicks": {}, "page_views": {"node-test-1": 7}}
        expected_canonical_after = hashlib.sha256(
            json.dumps(expected_state_after, sort_keys=True).encode("utf-8")
        ).hexdigest()
        assert hash2 == expected_canonical_after

    def test_merge_validation_and_push_pull(self, client: FlaskClient) -> None:
        # 1. Non-JSON or malformed payload -> 400
        resp = client.post("/merge", data="not-json", content_type="text/plain")
        assert resp.status_code == 400

        resp = client.post("/merge", json={"sender": "peer1"})  # Missing state
        assert resp.status_code == 400

        resp = client.post("/merge", json={"state": {}})  # Missing sender
        assert resp.status_code == 400

        # 2. Corrupt CRDT state -> 400
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": "not-a-dict"}},
        )
        assert resp.status_code == 400

        # 3. Valid merge -> 200 and returns local state (push-pull)
        client.post("/visit?metric=page_views&n=12")
        merge_payload = {
            "sender": "peer1",
            "state": {
                "page_views": {"peer1": 30},
                "clicks": {"peer1": 5},
            },
        }
        resp = client.post("/merge", json=merge_payload)
        assert resp.status_code == 200
        assert resp.json["node"] == "node-test-1"
        assert resp.json["state"]["page_views"] == {"node-test-1": 12, "peer1": 30}

        # Check /state reflects merged value
        state = client.get("/state").json
        assert state["totals"]["page_views"] == 42
        assert state["totals"]["clicks"] == 5

    def test_partition_block_and_heal(self, client: FlaskClient) -> None:
        # Block peer1
        resp = client.post("/partition", json={"blocked": ["peer1"]})
        assert resp.status_code == 200
        assert resp.json == {"blocked": ["peer1"]}

        # /state must reflect blocked peer
        state = client.get("/state").json
        assert state["blocked"] == ["peer1"]

        # Merge from blocked peer must return 503
        resp = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": 50}}},
        )
        assert resp.status_code == 503
        assert "blocked" in resp.json["error"]

        # Merge from non-blocked peer still accepted
        resp = client.post(
            "/merge",
            json={"sender": "peer2", "state": {"page_views": {"peer2": 50}}},
        )
        assert resp.status_code == 200

        # Heal partition
        del_resp = client.delete("/partition")
        assert del_resp.status_code == 200
        assert del_resp.json == {"blocked": []}

        # Now peer1 merge is accepted
        resp_healed = client.post(
            "/merge",
            json={"sender": "peer1", "state": {"page_views": {"peer1": 50}}},
        )
        assert resp_healed.status_code == 200

    def test_wildcard_partition(self, client: FlaskClient) -> None:
        client.post("/partition", json={"blocked": ["*"]})
        resp = client.post(
            "/merge",
            json={"sender": "any-peer", "state": {"page_views": {"any-peer": 10}}},
        )
        assert resp.status_code == 503

    def test_cors_preflight_and_headers(self, client: FlaskClient) -> None:
        # OPTIONS preflight
        resp = client.open("/visit", method="OPTIONS")
        assert resp.status_code == 204
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"
        assert "POST" in resp.headers.get("Access-Control-Allow-Methods", "")

        # GET request CORS header
        resp = client.get("/state")
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"

    def test_metrics_telemetry(self, client: FlaskClient) -> None:
        resp = client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json
        for field_name in ["sent", "received", "bytes_sent", "bytes_received", "failures", "merges_changed", "last_sync", "uptime", "peers"]:
            assert field_name in data
