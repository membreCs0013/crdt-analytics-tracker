"""tests/test_dashboard_api.py

Backend tests for the dashboard Flask app.
No Docker required: a custom CLUSTER_FETCHER is injected via app.config.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helpers to build fake cluster snapshots
# ---------------------------------------------------------------------------

FAKE_COMPOSE = Path(__file__).parent.parent / "docker-compose.yml"

METRICS_WHITELIST = {"page_views", "clicks"}


def _make_node(
    node_id: str,
    port: int,
    pv: int = 0,
    clicks: int = 0,
    state_hash: str = "aabbccdd",
    status: str = "up",
    blocked: list | None = None,
    source: str = "http",
) -> dict:
    """Build a fake node state dict as returned by _fetch_node."""
    per_node = {}
    if pv:
        per_node["page_views"] = {node_id: pv}
    if clicks:
        per_node["clicks"] = {node_id: clicks}
    return {
        "id": node_id,
        "port": port,
        "status": status,
        "source": source,
        "per_node": per_node,
        "totals": {
            **({"page_views": pv} if pv else {}),
            **({"clicks": clicks} if clicks else {}),
        },
        "blocked": blocked or [],
        "state_hash": state_hash,
    }


def _make_cluster(nodes: list[dict], converged: bool | None = None) -> dict:
    from dashboard.server import _crdt_merge_global

    reachable = [n for n in nodes if n["status"] != "down"]
    global_totals = _crdt_merge_global(reachable)
    hashes = [n["state_hash"] for n in reachable if n["state_hash"]]
    if converged is None:
        converged = len(hashes) > 1 and len(set(hashes)) == 1

    divergence = {}
    for metric in ["page_views", "clicks"]:
        vals = [n["totals"].get(metric, 0) for n in reachable]
        divergence[metric] = (max(vals) - min(vals)) if len(vals) >= 2 else 0

    return {
        "ts": time.time(),
        "nodes": nodes,
        "converged": converged,
        "global": global_totals,
        "divergence": divergence,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def app(tmp_path):
    """Create a test app instance with a minimal compose file."""
    from dashboard.server import create_app

    # Write a minimal 4-node compose file so _load_compose() returns nodes
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(
        "\n".join([
            "services:",
            "  node1:",
            "    ports:",
            "      - '5001:5000'",
            "  node2:",
            "    ports:",
            "      - '5002:5000'",
            "  node3:",
            "    ports:",
            "      - '5003:5000'",
            "  node4:",
            "    ports:",
            "      - '5004:5000'",
        ]),
        encoding="utf-8",
    )

    application = create_app(compose_path=compose)
    application.config["TESTING"] = True
    return application


@pytest.fixture()
def client(app):
    return app.test_client()


def _set_fetcher(app, nodes: list[dict], converged: bool | None = None):
    """Install a fake fetcher that returns a fixed cluster snapshot."""
    snapshot = _make_cluster(nodes, converged)

    def fetcher(services):
        return snapshot

    app.config["CLUSTER_FETCHER"] = fetcher


# ---------------------------------------------------------------------------
# Tests: /api/cluster — basic shape
# ---------------------------------------------------------------------------

class TestClusterEndpoint:
    def test_cluster_returns_expected_shape(self, app, client):
        nodes = [
            _make_node("node1", 5001, pv=10, state_hash="aabb"),
            _make_node("node2", 5002, pv=10, state_hash="aabb"),
        ]
        _set_fetcher(app, nodes)
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "nodes" in data
        assert "converged" in data
        assert "global" in data
        assert "divergence" in data
        assert "ts" in data
        assert "last_convergence_seconds" in data

    def test_converged_when_all_hashes_match(self, app, client):
        nodes = [
            _make_node("node1", 5001, pv=5, state_hash="deadbeef"),
            _make_node("node2", 5002, pv=5, state_hash="deadbeef"),
            _make_node("node3", 5003, pv=5, state_hash="deadbeef"),
        ]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        assert data["converged"] is True

    def test_diverged_when_hashes_differ(self, app, client):
        nodes = [
            _make_node("node1", 5001, pv=5, state_hash="aaaa"),
            _make_node("node2", 5002, pv=10, state_hash="bbbb"),
        ]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        assert data["converged"] is False

    def test_down_node_does_not_break_cluster(self, app, client):
        nodes = [
            _make_node("node1", 5001, pv=7, state_hash="cccc"),
            _make_node("node2", 5002, pv=7, state_hash="cccc"),
            {
                "id": "node3", "port": 5003, "status": "down",
                "source": "none", "per_node": {}, "totals": {},
                "blocked": [], "state_hash": "", "error": "unreachable",
            },
        ]
        _set_fetcher(app, nodes)
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        data = resp.get_json()
        # Down node should appear in nodes list
        statuses = {n["id"]: n["status"] for n in data["nodes"]}
        assert statuses["node3"] == "down"
        # Should still report convergence for the two reachable nodes
        assert data["converged"] is True


# ---------------------------------------------------------------------------
# Tests: global total must be CRDT merge, not sum of node totals
# ---------------------------------------------------------------------------

class TestGlobalTotalCRDTMerge:
    def test_global_is_merge_not_sum(self, app, client):
        """
        After convergence, each node has the full state including contributions
        from other nodes. Summing node totals would over-count.

        node1 has seen: {node1: 5, node2: 3}  -> total=8
        node2 has seen: {node1: 5, node2: 3}  -> total=8

        Naive sum of totals = 16  (WRONG)
        CRDT merge = max per node_id -> {node1:5, node2:3} -> sum = 8  (CORRECT)
        """
        nodes = [
            {
                "id": "node1", "port": 5001, "status": "up", "source": "http",
                "per_node": {"page_views": {"node1": 5, "node2": 3}},
                "totals": {"page_views": 8},
                "blocked": [], "state_hash": "same",
            },
            {
                "id": "node2", "port": 5002, "status": "up", "source": "http",
                "per_node": {"page_views": {"node1": 5, "node2": 3}},
                "totals": {"page_views": 8},
                "blocked": [], "state_hash": "same",
            },
        ]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        # Correct answer is 8, not 16
        assert data["global"].get("page_views") == 8

    def test_global_merge_during_partition(self, app, client):
        """
        During a partition node1 advanced to 10, node2 to 7.
        Each only knows its own contribution.
        CRDT merge = {node1:10, node2:7} -> 17
        """
        nodes = [
            {
                "id": "node1", "port": 5001, "status": "partitioned", "source": "http",
                "per_node": {"page_views": {"node1": 10}},
                "totals": {"page_views": 10},
                "blocked": ["node2"], "state_hash": "h1",
            },
            {
                "id": "node2", "port": 5002, "status": "partitioned", "source": "http",
                "per_node": {"page_views": {"node2": 7}},
                "totals": {"page_views": 7},
                "blocked": ["node1"], "state_hash": "h2",
            },
        ]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        assert data["global"].get("page_views") == 17

    def test_global_takes_max_not_sum_per_node_id(self, app, client):
        """
        node1 thinks node2 contributed 3 (stale), node2 thinks it contributed 7.
        CRDT merge picks max(3,7)=7 for node2 slot, plus node1's 5.
        Result: 12.
        """
        nodes = [
            {
                "id": "node1", "port": 5001, "status": "up", "source": "http",
                "per_node": {"page_views": {"node1": 5, "node2": 3}},
                "totals": {"page_views": 8},
                "blocked": [], "state_hash": "h1",
            },
            {
                "id": "node2", "port": 5002, "status": "up", "source": "http",
                "per_node": {"page_views": {"node2": 7}},
                "totals": {"page_views": 7},
                "blocked": [], "state_hash": "h2",
            },
        ]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        assert data["global"].get("page_views") == 12  # max(5,0)+max(3,7)=5+7=12


# ---------------------------------------------------------------------------
# Tests: /api/history
# ---------------------------------------------------------------------------

class TestHistory:
    def test_history_accumulates(self, app, client):
        nodes = [_make_node("node1", 5001, pv=1, state_hash="x")]
        _set_fetcher(app, nodes)
        for _ in range(5):
            client.get("/api/cluster")
        hist = client.get("/api/history").get_json()
        assert len(hist) == 5

    def test_history_capped_at_300(self, app, client):
        nodes = [_make_node("node1", 5001, state_hash="y")]
        _set_fetcher(app, nodes)
        for _ in range(350):
            client.get("/api/cluster")
        hist = client.get("/api/history").get_json()
        assert len(hist) == 300


# ---------------------------------------------------------------------------
# Tests: input validation — /api/visit
# ---------------------------------------------------------------------------

class TestVisitValidation:
    def test_unknown_node_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node99", "metric": "page_views", "n": 1})
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_bad_metric_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node1", "metric": "logins", "n": 1})
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_n_too_large_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node1", "metric": "clicks", "n": 99999})
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_n_zero_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node1", "metric": "clicks", "n": 0})
        assert resp.status_code == 400

    def test_n_negative_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node1", "metric": "clicks", "n": -5})
        assert resp.status_code == 400

    def test_boolean_n_returns_400(self, client):
        resp = client.post("/api/visit", json={"node": "node1", "metric": "clicks", "n": True})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Tests: input validation — /api/load
# ---------------------------------------------------------------------------

class TestLoadValidation:
    def test_rate_too_high_returns_400(self, client):
        resp = client.post("/api/load", json={"rate": 999, "duration": 5, "metric": "page_views"})
        assert resp.status_code == 400

    def test_duration_too_long_returns_400(self, client):
        resp = client.post("/api/load", json={"rate": 10, "duration": 120, "metric": "page_views"})
        assert resp.status_code == 400

    def test_bad_metric_returns_400(self, client):
        resp = client.post("/api/load", json={"rate": 10, "duration": 5, "metric": "logins"})
        assert resp.status_code == 400

    def test_valid_load_starts(self, app, client):
        # Patch start_load to avoid actually spinning up threads
        state = app.config["DASH_STATE"]
        original = state.start_load
        called = []

        def fake_start(rate, duration, metric, services):
            called.append((rate, duration, metric))
            return True

        state.start_load = fake_start
        resp = client.post("/api/load", json={"rate": 10, "duration": 5, "metric": "clicks"})
        state.start_load = original
        assert resp.status_code == 200
        assert called == [(10.0, 5.0, "clicks")]

    def test_only_one_load_at_a_time(self, app, client):
        state = app.config["DASH_STATE"]
        original = state.start_load

        def fake_start_busy(rate, duration, metric, services):
            return False  # simulate already running

        state.start_load = fake_start_busy
        resp = client.post("/api/load", json={"rate": 10, "duration": 5, "metric": "clicks"})
        state.start_load = original
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# Tests: input validation — /api/partition
# ---------------------------------------------------------------------------

class TestPartitionValidation:
    def test_single_group_returns_400(self, client):
        resp = client.post("/api/partition", json={"groups": [["node1", "node2"]]})
        assert resp.status_code == 400

    def test_unknown_node_in_group_returns_400(self, client):
        resp = client.post("/api/partition", json={"groups": [["node1"], ["node99"]]})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Tests: convergence time tracking
# ---------------------------------------------------------------------------

class TestConvergenceTracking:
    def test_last_convergence_seconds_null_initially(self, app, client):
        nodes = [_make_node("node1", 5001, state_hash="x")]
        _set_fetcher(app, nodes)
        data = client.get("/api/cluster").get_json()
        # Only 1 node — converged is False (need >1 node)
        assert data["last_convergence_seconds"] is None

    def test_convergence_time_recorded_after_heal(self, app, client):
        """Simulate diverged -> converged transition and check timing."""
        import time

        state = app.config["DASH_STATE"]

        # Step 1: diverged
        nodes_div = [
            _make_node("node1", 5001, pv=5, state_hash="h1"),
            _make_node("node2", 5002, pv=10, state_hash="h2"),
        ]
        app.config["CLUSTER_FETCHER"] = lambda s: _make_cluster(nodes_div)
        client.get("/api/cluster")

        # Simulate a bit of time passing
        time.sleep(0.05)

        # Step 2: converged
        nodes_conv = [
            _make_node("node1", 5001, pv=10, state_hash="same"),
            _make_node("node2", 5002, pv=10, state_hash="same"),
        ]
        app.config["CLUSTER_FETCHER"] = lambda s: _make_cluster(nodes_conv)
        data = client.get("/api/cluster").get_json()

        # Should now have a recorded convergence time
        assert data["last_convergence_seconds"] is not None
        assert data["last_convergence_seconds"] >= 0.0


# ---------------------------------------------------------------------------
# Tests: /api/events
# ---------------------------------------------------------------------------

class TestEvents:
    def test_events_endpoint_returns_list(self, client):
        resp = client.get("/api/events")
        assert resp.status_code == 200
        assert isinstance(resp.get_json(), list)
