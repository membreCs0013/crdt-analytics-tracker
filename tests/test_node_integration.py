"""End-to-end integration tests for multi-node cluster.

Spawns 3 real node subprocesses on free ports, testing:
1. Concurrent visits and convergence across all nodes.
2. Network partition isolation, divergence, and healing convergence.
3. Node failure, state restoration from disk persistence, and re-convergence.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Generator

import pytest
import requests

# Helper to find free localhost port
def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(
    predicate: Callable[[], bool],
    timeout: float = 10.0,
    interval: float = 0.1,
    desc: str = "condition",
) -> None:
    """Poll predicate until True or timeout occurs."""
    deadline = time.time() + timeout
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_exc = exc
        time.sleep(interval)

    # Final attempt to surface assertion errors if predicate uses assertions
    try:
        if predicate():
            return
    except Exception as exc:
        raise TimeoutError(f"Timed out waiting {timeout}s for {desc}: {exc}") from exc

    if last_exc:
        raise TimeoutError(f"Timed out waiting {timeout}s for {desc}: {last_exc}")
    raise TimeoutError(f"Timed out waiting {timeout}s for {desc}")


class NodeProcess:
    """Wrapper around a running node subprocess."""

    def __init__(
        self,
        node_id: str,
        port: int,
        data_dir: str,
        peers: dict[str, str],
        repo_root: Path,
        gossip_interval: float = 0.2,
    ) -> None:
        self.node_id = node_id
        self.port = port
        self.data_dir = data_dir
        self.peers = peers
        self.repo_root = repo_root
        self.gossip_interval = gossip_interval
        self.url = f"http://127.0.0.1:{port}"
        self.proc: subprocess.Popen[str] | None = None

    def start(self) -> None:
        """Start the node process and wait for /health endpoint."""
        peers_str = ",".join(f"{pid}={url}" for pid, url in self.peers.items())
        cmd = [
            sys.executable,
            "-m",
            "node",
            "--node-id",
            self.node_id,
            "--port",
            str(self.port),
            "--host",
            "127.0.0.1",
            "--peers",
            peers_str,
            "--gossip-interval",
            str(self.gossip_interval),
            "--fanout",
            "2",
            "--data-dir",
            self.data_dir,
            "--metrics",
            "page_views,clicks",
            "--request-timeout",
            "1.0",
        ]

        self.proc = subprocess.Popen(
            cmd,
            cwd=str(self.repo_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        # Wait until node is responsive
        wait_until(
            lambda: self.is_healthy(),
            timeout=8.0,
            interval=0.1,
            desc=f"{self.node_id} to become healthy",
        )

    def is_healthy(self) -> bool:
        try:
            resp = requests.get(f"{self.url}/health", timeout=0.5)
            return resp.status_code == 200 and resp.json().get("status") == "ok"
        except Exception:
            return False

    def visit(self, metric: str, n: int = 1) -> dict[str, Any]:
        resp = requests.post(f"{self.url}/visit?metric={metric}&n={n}", timeout=2.0)
        resp.raise_for_status()
        return resp.json()

    def get_state(self) -> dict[str, Any]:
        resp = requests.get(f"{self.url}/state", timeout=2.0)
        resp.raise_for_status()
        return resp.json()

    def partition(self, blocked: list[str]) -> None:
        resp = requests.post(f"{self.url}/partition", json={"blocked": blocked}, timeout=2.0)
        resp.raise_for_status()

    def heal(self) -> None:
        resp = requests.delete(f"{self.url}/partition", timeout=2.0)
        resp.raise_for_status()

    def stop(self) -> None:
        """Terminate the subprocess."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=1.0)
        self.proc = None


@pytest.fixture
def cluster() -> Generator[dict[str, NodeProcess], None, None]:
    """Launch a 3-node cluster with guaranteed cleanup on test exit."""
    repo_root = Path(__file__).resolve().parent.parent
    base_dir = tempfile.mkdtemp(prefix="crdt_cluster_test_")

    ports = {
        "node1": find_free_port(),
        "node2": find_free_port(),
        "node3": find_free_port(),
    }

    nodes: dict[str, NodeProcess] = {}
    try:
        for nid, port in ports.items():
            data_dir = os.path.join(base_dir, nid)
            peers = {
                other_id: f"http://127.0.0.1:{other_port}"
                for other_id, other_port in ports.items()
                if other_id != nid
            }
            node = NodeProcess(
                node_id=nid,
                port=port,
                data_dir=data_dir,
                peers=peers,
                repo_root=repo_root,
                gossip_interval=0.2,
            )
            node.start()
            nodes[nid] = node

        yield nodes
    finally:
        for node in nodes.values():
            node.stop()
        shutil.rmtree(base_dir, ignore_errors=True)


class TestClusterIntegration:
    """Integration test scenarios."""

    def test_concurrent_visits_and_convergence(
        self, cluster: dict[str, NodeProcess]
    ) -> None:
        """Concurrent visits on all 3 nodes converge to identical totals and state_hash."""
        n1, n2, n3 = cluster["node1"], cluster["node2"], cluster["node3"]

        # Record concurrent visits
        n1.visit("page_views", 100)
        n1.visit("clicks", 10)

        n2.visit("page_views", 200)
        n2.visit("clicks", 20)

        n3.visit("page_views", 300)
        n3.visit("clicks", 30)

        # Expected totals across cluster
        expected_totals = {"page_views": 600, "clicks": 60}

        def all_converged() -> bool:
            s1 = n1.get_state()
            s2 = n2.get_state()
            s3 = n3.get_state()

            totals_match = (
                s1["totals"] == expected_totals
                and s2["totals"] == expected_totals
                and s3["totals"] == expected_totals
            )
            hashes_match = s1["state_hash"] == s2["state_hash"] == s3["state_hash"]
            return totals_match and hashes_match

        wait_until(all_converged, timeout=8.0, desc="cluster full convergence")

    def test_partition_divergence_and_healing(
        self, cluster: dict[str, NodeProcess]
    ) -> None:
        """Partition node3 from node1 and node2, assert divergence, then heal and assert convergence."""
        n1, n2, n3 = cluster["node1"], cluster["node2"], cluster["node3"]

        # Step 1: Base convergence
        n1.visit("page_views", 50)
        wait_until(
            lambda: n1.get_state()["totals"]["page_views"] == 50
            and n2.get_state()["totals"]["page_views"] == 50
            and n3.get_state()["totals"]["page_views"] == 50,
            timeout=6.0,
            desc="base state convergence",
        )

        # Step 2: Establish partition isolating node3
        # Both sides must block to guarantee full bilateral partition
        n3.partition(["node1", "node2"])
        n1.partition(["node3"])
        n2.partition(["node3"])

        # Step 3: Add visits on both sides of partition
        n1.visit("page_views", 40)  # side A: 50 + 40 = 90
        n3.visit("page_views", 60)  # side B: 50 + 60 = 110

        # Wait for n1 and n2 to synchronize within their partition
        wait_until(
            lambda: n1.get_state()["totals"]["page_views"] == 90
            and n2.get_state()["totals"]["page_views"] == 90,
            timeout=6.0,
            desc="partition A internal sync",
        )

        # Assert node3 diverged from node1/node2
        s1 = n1.get_state()
        s3 = n3.get_state()
        assert s1["totals"]["page_views"] == 90
        assert s3["totals"]["page_views"] == 110
        assert s1["state_hash"] != s3["state_hash"]

        # Step 4: Heal partition
        n1.heal()
        n2.heal()
        n3.heal()

        # Step 5: Assert convergence to the sum of all visits (50 + 40 + 60 = 150)
        expected_total = 150

        def healed_converged() -> bool:
            st1 = n1.get_state()
            st2 = n2.get_state()
            st3 = n3.get_state()
            totals_ok = (
                st1["totals"]["page_views"] == expected_total
                and st2["totals"]["page_views"] == expected_total
                and st3["totals"]["page_views"] == expected_total
            )
            hashes_ok = st1["state_hash"] == st2["state_hash"] == st3["state_hash"]
            return totals_ok and hashes_ok

        wait_until(healed_converged, timeout=8.0, desc="healed cluster convergence")

    def test_persistence_and_node_restart(
        self, cluster: dict[str, NodeProcess]
    ) -> None:
        """Kill a node, restart with same DATA_DIR, verify state restoration and re-convergence."""
        n1, n2, n3 = cluster["node1"], cluster["node2"], cluster["node3"]

        # Record visits on node3
        n3.visit("page_views", 120)
        n3.visit("clicks", 15)

        # Wait until persisted to disk (background saver runs every 0.5s)
        state_file = os.path.join(n3.data_dir, f"{n3.node_id}.json")
        wait_until(
            lambda: os.path.exists(state_file)
            and json.loads(Path(state_file).read_text(encoding="utf-8"))
            .get("page_views", {})
            .get("node3", 0)
            == 120,
            timeout=4.0,
            desc="node3 state file on disk",
        )

        # Kill node3
        n3.stop()
        assert not n3.is_healthy()

        # Add concurrent visit on node1 while node3 is offline
        n1.visit("page_views", 80)

        # Restart node3 on the exact same port and data_dir
        restarted_n3 = NodeProcess(
            node_id=n3.node_id,
            port=n3.port,
            data_dir=n3.data_dir,
            peers=n3.peers,
            repo_root=n3.repo_root,
            gossip_interval=0.2,
        )
        restarted_n3.start()
        cluster["node3"] = restarted_n3

        # Immediate state check: node3 must have loaded its 120 page_views
        restored_state = restarted_n3.get_state()
        assert restored_state["per_node"]["page_views"].get("node3") == 120

        # Wait for all 3 nodes to converge (120 + 80 = 200 page views)
        expected_totals = {"page_views": 200, "clicks": 15}

        def cluster_converged() -> bool:
            s1 = n1.get_state()
            s2 = n2.get_state()
            s3 = restarted_n3.get_state()
            totals_ok = (
                s1["totals"] == expected_totals
                and s2["totals"] == expected_totals
                and s3["totals"] == expected_totals
            )
            hashes_ok = s1["state_hash"] == s2["state_hash"] == s3["state_hash"]
            return totals_ok and hashes_ok

        wait_until(cluster_converged, timeout=8.0, desc="restarted node convergence")
