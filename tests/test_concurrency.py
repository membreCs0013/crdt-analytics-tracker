"""Concurrency test: 8 threads x 500 /visit calls while gossip merges run.

Final totals must be exact (no lost updates), no deadlock (enforced by join timeout).
"""

from __future__ import annotations

import shutil
import tempfile
import threading

import pytest

from node.app import create_app
from node.config import Config


class TestConcurrency:
    """Verifies thread-safety of the Flask node under heavy concurrent /visit load."""

    def test_concurrent_visits_exact_count(self) -> None:
        """8 threads × 500 visits = 4000 exact total; no deadlock within 30 s."""
        tmp = tempfile.mkdtemp(prefix="crdt_conc_")
        try:
            cfg = Config(
                node_id="conc-node",
                port=5098,
                host="127.0.0.1",
                peers={},
                gossip_interval=0.05,  # frequent gossip to stress-test merge path
                fanout=1,
                data_dir=tmp,
                metrics=["page_views", "clicks"],
                request_timeout=1.0,
            )
            app = create_app(cfg)
            ctx = app.extensions["node"]
            ctx.start()  # starts gossip + persistence threads

            NUM_THREADS = 8
            VISITS_PER_THREAD = 500
            errors: list[int] = []
            lock = threading.Lock()

            def worker() -> None:
                with app.test_client() as c:
                    for _ in range(VISITS_PER_THREAD):
                        r = c.post("/visit?metric=page_views&n=1")
                        if r.status_code != 200:
                            with lock:
                                errors.append(r.status_code)

            threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30.0)
                assert not t.is_alive(), "Thread deadlocked — did not finish within 30 s"

            ctx.stop()

            assert not errors, f"Got unexpected HTTP errors during concurrent visits: {errors}"

            # Verify exact total count — no lost or duplicated increments
            with app.test_client() as c:
                state = c.get("/state").json

            expected = NUM_THREADS * VISITS_PER_THREAD  # 4000
            actual = state["totals"]["page_views"]
            assert actual == expected, f"Expected {expected} page_views, got {actual}"

        finally:
            shutil.rmtree(tmp, ignore_errors=True)
