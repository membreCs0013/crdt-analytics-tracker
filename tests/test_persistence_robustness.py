"""Persistence robustness tests (Phase 0 Check E).

Tests:
1. Corrupt state file → starts empty with a warning.
2. Empty state file → starts empty with a warning.
3. Graceful shutdown flush → state written to disk when stop() is called directly.
4. Dirty-flag atomicity → cleared in the same lock hold as the snapshot.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from node.app import create_app
from node.config import Config


def _make_app(node_id: str, data_dir: str, metrics: list[str] | None = None):
    cfg = Config(
        node_id=node_id,
        port=5000,
        host="127.0.0.1",
        peers={},
        gossip_interval=1.0,
        fanout=1,
        data_dir=data_dir,
        metrics=metrics or ["page_views"],
        request_timeout=1.0,
    )
    return create_app(cfg)


class TestPersistenceRobustness:

    def test_corrupt_state_file_starts_empty(self, tmp_path: Path) -> None:
        """Corrupt JSON in state file → node starts with clean empty state."""
        state_file = tmp_path / "corrupt-node.json"
        state_file.write_text("{{not valid json!!", encoding="utf-8")

        app = _make_app("corrupt-node", str(tmp_path))
        with app.test_client() as c:
            state = c.get("/state").json
        assert state["totals"]["page_views"] == 0, "Should start with 0 after corrupt file"

    def test_empty_state_file_starts_empty(self, tmp_path: Path) -> None:
        """Empty state file → node starts with clean empty state."""
        state_file = tmp_path / "empty-node.json"
        state_file.write_text("", encoding="utf-8")

        app = _make_app("empty-node", str(tmp_path))
        with app.test_client() as c:
            state = c.get("/state").json
        assert state["totals"]["page_views"] == 0, "Should start with 0 after empty file"

    def test_graceful_shutdown_flush(self, tmp_path: Path) -> None:
        """Calling ctx.stop() directly triggers flush and writes state to disk."""
        app = _make_app("flush-node", str(tmp_path))
        ctx = app.extensions["node"]
        ctx.start()

        with app.test_client() as c:
            c.post("/visit?metric=page_views&n=42")

        # Simulate graceful shutdown (what SIGTERM handler calls)
        ctx.stop()

        state_file = tmp_path / "flush-node.json"
        assert state_file.exists(), "State file must exist after graceful shutdown"

        data = json.loads(state_file.read_text(encoding="utf-8"))
        assert data.get("page_views", {}).get("flush-node", 0) == 42, (
            "Flushed state must reflect the 42 visits"
        )

    def test_dirty_flag_cleared_atomically(self, tmp_path: Path) -> None:
        """dirty flag is cleared inside the lock before snapshot is taken.

        Contract: after flush() returns, is_dirty == False.
        A subsequent mark_dirty() must set it back to True (write during save
        triggers another save on the next loop iteration).
        """
        app = _make_app("dirty-node", str(tmp_path))
        ctx = app.extensions["node"]
        ctx.start()

        with app.test_client() as c:
            c.post("/visit?metric=page_views&n=10")

        # Flush clears dirty flag atomically
        ctx.persistence.mark_dirty()
        ctx.persistence.flush()
        assert not ctx.persistence.is_dirty, "Dirty flag must be cleared after flush()"

        # A write that arrives "after" flush sets dirty again → will be saved on next loop
        ctx.persistence.mark_dirty()
        assert ctx.persistence.is_dirty, "mark_dirty() after flush() must set dirty=True"

        ctx.stop()

    def test_state_restored_from_disk(self, tmp_path: Path) -> None:
        """State saved on shutdown can be restored on the next startup."""
        # First run: record visits and shut down gracefully
        app1 = _make_app("restore-node", str(tmp_path))
        ctx1 = app1.extensions["node"]
        ctx1.start()
        with app1.test_client() as c:
            c.post("/visit?metric=page_views&n=99")
        ctx1.stop()

        # Second run on the same data_dir must restore state
        app2 = _make_app("restore-node", str(tmp_path))
        with app2.test_client() as c:
            state = c.get("/state").json
        assert state["totals"]["page_views"] == 99, (
            "Restarted node must restore 99 page_views from disk"
        )
