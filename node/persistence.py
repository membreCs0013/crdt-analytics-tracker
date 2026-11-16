"""State persistence layer with atomic disk writes and background sync.

Ensures safe persistence to disk on Windows and POSIX systems using temporary
files and atomic file replacement (os.replace).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from typing import Any, Callable

from crdt.store import MetricsStore

logger = logging.getLogger(__name__)


class PersistenceManager:
    """Manages disk persistence for a node's MetricsStore.

    Features:
    - Atomic JSON persistence via temporary file and os.replace.
    - Periodic background saver thread running every 0.5s if dirty.
    - Graceful flush on shutdown.
    - Fault-tolerant loader that starts empty if file is missing or corrupt.
    """

    def __init__(
        self,
        node_id: str,
        data_dir: str,
        get_snapshot: Callable[[], dict[str, dict[str, int]]],
        save_interval: float = 0.5,
    ) -> None:
        self.node_id = node_id
        self.data_dir = os.path.abspath(data_dir)
        self.file_path = os.path.join(self.data_dir, f"{node_id}.json")
        self._get_snapshot = get_snapshot
        self.save_interval = save_interval

        self._dirty = False
        self._dirty_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._saver_thread: threading.Thread | None = None

        os.makedirs(self.data_dir, exist_ok=True)

    def mark_dirty(self) -> None:
        """Mark local state as needing disk persistence."""
        with self._dirty_lock:
            self._dirty = True

    @property
    def is_dirty(self) -> bool:
        """Check whether there are unsaved state changes."""
        with self._dirty_lock:
            return self._dirty

    def load_initial_state(self, initial_metrics: list[str] | None = None) -> MetricsStore:
        """Load state from disk if available, or return a fresh empty MetricsStore.

        Never crashes on corrupt or unreadable files.
        """
        if not os.path.exists(self.file_path):
            logger.info("[%s] No existing state file at %s. Starting fresh.", self.node_id, self.file_path)
            return MetricsStore(initial_metrics)

        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            store = MetricsStore.from_dict(data)
            # Ensure all configured whitelist metrics exist in store
            if initial_metrics:
                for m in initial_metrics:
                    store.value(m)  # Ensures metric is present
            logger.info("[%s] Successfully restored state from %s", self.node_id, self.file_path)
            return store
        except Exception as exc:
            logger.warning(
                "[%s] Failed to load state from %s (%s). Starting fresh.",
                self.node_id,
                self.file_path,
                exc,
            )
            return MetricsStore(initial_metrics)

    def atomic_save(self, snapshot: dict[str, Any]) -> None:
        """Atomically persist snapshot to disk via a temp file and os.replace."""
        os.makedirs(self.data_dir, exist_ok=True)
        # Create temp file in the same directory to guarantee atomic rename on the same filesystem
        temp_file = os.path.join(self.data_dir, f"{self.node_id}.json.tmp.{uuid.uuid4().hex}")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace (works on Windows when file is closed)
            os.replace(temp_file, self.file_path)
        except Exception as exc:
            logger.error("[%s] Failed to atomically save state to disk: %s", self.node_id, exc)
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    pass
            raise

    def flush(self) -> None:
        """Immediately persist state to disk if dirty."""
        with self._dirty_lock:
            should_save = self._dirty
            self._dirty = False

        if should_save:
            snapshot = self._get_snapshot()
            try:
                self.atomic_save(snapshot)
                logger.debug("[%s] Flushed state to disk (%s)", self.node_id, self.file_path)
            except Exception:
                # Restore dirty flag so next attempt retries
                with self._dirty_lock:
                    self._dirty = True

    def start(self) -> None:
        """Start the periodic background saver thread."""
        if self._saver_thread is not None and self._saver_thread.is_alive():
            return

        self._stop_event.clear()
        self._saver_thread = threading.Thread(
            target=self._run_saver_loop,
            name=f"persistence-{self.node_id}",
            daemon=True,
        )
        self._saver_thread.start()
        logger.info("[%s] Background persistence saver started (interval=%.2fs)", self.node_id, self.save_interval)

    def stop(self) -> None:
        """Stop background saver thread and flush pending state."""
        self._stop_event.set()
        if self._saver_thread is not None and self._saver_thread.is_alive():
            self._saver_thread.join(timeout=2.0)
        self.flush()
        logger.info("[%s] Background persistence saver stopped and flushed", self.node_id)

    def _run_saver_loop(self) -> None:
        """Worker loop for background persistence thread."""
        while not self._stop_event.is_set():
            if self._stop_event.wait(self.save_interval):
                break
            self.flush()
