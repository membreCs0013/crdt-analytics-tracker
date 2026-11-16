"""Gossip synchronization protocol and telemetry tracking.

Runs a periodic push-pull gossip loop that exchanges CRDT states with randomly
selected peer nodes to guarantee Strong Eventual Consistency (SEC).
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

from crdt.store import MetricsStore

logger = logging.getLogger(__name__)


@dataclass
class GossipStats:
    """Telemetry counters for gossip operations."""

    sent: int = 0
    received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    failures: int = 0
    merges_changed: int = 0
    last_sync: dict[str, float] = field(default_factory=dict)
    last_sync_global: float | None = None
    start_time: float = field(default_factory=time.time)

    def to_dict(self, peers: list[str]) -> dict[str, Any]:
        """Format stats for GET /metrics endpoint."""
        now = time.time()
        return {
            "sent": self.sent,
            "received": self.received,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "failures": self.failures,
            "merges_changed": self.merges_changed,
            "last_sync": {
                "global": self.last_sync_global,
                "peers": dict(self.last_sync),
            },
            "uptime": round(now - self.start_time, 2),
            "peers": peers,
        }


class GossipManager:
    """Manages peer-to-peer push-pull gossip synchronization.

    Features:
    - Periodic randomized fanout gossip with +/-10% jitter.
    - Connection reuse via requests.Session per peer.
    - Fault-tolerant push-pull exchange: never crashes on peer failure.
    - Respects simulated network partitions (both incoming and outgoing).
    """

    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        gossip_interval: float,
        fanout: int,
        request_timeout: float,
        get_snapshot: Callable[[], dict[str, dict[str, int]]],
        merge_remote_state: Callable[[dict[str, Any]], bool],
        is_peer_blocked: Callable[[str], bool],
    ) -> None:
        self.node_id = node_id
        self.peers = dict(peers)  # {node_id: base_url}
        self.gossip_interval = gossip_interval
        self.fanout = fanout
        self.request_timeout = request_timeout
        self.get_snapshot = get_snapshot
        self.merge_remote_state = merge_remote_state
        self.is_peer_blocked = is_peer_blocked

        self.stats = GossipStats()
        self._stats_lock = threading.Lock()
        self._sessions: dict[str, requests.Session] = {
            peer_id: requests.Session() for peer_id in self.peers
        }

        # Track failure state per peer to avoid log spam.
        # True = peer was unreachable on the last attempt.
        self._peer_failing: dict[str, bool] = {peer_id: False for peer_id in self.peers}

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the gossip background thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_gossip_loop,
            name=f"gossip-{self.node_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "[%s] Gossip service started (peers=%s, interval=%.2fs, fanout=%d)",
            self.node_id,
            list(self.peers.keys()),
            self.gossip_interval,
            self.fanout,
        )

    def stop(self) -> None:
        """Stop the gossip background thread and close HTTP sessions."""
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        for session in self._sessions.values():
            try:
                session.close()
            except Exception:
                pass
        logger.info("[%s] Gossip service stopped", self.node_id)

    def get_stats_dict(self) -> dict[str, Any]:
        """Return safe copy of current gossip statistics."""
        with self._stats_lock:
            return self.stats.to_dict(list(self.peers.keys()))

    def record_received(self, byte_count: int, changed: bool) -> None:
        """Record telemetry for incoming merge request."""
        with self._stats_lock:
            self.stats.received += 1
            self.stats.bytes_received += byte_count
            if changed:
                self.stats.merges_changed += 1

    def _run_gossip_loop(self) -> None:
        """Worker loop for periodic gossip."""
        while not self._stop_event.is_set():
            # Add +/-10% jitter to avoid thundering-herd synchronized rounds
            jitter = random.uniform(0.9, 1.1)
            sleep_duration = max(0.05, self.gossip_interval * jitter)

            if self._stop_event.wait(sleep_duration):
                break

            self._perform_gossip_round()

    def _perform_gossip_round(self) -> None:
        """Execute one fanout round of gossip."""
        if not self.peers:
            return

        # Filter out peers blocked by network partition simulation
        eligible_peers = [
            pid for pid in self.peers if not self.is_peer_blocked(pid)
        ]
        if not eligible_peers:
            return

        sample_size = min(self.fanout, len(eligible_peers))
        chosen_peers = random.sample(eligible_peers, sample_size)

        snapshot = self.get_snapshot()
        payload = {
            "sender": self.node_id,
            "state": snapshot,
        }
        raw_payload = json.dumps(payload).encode("utf-8")
        bytes_to_send = len(raw_payload)

        for peer_id in chosen_peers:
            if self._stop_event.is_set():
                break
            self._gossip_to_peer(peer_id, payload, raw_payload, bytes_to_send)

    def _gossip_to_peer(
        self,
        peer_id: str,
        payload: dict[str, Any],
        raw_payload: bytes,
        bytes_to_send: int,
    ) -> None:
        """Push state to a single peer and merge its reply (push-pull)."""
        peer_url = self.peers.get(peer_id)
        if not peer_url:
            return

        endpoint = f"{peer_url}/merge"
        session = self._sessions.get(peer_id)
        if session is None:
            session = requests.Session()
            self._sessions[peer_id] = session

        try:
            with self._stats_lock:
                self.stats.sent += 1
                self.stats.bytes_sent += bytes_to_send

            resp = session.post(
                endpoint,
                data=raw_payload,
                headers={"Content-Type": "application/json"},
                timeout=self.request_timeout,
            )

            # Peer responded
            if resp.status_code == 200:
                response_bytes = len(resp.content)
                now = time.time()
                with self._stats_lock:
                    self.stats.received += 1
                    self.stats.bytes_received += response_bytes
                    self.stats.last_sync[peer_id] = now
                    self.stats.last_sync_global = now

                # Log recovery if peer was previously failing
                if self._peer_failing.get(peer_id):
                    logger.info(
                        "[%s] Peer %s recovered (reconnected successfully)",
                        self.node_id,
                        peer_id,
                    )
                    self._peer_failing[peer_id] = False

                # Peer replied with its own state (push-pull)
                try:
                    resp_json = resp.json()
                    peer_state = resp_json.get("state")
                    if isinstance(peer_state, dict):
                        changed = self.merge_remote_state(peer_state)
                        if changed:
                            with self._stats_lock:
                                self.stats.merges_changed += 1
                except Exception as parse_err:
                    logger.warning(
                        "[%s] Invalid JSON response from peer %s: %s",
                        self.node_id,
                        peer_id,
                        parse_err,
                    )
            elif resp.status_code == 503:
                # Peer rejected us because it considers us blocked in partition simulation
                logger.debug("[%s] Peer %s rejected merge (503 partition)", self.node_id, peer_id)
                with self._stats_lock:
                    self.stats.failures += 1
            else:
                logger.warning(
                    "[%s] Peer %s returned unexpected status %d",
                    self.node_id,
                    peer_id,
                    resp.status_code,
                )
                with self._stats_lock:
                    self.stats.failures += 1

        except (requests.Timeout, requests.ConnectionError) as net_err:
            # Only log WARNING on first failure; use DEBUG for subsequent ones in the same episode
            if not self._peer_failing.get(peer_id):
                logger.warning(
                    "[%s] Peer %s unreachable at %s (%s) -- suppressing repeats until recovery",
                    self.node_id,
                    peer_id,
                    endpoint,
                    net_err.__class__.__name__,
                )
                self._peer_failing[peer_id] = True
            else:
                logger.debug(
                    "[%s] Peer %s still unreachable (%s)",
                    self.node_id,
                    peer_id,
                    net_err.__class__.__name__,
                )
            with self._stats_lock:
                self.stats.failures += 1
        except Exception as exc:
            if not self._peer_failing.get(peer_id):
                logger.warning(
                    "[%s] Unexpected error gossiping to peer %s: %s -- suppressing repeats",
                    self.node_id,
                    peer_id,
                    exc,
                )
                self._peer_failing[peer_id] = True
            else:
                logger.debug(
                    "[%s] Peer %s error (suppressed): %s",
                    self.node_id,
                    peer_id,
                    exc,
                )
            with self._stats_lock:
                self.stats.failures += 1
