"""Flask application factory and HTTP API for a distributed analytics node."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any

from flask import Flask, jsonify, request

from crdt.store import MetricsStore
from node.config import Config
from node.gossip import GossipManager
from node.persistence import PersistenceManager

logger = logging.getLogger(__name__)


class NodeContext:
    """Encapsulates all runtime components for a node instance."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.lock = threading.Lock()
        self.blocked_lock = threading.Lock()
        self.blocked_set: set[str] = set()

        # Initialize persistence manager with snapshot callback
        self.persistence = PersistenceManager(
            node_id=config.node_id,
            data_dir=config.data_dir,
            get_snapshot=self.get_snapshot,
            save_interval=0.5,
        )

        # Load existing state from disk or start clean
        self.store: MetricsStore = self.persistence.load_initial_state(config.metrics)

        # Initialize gossip manager
        self.gossip = GossipManager(
            node_id=config.node_id,
            peers=config.peers,
            gossip_interval=config.gossip_interval,
            fanout=config.fanout,
            request_timeout=config.request_timeout,
            get_snapshot=self.get_snapshot,
            merge_remote_state=self.merge_remote_state,
            is_peer_blocked=self.is_peer_blocked,
        )

    def get_snapshot(self) -> dict[str, dict[str, int]]:
        """Return a thread-safe snapshot copy of the current MetricsStore."""
        with self.lock:
            return self.store.to_dict()

    def merge_remote_state(self, remote_state: dict[str, Any]) -> bool:
        """Safely deserialize and merge remote CRDT state under lock."""
        try:
            remote_store = MetricsStore.from_dict(remote_state)
        except Exception as exc:
            logger.warning("[%s] Failed to parse remote CRDT state: %s", self.config.node_id, exc)
            return False

        with self.lock:
            changed = self.store.merge(remote_store)

        if changed:
            self.persistence.mark_dirty()

        return changed

    def is_peer_blocked(self, peer_id: str) -> bool:
        """Check whether traffic to/from peer_id is blocked by partition simulation."""
        with self.blocked_lock:
            return peer_id in self.blocked_set or "*" in self.blocked_set

    def start(self) -> None:
        """Start background persistence and gossip threads."""
        self.persistence.start()
        self.gossip.start()

    def stop(self) -> None:
        """Stop background services and flush state."""
        self.gossip.stop()
        self.persistence.stop()


def create_app(config: Config) -> Flask:
    """Application factory creating a testable Flask app instance without global state."""
    app = Flask(__name__)
    ctx = NodeContext(config)
    app.extensions["node"] = ctx

    @app.before_request
    def handle_options_preflight():
        if request.method == "OPTIONS":
            return app.make_response(("", 204))

    @app.after_request
    def apply_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return response

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({"status": "ok", "node": ctx.config.node_id}), 200

    @app.route("/visit", methods=["POST"])
    def record_visit():
        # Query parameters take precedence, fallback to JSON body
        metric = request.args.get("metric")
        n_param = request.args.get("n")

        if not metric or n_param is None:
            body = request.get_json(silent=True)
            if isinstance(body, dict):
                metric = metric or body.get("metric")
                if n_param is None and "n" in body:
                    n_param = body.get("n")

        if n_param is None:
            n_param = 1

        # 1. Whitelist validation
        if not metric or metric not in ctx.config.metrics:
            return jsonify({
                "error": f"Invalid metric '{metric}'. Whitelisted metrics: {ctx.config.metrics}"
            }), 400

        # 2. Integer n validation [1, 1_000_000]
        try:
            if isinstance(n_param, bool):
                raise ValueError("Boolean is not a valid count")
            n = int(n_param)
            if n < 1 or n > 1_000_000:
                raise ValueError("Out of bounds")
        except (ValueError, TypeError):
            return jsonify({
                "error": "Parameter 'n' must be an integer between 1 and 1,000,000"
            }), 400

        # 3. Update store under lock
        with ctx.lock:
            ctx.store.increment(metric, ctx.config.node_id, n)
            local_val = ctx.store.per_node(metric).get(ctx.config.node_id, 0)
            global_val = ctx.store.value(metric)

        ctx.persistence.mark_dirty()

        return jsonify({
            "node": ctx.config.node_id,
            "metric": metric,
            "local_value": local_val,
            "global_value": global_val,
        }), 200

    @app.route("/state", methods=["GET"])
    def get_state():
        with ctx.lock:
            state_dict = ctx.store.to_dict()
            totals_dict = ctx.store.totals()
            per_node_dict = {m: ctx.store.per_node(m) for m in state_dict}

        with ctx.blocked_lock:
            blocked_list = sorted(list(ctx.blocked_set))

        # Calculate canonical SHA256 hash of sorted JSON state
        canonical_json = json.dumps(state_dict, sort_keys=True).encode("utf-8")
        state_hash = hashlib.sha256(canonical_json).hexdigest()

        return jsonify({
            "node": ctx.config.node_id,
            "per_node": per_node_dict,
            "totals": totals_dict,
            "blocked": blocked_list,
            "state_hash": state_hash,
            "timestamp": time.time(),
        }), 200

    @app.route("/merge", methods=["POST"])
    def merge_state():
        raw_data = request.data
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be a valid JSON object"}), 400

        sender = body.get("sender")
        remote_state = body.get("state")

        if not isinstance(sender, str) or not sender or not isinstance(remote_state, dict):
            return jsonify({
                "error": "Payload must contain 'sender' (string) and 'state' (dictionary)"
            }), 400

        # Check partition block
        if ctx.is_peer_blocked(sender):
            logger.debug("[%s] Rejected /merge from blocked peer %s", ctx.config.node_id, sender)
            return jsonify({"error": f"Peer '{sender}' is blocked due to simulated network partition"}), 503

        # Validate remote CRDT state
        try:
            remote_store = MetricsStore.from_dict(remote_state)
        except Exception as exc:
            return jsonify({"error": f"Malformed CRDT state: {exc}"}), 400

        with ctx.lock:
            changed = ctx.store.merge(remote_store)
            local_state = ctx.store.to_dict()

        if changed:
            ctx.persistence.mark_dirty()

        ctx.gossip.record_received(len(raw_data), changed)

        # Push-pull reply: return current local state
        return jsonify({
            "node": ctx.config.node_id,
            "state": local_state,
        }), 200

    @app.route("/metrics", methods=["GET"])
    def get_metrics():
        return jsonify(ctx.gossip.get_stats_dict()), 200

    @app.route("/partition", methods=["POST"])
    def set_partition():
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or "blocked" not in body or not isinstance(body["blocked"], list):
            return jsonify({"error": "Payload must be JSON object containing 'blocked' list"}), 400

        for item in body["blocked"]:
            if not isinstance(item, str) or not item:
                return jsonify({"error": "Elements in 'blocked' list must be non-empty strings"}), 400

        with ctx.blocked_lock:
            ctx.blocked_set = set(body["blocked"])
            current = sorted(list(ctx.blocked_set))

        logger.info("[%s] Partition rules updated: blocked=%s", ctx.config.node_id, current)
        return jsonify({"blocked": current}), 200

    @app.route("/partition", methods=["DELETE"])
    def clear_partition():
        with ctx.blocked_lock:
            ctx.blocked_set.clear()

        logger.info("[%s] Partition rules cleared (healed)", ctx.config.node_id)
        return jsonify({"blocked": []}), 200

    return app
