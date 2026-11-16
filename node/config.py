"""Configuration management for a distributed analytics node.

Supports loading configuration from environment variables and CLI arguments,
with CLI arguments taking precedence over environment variables.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from typing import Mapping, Sequence


def parse_peers_string(raw: str | None) -> dict[str, str]:
    """Parse peer definitions from string format 'node2=http://127.0.0.1:5002,node3=...'.

    Returns a mapping of peer node_id to base URL.
    """
    if not raw or not raw.strip():
        return {}

    peers: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(f"Invalid peer definition '{part}'. Expected 'node_id=url'")
        node_id, url = part.split("=", 1)
        node_id = node_id.strip()
        url = url.strip().rstrip("/")
        if not node_id or not url:
            raise ValueError(f"Invalid peer definition '{part}'")
        peers[node_id] = url

    return peers


def parse_metrics_string(raw: str | None) -> list[str]:
    """Parse comma-separated metric whitelist string into a list of metric names."""
    if not raw or not raw.strip():
        return ["page_views", "clicks"]

    metrics = [m.strip() for m in raw.split(",") if m.strip()]
    return metrics if metrics else ["page_views", "clicks"]


@dataclass
class Config:
    """Node configuration settings."""

    node_id: str
    port: int = 5000
    host: str = "127.0.0.1"
    peers: dict[str, str] = field(default_factory=dict)
    gossip_interval: float = 1.0
    fanout: int = 2
    data_dir: str = "./data"
    metrics: list[str] = field(default_factory=lambda: ["page_views", "clicks"])
    request_timeout: float = 1.0

    @classmethod
    def from_env_and_args(
        cls,
        args: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Config:
        """Create Config resolving environment variables and overriding with CLI args."""
        if env is None:
            env = os.environ

        parser = argparse.ArgumentParser(
            description="Distributed Analytics Tracker Node",
            add_help=False,
        )
        parser.add_argument("--node-id", dest="node_id", type=str, default=None)
        parser.add_argument("--port", dest="port", type=int, default=None)
        parser.add_argument("--host", dest="host", type=str, default=None)
        parser.add_argument("--peers", dest="peers", type=str, default=None)
        parser.add_argument("--gossip-interval", dest="gossip_interval", type=float, default=None)
        parser.add_argument("--fanout", dest="fanout", type=int, default=None)
        parser.add_argument("--data-dir", dest="data_dir", type=str, default=None)
        parser.add_argument("--metrics", dest="metrics", type=str, default=None)
        parser.add_argument("--request-timeout", dest="request_timeout", type=float, default=None)
        parser.add_argument("-h", "--help", action="help", help="Show this help message and exit")

        parsed, _ = parser.parse_known_args(args)

        # 1. Resolve NODE_ID
        node_id = parsed.node_id or env.get("NODE_ID")
        if not node_id:
            raise ValueError("NODE_ID must be specified via --node-id or NODE_ID env var")

        # 2. Resolve PORT
        port_val = parsed.port
        if port_val is None and "PORT" in env:
            port_val = int(env["PORT"])
        if port_val is None:
            port_val = 5000

        # 3. Resolve HOST
        host_val = parsed.host or env.get("HOST") or "127.0.0.1"

        # 4. Resolve PEERS
        peers_raw = parsed.peers if parsed.peers is not None else env.get("PEERS", "")
        peers_val = parse_peers_string(peers_raw)

        # 5. Resolve GOSSIP_INTERVAL
        gossip_val = parsed.gossip_interval
        if gossip_val is None and "GOSSIP_INTERVAL" in env:
            gossip_val = float(env["GOSSIP_INTERVAL"])
        if gossip_val is None:
            gossip_val = 1.0

        # 6. Resolve FANOUT
        fanout_val = parsed.fanout
        if fanout_val is None and "FANOUT" in env:
            fanout_val = int(env["FANOUT"])
        if fanout_val is None:
            fanout_val = 2

        # 7. Resolve DATA_DIR
        data_dir_val = parsed.data_dir or env.get("DATA_DIR") or "./data"

        # 8. Resolve METRICS
        metrics_raw = parsed.metrics if parsed.metrics is not None else env.get("METRICS")
        metrics_val = parse_metrics_string(metrics_raw)

        # 9. Resolve REQUEST_TIMEOUT
        timeout_val = parsed.request_timeout
        if timeout_val is None and "REQUEST_TIMEOUT" in env:
            timeout_val = float(env["REQUEST_TIMEOUT"])
        if timeout_val is None:
            timeout_val = 1.0

        return cls(
            node_id=node_id,
            port=port_val,
            host=host_val,
            peers=peers_val,
            gossip_interval=gossip_val,
            fanout=fanout_val,
            data_dir=data_dir_val,
            metrics=metrics_val,
            request_timeout=timeout_val,
        )
