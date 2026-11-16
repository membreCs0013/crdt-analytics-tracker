"""dashboard/server.py — Flask application factory for the CRDT dashboard.

Design decisions
----------------
* Host-side server: needs Docker CLI for isolate/kill/exec features; running
  inside a container would require DinD (Docker-in-Docker) which is fragile.
* Global totals via CRDT merge: we do a point-wise max per node_id per metric
  over all reachable per_node dicts, then sum. This correctly reflects the
  G-Counter LUB instead of naively summing node totals (which over-counts in a
  converged cluster).
* Exec fallback: when a node is network-isolated its published port is gone;
  `docker exec <node> python -c 'urllib ...'` reads /state from inside the
  container, same as ctl.py does.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METRICS_WHITELIST = {"page_views", "clicks"}
MAX_HISTORY = 300
MAX_EVENTS = 100
FETCH_TIMEOUT = 1.0  # seconds per node HTTP fetch
MAX_RATE = 500
MAX_DURATION = 60
MAX_N = 10_000

# ---------------------------------------------------------------------------
# Helpers shared with ctl.py (re-implemented without importing ctl to avoid
# the _check_docker() side-effect at import time)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_compose(compose_path: Path) -> dict[str, int]:
    """Parse docker-compose.yml; return {service_name: host_port}."""
    if not compose_path.exists():
        return {}
    services: dict[str, int] = {}
    current_service: str | None = None
    in_ports = False
    in_services = False
    with open(compose_path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip()
            stripped = line.lstrip()
            if stripped.startswith("services:"):
                in_services = True
                continue
            if not in_services:
                continue
            if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
                svc = stripped.rstrip(":")
                if svc not in ("networks", "volumes"):
                    current_service = svc
                    in_ports = False
                continue
            if current_service and "ports:" in stripped:
                in_ports = True
                continue
            if in_ports and stripped.startswith("- '") and current_service:
                port_def = stripped.strip("- '\"").split(":")[0]
                try:
                    services[current_service] = int(port_def)
                except ValueError:
                    pass
                in_ports = False
    return services


def _http_get_state(url: str, timeout: float = FETCH_TIMEOUT) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _exec_state(node: str) -> dict | None:
    """Read /state from inside a network-isolated container via docker exec."""
    script = (
        "import urllib.request, json, sys; "
        "r=urllib.request.urlopen('http://localhost:5000/state', timeout=3); "
        "print(r.read().decode())"
    )
    try:
        result = subprocess.run(
            ["docker", "exec", node, "python", "-c", script],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout.strip())
    except Exception:
        pass
    return None


def _container_running(node: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", node],
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.returncode == 0 and result.stdout.strip() == "true"
    except Exception:
        return False


def _run_docker_plain(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=10,
    )


def _run_docker_compose(compose_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(compose_path), *args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=15,
    )


# ---------------------------------------------------------------------------
# CRDT global merge
# ---------------------------------------------------------------------------

def _crdt_merge_global(node_states: list[dict]) -> dict[str, int]:
    """Compute the CRDT LUB of all reachable per_node dicts.

    For each metric, take the component-wise maximum over node_id entries
    across all reachable nodes, then sum. This is equivalent to merging all
    GCounters and reading .value().
    """
    merged: dict[str, dict[str, int]] = {}  # metric -> {node_id -> max_count}
    for state in node_states:
        per_node = state.get("per_node", {})
        for metric, node_counts in per_node.items():
            if not isinstance(node_counts, dict):
                continue
            bucket = merged.setdefault(metric, {})
            for node_id, count in node_counts.items():
                if isinstance(count, int) and count > bucket.get(node_id, 0):
                    bucket[node_id] = count
    return {metric: sum(counts.values()) for metric, counts in merged.items()}


# ---------------------------------------------------------------------------
# App state (in-memory, single-process)
# ---------------------------------------------------------------------------

class DashboardState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.history: list[dict] = []          # capped at MAX_HISTORY
        self.events: list[dict] = []           # capped at MAX_EVENTS

        self._load_running = False
        self._load_lock = threading.Lock()

        # Convergence tracking
        self._was_converged: bool | None = None
        self._diverged_at: float | None = None
        self.last_convergence_seconds: float | None = None

    # ------------------------------------------------------------------
    # History / events helpers
    # ------------------------------------------------------------------

    def push_history(self, sample: dict) -> None:
        with self.lock:
            self.history.append(sample)
            if len(self.history) > MAX_HISTORY:
                self.history = self.history[-MAX_HISTORY:]

    def push_event(self, action: str, result: str) -> None:
        with self.lock:
            self.events.append({
                "ts": time.time(),
                "action": action,
                "result": result,
            })
            if len(self.events) > MAX_EVENTS:
                self.events = self.events[-MAX_EVENTS:]

    def get_history(self) -> list:
        with self.lock:
            return list(self.history)

    def get_events(self) -> list:
        with self.lock:
            return list(self.events)

    # ------------------------------------------------------------------
    # Convergence tracking
    # ------------------------------------------------------------------

    def update_convergence(self, converged: bool) -> float | None:
        """Track not-converged->converged transitions and return last_convergence_seconds."""
        now = time.time()
        with self.lock:
            if not converged:
                # Mark the start of divergence (whether we were converged before or not)
                if self._diverged_at is None:
                    self._diverged_at = now
            else:
                # converged=True
                if self._diverged_at is not None:
                    # Reconverged after a diverged period
                    self.last_convergence_seconds = now - self._diverged_at
                    self._diverged_at = None
            self._was_converged = converged
            return self.last_convergence_seconds

    # ------------------------------------------------------------------
    # Load test
    # ------------------------------------------------------------------

    def start_load(self, rate: float, duration: float, metric: str,
                   services: dict[str, int]) -> bool:
        """Return True if load test started, False if one is already running."""
        with self._load_lock:
            if self._load_running:
                return False
            self._load_running = True

        t = threading.Thread(
            target=self._run_load,
            args=(rate, duration, metric, services),
            daemon=True,
        )
        t.start()
        return True

    def _run_load(self, rate: float, duration: float, metric: str,
                  services: dict[str, int]) -> None:
        try:
            nodes = sorted(services.keys())
            if not nodes:
                return
            total = int(rate * duration)
            per_node = max(1, total // len(nodes))
            interval = 1.0 / (rate / len(nodes)) if rate > 0 else 1.0

            def send(node: str, count: int) -> None:
                port = services[node]
                url = f"http://127.0.0.1:{port}/visit?metric={metric}&n=1"
                for _ in range(count):
                    try:
                        req = urllib.request.Request(url, data=b"", method="POST")
                        urllib.request.urlopen(req, timeout=1.0)
                    except Exception:
                        pass
                    time.sleep(interval)

            threads = [threading.Thread(target=send, args=(n, per_node), daemon=True)
                       for n in nodes]
            for th in threads:
                th.start()
            for th in threads:
                th.join()
        finally:
            with self._load_lock:
                self._load_running = False


# ---------------------------------------------------------------------------
# Node fetcher
# ---------------------------------------------------------------------------

def _fetch_node(node: str, port: int) -> dict:
    """Fetch /state for one node; use exec fallback if unreachable but running."""
    url = f"http://127.0.0.1:{port}/state"
    state = _http_get_state(url, timeout=FETCH_TIMEOUT)
    source = "http"
    via_exec = False

    if state is None:
        if _container_running(node):
            state = _exec_state(node)
            if state is not None:
                source = "exec"
                via_exec = True

    if state is None:
        return {
            "id": node,
            "port": port,
            "status": "down",
            "source": "none",
            "per_node": {},
            "totals": {},
            "blocked": [],
            "state_hash": "",
            "error": "unreachable",
        }

    # Determine status
    blocked = state.get("blocked", [])
    if via_exec:
        status = "isolated"
    elif blocked:
        status = "partitioned"
    else:
        status = "up"

    return {
        "id": node,
        "port": port,
        "status": status,
        "source": source,
        "per_node": state.get("per_node", {}),
        "totals": state.get("totals", {}),
        "blocked": blocked,
        "state_hash": state.get("state_hash", ""),
    }


def _poll_cluster(services: dict[str, int]) -> dict:
    """Poll all nodes in parallel and return the full cluster snapshot."""
    node_results: list[dict] = []

    with ThreadPoolExecutor(max_workers=len(services) or 1) as pool:
        futures = {pool.submit(_fetch_node, node, port): node
                   for node, port in services.items()}
        for fut in as_completed(futures):
            try:
                node_results.append(fut.result())
            except Exception as exc:
                node = futures[fut]
                port = services.get(node, 0)
                node_results.append({
                    "id": node, "port": port, "status": "down",
                    "source": "none", "per_node": {}, "totals": {},
                    "blocked": [], "state_hash": "", "error": str(exc),
                })

    # Sort by node id for stable output
    node_results.sort(key=lambda n: n["id"])

    # Compute global via CRDT merge over all reachable nodes
    reachable = [n for n in node_results if n["status"] != "down"]
    global_totals = _crdt_merge_global(reachable)

    # Convergence: all reachable nodes with a hash share the same hash
    hashes = [n["state_hash"] for n in reachable if n["state_hash"]]
    converged = len(hashes) > 1 and len(set(hashes)) == 1

    # Divergence: max total - min total per metric across reachable nodes
    divergence: dict[str, int] = {}
    for metric in METRICS_WHITELIST:
        vals = [n["totals"].get(metric, 0) for n in reachable]
        if len(vals) >= 2:
            divergence[metric] = max(vals) - min(vals)
        else:
            divergence[metric] = 0

    return {
        "ts": time.time(),
        "nodes": node_results,
        "converged": converged,
        "global": global_totals,
        "divergence": divergence,
    }


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def _validate_node(node_name: str, services: dict[str, int]) -> str | None:
    """Return error message or None."""
    if not isinstance(node_name, str) or not node_name:
        return "node must be a non-empty string"
    if node_name not in services:
        return f"unknown node '{node_name}'; valid nodes: {sorted(services)}"
    return None


def _validate_metric(metric: str) -> str | None:
    if not isinstance(metric, str) or metric not in METRICS_WHITELIST:
        return f"metric must be one of {sorted(METRICS_WHITELIST)}"
    return None


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(compose_path: Path | None = None) -> Flask:
    if compose_path is None:
        compose_path = PROJECT_ROOT / "docker-compose.yml"

    app = Flask(__name__, static_folder=str(Path(__file__).parent / "static"))
    app.config["COMPOSE_PATH"] = compose_path
    state = DashboardState()
    app.config["DASH_STATE"] = state

    # Allow tests to inject a custom fetcher
    app.config.setdefault("CLUSTER_FETCHER", None)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _services() -> dict[str, int]:
        return _load_compose(app.config["COMPOSE_PATH"])

    def _fetcher(services: dict[str, int]) -> dict:
        custom = app.config.get("CLUSTER_FETCHER")
        if custom is not None:
            return custom(services)
        return _poll_cluster(services)

    def _json_error(msg: str, code: int = 400):
        return jsonify({"error": msg}), code

    # ------------------------------------------------------------------ #
    # Static / index
    # ------------------------------------------------------------------ #

    @app.route("/")
    def index():
        return app.send_static_file("index.html")

    # ------------------------------------------------------------------ #
    # GET /api/cluster
    # ------------------------------------------------------------------ #

    @app.route("/api/cluster")
    def api_cluster():
        services = _services()
        if not services:
            return _json_error("No docker-compose.yml found or no services defined", 503)

        snapshot = _fetcher(services)
        converged: bool = snapshot["converged"]
        last_conv = state.update_convergence(converged)
        snapshot["last_convergence_seconds"] = last_conv

        # Push history sample
        per_node_totals = {n["id"]: n["totals"] for n in snapshot["nodes"]}
        state.push_history({
            "ts": snapshot["ts"],
            "per_node_totals": per_node_totals,
            "converged": converged,
            "global": snapshot["global"],
        })

        return jsonify(snapshot)

    # ------------------------------------------------------------------ #
    # GET /api/history
    # ------------------------------------------------------------------ #

    @app.route("/api/history")
    def api_history():
        return jsonify(state.get_history())

    # ------------------------------------------------------------------ #
    # GET /api/events
    # ------------------------------------------------------------------ #

    @app.route("/api/events")
    def api_events():
        return jsonify(state.get_events())

    # ------------------------------------------------------------------ #
    # POST /api/visit
    # ------------------------------------------------------------------ #

    @app.route("/api/visit", methods=["POST"])
    def api_visit():
        body = request.get_json(silent=True) or {}
        services = _services()

        node_name = body.get("node")
        metric = body.get("metric")
        n = body.get("n", 1)

        err = _validate_node(node_name, services)
        if err:
            return _json_error(err)
        err = _validate_metric(metric)
        if err:
            return _json_error(err)
        if not isinstance(n, int) or isinstance(n, bool) or n < 1 or n > MAX_N:
            return _json_error(f"n must be an integer between 1 and {MAX_N}")

        port = services[node_name]
        url = f"http://127.0.0.1:{port}/visit?metric={metric}&n={n}"
        try:
            req = urllib.request.Request(url, data=b"", method="POST")
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                result = json.loads(resp.read().decode())
        except Exception as exc:
            state.push_event(f"visit {node_name} {metric} n={n}", f"ERROR: {exc}")
            return _json_error(f"node unreachable: {exc}", 502)

        state.push_event(f"visit {node_name} {metric} n={n}", "ok")
        return jsonify(result)

    # ------------------------------------------------------------------ #
    # POST /api/load
    # ------------------------------------------------------------------ #

    @app.route("/api/load", methods=["POST"])
    def api_load():
        body = request.get_json(silent=True) or {}
        services = _services()

        rate = body.get("rate")
        duration = body.get("duration")
        metric = body.get("metric", "page_views")

        if not isinstance(rate, (int, float)) or isinstance(rate, bool) or rate <= 0 or rate > MAX_RATE:
            return _json_error(f"rate must be a number in (0, {MAX_RATE}]")
        if not isinstance(duration, (int, float)) or isinstance(duration, bool) or duration <= 0 or duration > MAX_DURATION:
            return _json_error(f"duration must be a number in (0, {MAX_DURATION}]")
        err = _validate_metric(metric)
        if err:
            return _json_error(err)

        started = state.start_load(float(rate), float(duration), metric, services)
        if not started:
            return _json_error("a load test is already running", 409)

        state.push_event(f"load rate={rate} dur={duration} metric={metric}", "started")
        return jsonify({"status": "started", "rate": rate, "duration": duration, "metric": metric})

    # ------------------------------------------------------------------ #
    # POST /api/partition
    # ------------------------------------------------------------------ #

    @app.route("/api/partition", methods=["POST"])
    def api_partition():
        body = request.get_json(silent=True) or {}
        services = _services()
        groups = body.get("groups")

        if not isinstance(groups, list) or len(groups) < 2:
            return _json_error("groups must be a list of at least 2 node lists")

        all_nodes: list[str] = []
        for grp in groups:
            if not isinstance(grp, list):
                return _json_error("each group must be a list of node names")
            for n in grp:
                err = _validate_node(n, services)
                if err:
                    return _json_error(err)
                all_nodes.append(n)

        results = {}
        for i, grp in enumerate(groups):
            blocked = [n for j, g in enumerate(groups) for n in g if j != i]
            for node_name in grp:
                port = services[node_name]
                url = f"http://127.0.0.1:{port}/partition"
                try:
                    data = json.dumps({"blocked": blocked}).encode()
                    req = urllib.request.Request(
                        url, data=data,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=3.0) as resp:
                        results[node_name] = json.loads(resp.read().decode())
                except Exception as exc:
                    results[node_name] = {"error": str(exc)}

        state.push_event(f"partition groups={[list(g) for g in groups]}", "ok")
        return jsonify({"results": results})

    # ------------------------------------------------------------------ #
    # POST /api/heal
    # ------------------------------------------------------------------ #

    @app.route("/api/heal", methods=["POST"])
    def api_heal():
        services = _services()
        results = {}
        for node_name, port in services.items():
            url = f"http://127.0.0.1:{port}/partition"
            try:
                req = urllib.request.Request(url, method="DELETE")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    results[node_name] = json.loads(resp.read().decode())
            except Exception as exc:
                results[node_name] = {"error": str(exc)}

        state.push_event("heal", "ok")
        return jsonify({"results": results})

    # ------------------------------------------------------------------ #
    # POST /api/isolate  POST /api/rejoin
    # ------------------------------------------------------------------ #

    @app.route("/api/isolate", methods=["POST"])
    def api_isolate():
        body = request.get_json(silent=True) or {}
        services = _services()
        node_name = body.get("node")
        err = _validate_node(node_name, services)
        if err:
            return _json_error(err)

        result = _run_docker_plain("network", "disconnect", "crdt-net", node_name)
        if result.returncode != 0:
            msg = result.stderr.strip() or "unknown error"
            state.push_event(f"isolate {node_name}", f"ERROR: {msg}")
            return _json_error(f"docker error: {msg}", 502)

        state.push_event(f"isolate {node_name}", "ok")
        return jsonify({"status": "isolated", "node": node_name})

    @app.route("/api/rejoin", methods=["POST"])
    def api_rejoin():
        body = request.get_json(silent=True) or {}
        services = _services()
        node_name = body.get("node")
        err = _validate_node(node_name, services)
        if err:
            return _json_error(err)

        result = _run_docker_plain("network", "connect", "--alias", node_name, "crdt-net", node_name)
        if result.returncode != 0:
            msg = result.stderr.strip() or "unknown error"
            state.push_event(f"rejoin {node_name}", f"ERROR: {msg}")
            return _json_error(f"docker error: {msg}", 502)

        state.push_event(f"rejoin {node_name}", "ok")
        return jsonify({"status": "rejoined", "node": node_name})

    # ------------------------------------------------------------------ #
    # POST /api/kill  POST /api/revive
    # ------------------------------------------------------------------ #

    @app.route("/api/kill", methods=["POST"])
    def api_kill():
        body = request.get_json(silent=True) or {}
        services = _services()
        node_name = body.get("node")
        err = _validate_node(node_name, services)
        if err:
            return _json_error(err)

        result = _run_docker_plain("kill", node_name)
        if result.returncode != 0:
            msg = result.stderr.strip() or "unknown error"
            state.push_event(f"kill {node_name}", f"ERROR: {msg}")
            return _json_error(f"docker error: {msg}", 502)

        state.push_event(f"kill {node_name}", "ok")
        return jsonify({"status": "killed", "node": node_name})

    @app.route("/api/revive", methods=["POST"])
    def api_revive():
        body = request.get_json(silent=True) or {}
        services = _services()
        node_name = body.get("node")
        err = _validate_node(node_name, services)
        if err:
            return _json_error(err)

        result = _run_docker_plain("start", node_name)
        if result.returncode != 0:
            msg = result.stderr.strip() or "unknown error"
            state.push_event(f"revive {node_name}", f"ERROR: {msg}")
            return _json_error(f"docker error: {msg}", 502)

        state.push_event(f"revive {node_name}", "ok")
        return jsonify({"status": "revived", "node": node_name})

    return app
