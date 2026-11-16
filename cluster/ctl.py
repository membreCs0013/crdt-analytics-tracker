"""Cluster control CLI for the CRDT analytics distributed tracker.

Subcommands:
    up --nodes N [--gossip-interval X] [--fanout Y]
    down [--volumes] [--purge-data]
    status
    visit --node NODE --metric M --n K
    load --rate R --duration S [--metric M] [--verify]
    partition --groups "node1,node2|node3,node4"
    heal
    isolate NODE
    rejoin NODE
    kill NODE
    revive NODE
    wait-converged [--timeout T]

All output uses ASCII-only characters for Windows PowerShell compatibility.
Expected failures (Docker not running, node unreachable) print a short message
without tracebacks.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path so `cluster` package can be imported
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COMPOSE_FILE = "docker-compose.yml"


def _run_docker(*args: str, capture: bool = False) -> subprocess.CompletedProcess:
    """Run `docker compose ...` with the given arguments.

    Never uses shell=True. Raises CalledProcessError on non-zero exit
    when capture=False.
    """
    cmd = ["docker", "compose", "-f", str(PROJECT_ROOT / COMPOSE_FILE), *args]
    if capture:
        return subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )
    return subprocess.run(cmd, cwd=str(PROJECT_ROOT))


def _run_docker_plain(*args: str) -> subprocess.CompletedProcess:
    """Run `docker ...` (not compose) with the given arguments."""
    cmd = ["docker", *args]
    return subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def _check_docker() -> bool:
    """Return True if the Docker daemon is reachable."""
    result = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _load_compose() -> dict[str, Any]:
    """Parse docker-compose.yml and return a minimal service->port mapping."""
    compose_path = PROJECT_ROOT / COMPOSE_FILE
    if not compose_path.exists():
        return {}

    # Minimal YAML parser: extract service names and host ports
    services: dict[str, int] = {}
    current_service: str | None = None
    in_ports = False
    in_services = False

    with open(compose_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip()
            stripped = line.lstrip()

            if stripped.startswith("services:"):
                in_services = True
                continue

            if not in_services:
                continue

            # Top-level service name (2 spaces indent)
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
                # Format: '5001:5000'
                port_def = stripped.strip("- '\"").split(":")[0]
                try:
                    services[current_service] = int(port_def)
                except ValueError:
                    pass
                in_ports = False

    return services


def _node_url(node: str, services: dict[str, int]) -> str:
    """Return http://127.0.0.1:PORT for a node."""
    port = services.get(node, 5000)
    return f"http://127.0.0.1:{port}"


def _http_get(url: str, timeout: float = 2.0) -> dict | None:
    """HTTP GET; returns parsed JSON or None on failure."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _http_post(url: str, body: dict, timeout: float = 2.0) -> dict | None:
    """HTTP POST with JSON body; returns parsed JSON or None on failure."""
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _http_delete(url: str, timeout: float = 2.0) -> dict | None:
    """HTTP DELETE; returns parsed JSON or None on failure."""
    try:
        req = urllib.request.Request(url, method="DELETE")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _exec_state(node: str) -> dict | None:
    """Read /state via docker exec (fallback when container is network-isolated).

    Uses a minimal urllib call inside the container.
    """
    script = (
        "import urllib.request, json, sys; "
        "r=urllib.request.urlopen('http://localhost:5000/state', timeout=3); "
        "print(r.read().decode())"
    )
    result = _run_docker_plain("exec", node, "python", "-c", script)
    if result.returncode == 0 and result.stdout.strip():
        try:
            return json.loads(result.stdout.strip())
        except json.JSONDecodeError:
            return None
    return None


def _container_running(node: str) -> bool:
    """Return True if the named container is in Running state."""
    result = _run_docker_plain(
        "inspect", "--format", "{{.State.Running}}", node
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _pad(s: str, width: int) -> str:
    """Left-align s in a field of given width (ASCII safe)."""
    return s[:width].ljust(width)


# ---------------------------------------------------------------------------
# Subcommand implementations
# ---------------------------------------------------------------------------

def cmd_up(args: argparse.Namespace) -> int:
    from cluster.gen_compose import main as gen_main
    gen_main([
        "--nodes", str(args.nodes),
        "--gossip-interval", str(args.gossip_interval),
        "--fanout", str(args.fanout),
        "--output", str(PROJECT_ROOT / COMPOSE_FILE),
    ])
    result = _run_docker("up", "-d", "--build")
    return result.returncode


def cmd_down(args: argparse.Namespace) -> int:
    docker_args = ["down"]
    if args.volumes:
        docker_args.append("--volumes")
    result = _run_docker(*docker_args)
    # docker compose down may exit 1 when some containers were already stopped/killed;
    # treat that as success (the cluster IS down).
    if args.purge_data:
        data_dir = PROJECT_ROOT / "data"
        if data_dir.exists():
            import shutil
            for child in data_dir.iterdir():
                if child.is_dir() and child.name.startswith("node"):
                    shutil.rmtree(child, ignore_errors=True)
            print(f"Purged data directories under {data_dir}")
    return result.returncode


def cmd_status(args: argparse.Namespace) -> int:
    services = _load_compose()
    if not services:
        print("No docker-compose.yml found. Run 'up' first.")
        return 1

    # Collect state from all nodes
    rows = []
    hashes = []
    for node in sorted(services.keys()):
        url = _node_url(node, services)
        state = _http_get(f"{url}/state")
        via_exec = False

        if state is None and _container_running(node):
            # Network-isolated container: fall back to docker exec
            state = _exec_state(node)
            via_exec = True

        if state is None:
            rows.append({
                "node": node,
                "health": "UNREACHABLE",
                "totals": {},
                "state_hash": "N/A",
                "blocked": [],
                "via_exec": False,
            })
        else:
            hashes.append(state.get("state_hash", ""))
            rows.append({
                "node": node,
                "health": "OK",
                "totals": state.get("totals", {}),
                "state_hash": state.get("state_hash", "")[:12],
                "blocked": state.get("blocked", []),
                "via_exec": via_exec,
            })

    # Collect all metric names
    all_metrics: list[str] = []
    for row in rows:
        for m in row["totals"]:
            if m not in all_metrics:
                all_metrics.append(m)

    # Build ASCII table
    col_node = 12
    col_health = 14
    col_hash = 14
    col_metric = 12
    col_blocked = 20

    header_parts = [
        _pad("NODE", col_node),
        _pad("HEALTH", col_health),
    ]
    for m in all_metrics:
        header_parts.append(_pad(m.upper()[:col_metric], col_metric))
    header_parts += [
        _pad("STATE_HASH", col_hash),
        _pad("BLOCKED", col_blocked),
    ]
    header = " | ".join(header_parts)
    separator = "-" * len(header)

    print(separator)
    print(header)
    print(separator)

    for row in rows:
        suffix = " (via exec)" if row["via_exec"] else ""
        health_str = row["health"] + suffix
        blocked_str = ",".join(row["blocked"]) if row["blocked"] else "-"
        parts = [
            _pad(row["node"], col_node),
            _pad(health_str, col_health),
        ]
        for m in all_metrics:
            val = str(row["totals"].get(m, "-"))
            parts.append(_pad(val, col_metric))
        parts += [
            _pad(row["state_hash"], col_hash),
            _pad(blocked_str, col_blocked),
        ]
        print(" | ".join(parts))

    print(separator)

    # Convergence check
    reachable_hashes = [h for h in hashes if h]
    if len(reachable_hashes) > 1 and len(set(reachable_hashes)) == 1:
        print("CONVERGED: yes")
    elif reachable_hashes:
        print("CONVERGED: no (hashes differ)")
    else:
        print("CONVERGED: unknown (no reachable nodes)")
    print()

    return 0


def cmd_visit(args: argparse.Namespace) -> int:
    services = _load_compose()
    url = _node_url(args.node, services)
    result = _http_post(f"{url}/visit?metric={args.metric}&n={args.n}", {})
    if result is None:
        print(f"ERROR: Could not reach {args.node} at {url}")
        return 1
    print(json.dumps(result, indent=2))
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    services = _load_compose()
    nodes = sorted(services.keys())
    if not nodes:
        print("No nodes found in docker-compose.yml")
        return 1

    metric = getattr(args, "metric", "page_views")
    rate = args.rate        # requests/second total
    duration = args.duration  # seconds
    total_requests = int(rate * duration)
    per_node = max(1, total_requests // len(nodes))

    print(f"Load test: {total_requests} total requests at {rate} req/s over {duration}s")
    print(f"Metric: {metric}  Nodes: {nodes}")

    success_counts = {n: 0 for n in nodes}
    error_counts = {n: 0 for n in nodes}
    lock = threading.Lock()

    def send_to_node(node: str, count: int) -> None:
        url = _node_url(node, services)
        interval = 1.0 / (rate / len(nodes))
        for _ in range(count):
            resp = _http_post(f"{url}/visit?metric={metric}&n=1", {})
            with lock:
                if resp is not None:
                    success_counts[node] += 1
                else:
                    error_counts[node] += 1
            time.sleep(interval)

    threads = [
        threading.Thread(target=send_to_node, args=(node, per_node))
        for node in nodes
    ]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    total_sent = sum(success_counts.values()) + sum(error_counts.values())
    total_ok = sum(success_counts.values())
    total_err = sum(error_counts.values())
    print(f"\nSent: {total_sent}  Successes: {total_ok}  Errors: {total_err}  Elapsed: {elapsed:.1f}s")

    if getattr(args, "verify", False):
        print("Waiting for convergence...")
        rc = _wait_converged_impl(timeout=30.0, services=services)
        if rc != 0:
            print("VERIFY: FAIL (did not converge)")
            return 1
        # Check totals
        any_state = None
        for node in nodes:
            url = _node_url(node, services)
            s = _http_get(f"{url}/state")
            if s:
                any_state = s
                break
        if any_state:
            actual = any_state.get("totals", {}).get(metric, 0)
            # successes <= actual <= successes + errors (timed-out may still apply)
            if total_ok <= actual <= total_ok + total_err:
                print(f"VERIFY: PASS ({total_ok} <= {actual} <= {total_ok + total_err})")
            else:
                print(f"VERIFY: FAIL (actual={actual} outside [{total_ok}, {total_ok + total_err}])")
                return 1
    return 0


def cmd_partition(args: argparse.Namespace) -> int:
    services = _load_compose()
    groups_raw = args.groups
    # Parse "node1,node2|node3,node4" into groups
    groups = [g.strip().split(",") for g in groups_raw.split("|")]
    if len(groups) < 2:
        print("ERROR: At least 2 groups required, separated by '|'")
        return 1

    # For each group, block all nodes outside its group
    for i, group in enumerate(groups):
        blocked = [
            n.strip() for j, grp in enumerate(groups)
            for n in grp if j != i
        ]
        for node_name in group:
            node_name = node_name.strip()
            url = _node_url(node_name, services)
            resp = _http_post(f"{url}/partition", {"blocked": blocked})
            if resp is not None:
                print(f"  {node_name}: blocked={resp.get('blocked', [])}")
            else:
                print(f"  {node_name}: UNREACHABLE")
    return 0


def cmd_heal(args: argparse.Namespace) -> int:
    services = _load_compose()
    for node in sorted(services.keys()):
        url = _node_url(node, services)
        resp = _http_delete(f"{url}/partition")
        if resp is not None:
            print(f"  {node}: healed (blocked={resp.get('blocked', [])})")
        else:
            print(f"  {node}: UNREACHABLE")
    return 0


def cmd_isolate(args: argparse.Namespace) -> int:
    """Real network partition: disconnect container from crdt-net."""
    result = _run_docker_plain("network", "disconnect", "crdt-net", args.node)
    if result.returncode == 0:
        print(f"Isolated {args.node} from crdt-net (network disconnected)")
        print("Note: published ports are also lost until rejoin.")
    else:
        print(f"ERROR: Failed to isolate {args.node}: {result.stderr.strip()}")
        return 1
    return 0


def cmd_rejoin(args: argparse.Namespace) -> int:
    """Re-attach container to crdt-net with its hostname alias restored."""
    # --alias is mandatory: without it the DNS name 'nodeX' is lost
    result = _run_docker_plain(
        "network", "connect", "--alias", args.node, "crdt-net", args.node
    )
    if result.returncode == 0:
        print(f"Rejoined {args.node} to crdt-net (alias={args.node})")
    else:
        print(f"ERROR: Failed to rejoin {args.node}: {result.stderr.strip()}")
        return 1
    return 0


def cmd_kill(args: argparse.Namespace) -> int:
    """Send SIGKILL to a container (hard kill, no graceful flush)."""
    result = _run_docker_plain("kill", args.node)
    if result.returncode == 0:
        print(f"Killed {args.node} (SIGKILL)")
    else:
        print(f"ERROR: Failed to kill {args.node}: {result.stderr.strip()}")
        return 1
    return 0


def cmd_revive(args: argparse.Namespace) -> int:
    """Start a stopped/killed container."""
    result = _run_docker_plain("start", args.node)
    if result.returncode == 0:
        print(f"Revived {args.node}")
    else:
        print(f"ERROR: Failed to revive {args.node}: {result.stderr.strip()}")
        return 1
    return 0


def _wait_converged_impl(timeout: float, services: dict[str, int]) -> int:
    """Poll until all reachable running nodes share one state_hash.

    Returns 0 on convergence, 1 on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        hashes: list[str] = []
        for node in sorted(services.keys()):
            if not _container_running(node):
                continue
            url = _node_url(node, services)
            state = _http_get(f"{url}/state")
            if state is None:
                # Try exec fallback for isolated containers
                state = _exec_state(node)
            if state:
                hashes.append(state.get("state_hash", ""))

        if hashes and len(set(hashes)) == 1:
            return 0
        time.sleep(0.5)
    return 1


def cmd_wait_converged(args: argparse.Namespace) -> int:
    services = _load_compose()
    if not services:
        print("No docker-compose.yml found.")
        return 1

    timeout = getattr(args, "timeout", 60.0)
    print(f"Waiting for convergence (timeout={timeout}s)...")
    start = time.time()
    rc = _wait_converged_impl(timeout=timeout, services=services)
    elapsed = time.time() - start
    if rc == 0:
        print(f"Converged in {elapsed:.1f}s")
    else:
        print(f"Timed out after {elapsed:.1f}s — nodes did not converge")
    return rc


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python cluster/ctl.py",
        description="CRDT analytics cluster control CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # up
    p_up = sub.add_parser("up", help="Generate compose file and start the cluster")
    p_up.add_argument("--nodes", "-n", type=int, required=True, help="Number of nodes (4-10)")
    p_up.add_argument("--gossip-interval", type=float, default=1.0)
    p_up.add_argument("--fanout", type=int, default=2)

    # down
    p_down = sub.add_parser("down", help="Stop and remove the cluster")
    p_down.add_argument("--volumes", action="store_true")
    p_down.add_argument("--purge-data", action="store_true",
                        help="Also delete ./data/node* directories")

    # status
    sub.add_parser("status", help="Print cluster status table")

    # visit
    p_visit = sub.add_parser("visit", help="Send a single visit to a node")
    p_visit.add_argument("--node", required=True)
    p_visit.add_argument("--metric", required=True)
    p_visit.add_argument("--n", type=int, default=1)

    # load
    p_load = sub.add_parser("load", help="Run a load test across all nodes")
    p_load.add_argument("--rate", type=float, required=True, help="Requests/second total")
    p_load.add_argument("--duration", type=float, required=True, help="Duration in seconds")
    p_load.add_argument("--metric", default="page_views")
    p_load.add_argument("--verify", action="store_true",
                        help="After load: wait-converged and check totals")

    # partition
    p_part = sub.add_parser("partition", help="App-level partition simulation")
    p_part.add_argument(
        "--groups", required=True,
        help='Partition groups, e.g. "node1,node2|node3,node4"',
    )

    # heal
    sub.add_parser("heal", help="Clear all partition rules on all nodes")

    # isolate
    p_iso = sub.add_parser("isolate", help="Real network isolation (docker network disconnect)")
    p_iso.add_argument("node", help="Node to isolate")

    # rejoin
    p_rej = sub.add_parser("rejoin", help="Re-attach to crdt-net with DNS alias restored")
    p_rej.add_argument("node", help="Node to rejoin")

    # kill
    p_kill = sub.add_parser("kill", help="SIGKILL a container")
    p_kill.add_argument("node", help="Node to kill")

    # revive
    p_rev = sub.add_parser("revive", help="Start a stopped/killed container")
    p_rev.add_argument("node", help="Node to revive")

    # wait-converged
    p_wc = sub.add_parser("wait-converged", help="Poll until all nodes share one state_hash")
    p_wc.add_argument("--timeout", type=float, default=60.0)

    return parser


def main(argv: list[str] | None = None) -> int:
    if not _check_docker():
        print("ERROR: Docker daemon is not running or not reachable.")
        print("Start Docker Desktop and try again.")
        return 1

    parser = build_parser()
    args = parser.parse_args(argv)

    dispatch = {
        "up": cmd_up,
        "down": cmd_down,
        "status": cmd_status,
        "visit": cmd_visit,
        "load": cmd_load,
        "partition": cmd_partition,
        "heal": cmd_heal,
        "isolate": cmd_isolate,
        "rejoin": cmd_rejoin,
        "kill": cmd_kill,
        "revive": cmd_revive,
        "wait-converged": cmd_wait_converged,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        print(f"Unknown command: {args.command}")
        return 1

    try:
        return handler(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
