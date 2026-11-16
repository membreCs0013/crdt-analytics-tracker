# CRDT-Based Distributed Website Analytics Tracker

A high-performance, fault-tolerant distributed analytics engine powered by state-based Convergent Replicated Data Types (CvRDTs).

Designed to track web metrics (e.g., page views, button clicks, user engagements) across 4–10 distributed nodes without requiring central coordination, consensus rounds (Paxos/Raft), or distributed locks.

---

## CRDT Mathematical Foundations

In distributed systems, the CAP theorem and network realities dictate that partitions, message drops, and variable latencies will occur. This system uses state-based CRDTs (CvRDTs) to provide **Strong Eventual Consistency (SEC)**.

Every CRDT in this library forms a **bounded join-semilattice** $(\mathcal{S}, \le, \sqcup, \bot)$:
- $\mathcal{S}$: Set of states.
- $\le$: Partial order representing causal progress.
- $\sqcup$: Join operator (merge) calculating the least upper bound (LUB).
- $\bot$: Bottom element (empty state with value 0).

### Core Semilattice Properties:
1. **Commutative**: $A \sqcup B = B \sqcup A$  
   Replicas can receive state updates in any arbitrary order without divergence.
2. **Associative**: $(A \sqcup B) \sqcup C = A \sqcup (B \sqcup C)$  
   State merges are independent of intermediate batching, grouping, or routing topologies.
3. **Idempotent**: $A \sqcup A = A$  
   Duplicate gossip deliveries over lossy or at-least-once networks introduce no duplicate counting.
4. **Monotonic**: $A \le A \sqcup B$  
   State progresses strictly forward; counts never regress.

---

## Components

- **`GCounter` (`crdt/gcounter.py`)**:  
  State-based Grow-only Counter. Each node increments only its own entry in a vector $\mathbf{v} \in \mathbb{N}^k$. The merge operation takes the component-wise maximum:
  $$\mathbf{v}_{\text{merged}}[i] = \max(\mathbf{v}_1[i], \mathbf{v}_2[i])$$
  Global value is $\sum_i \mathbf{v}[i]$.

- **`PNCounter` (`crdt/pncounter.py`)**:  
  Positive-Negative Counter supporting both increments and decrements. Built as the direct product of two independent GCounters: $P$ (increments) and $N$ (decrements).
  $$\text{value} = \text{value}(P) - \text{value}(N)$$

- **`MetricsStore` (`crdt/store.py`)**:  
  Container managing multiple named metrics mapped to GCounters. Handles asymmetric metrics (keys present in one replica and not another) and provides lazy metric creation, totals aggregation, per-node breakdown, and serialization.

- **`node` package (`node/`)**:  
  Flask-based cluster node process running Waitress WSGI server with:
  - Atomic JSON disk persistence (`os.replace`) with periodic dirty-state flushing.
  - Background peer-to-peer push-pull gossip loop with $\pm 10\%$ jitter.
  - Runtime network partition simulation (`/partition`).
  - Permissive CORS headers on all endpoints for dashboard interaction.

---

## Project Structure

```
crdt-tracker/
├── crdt/
│   ├── __init__.py               # Public package exports
│   ├── gcounter.py               # Grow-only counter (G-Counter)
│   ├── pncounter.py              # Positive-Negative counter (PN-Counter)
│   └── store.py                  # Multi-metric container (MetricsStore)
├── node/
│   ├── __init__.py               # Node package exports
│   ├── __main__.py               # Entry point: python -m node (Waitress server)
│   ├── app.py                    # Flask application factory (create_app)
│   ├── config.py                 # Config dataclass (CLI + Env variables)
│   ├── gossip.py                 # Push-pull gossip loop & telemetry stats
│   └── persistence.py            # Atomic disk save (os.replace) & loader
├── tests/
│   ├── __init__.py
│   ├── test_gcounter.py          # Unit & Hypothesis property tests for GCounter
│   ├── test_pncounter.py         # Unit & Hypothesis property tests for PNCounter
│   ├── test_store.py             # Unit & Hypothesis property tests for MetricsStore
│   ├── test_convergence_sim.py   # Multi-replica simulation under network chaos
│   ├── test_node_api.py          # Unit tests for Flask endpoints & validation
│   └── test_node_integration.py  # 3-node cluster integration tests (convergence, partition, restart)
├── demo.py                       # Step 1 interactive terminal demo
├── pyproject.toml                # Pytest configuration
├── requirements.txt              # Pinned dependencies (pytest, hypothesis, flask, requests, waitress)
├── .gitignore                    # Git ignore rules
└── README.md                     # Project documentation
```

---

## HTTP API Table

All endpoints return JSON and include permissive CORS headers (`Access-Control-Allow-Origin: *`).

| Method | Endpoint | Description | Status Codes |
|---|---|---|---|
| `POST` | `/visit?metric=<name>&n=<int>` | Increments local count for whitelisted metric. `n` must be in `[1, 1_000_000]`. | `200 OK`, `400 Bad Request` |
| `GET` | `/state` | Returns node ID, per-node breakdown, totals, blocked list, and SHA256 canonical `state_hash`. | `200 OK` |
| `POST` | `/merge` | Push-pull gossip receiver. Merges remote CRDT state and returns local state. | `200 OK`, `400 Bad Request`, `503 Service Unavailable` (partitioned) |
| `GET` | `/metrics` | Telemetry counters (`sent`, `received`, `bytes_sent`, `failures`, `uptime`, etc.). | `200 OK` |
| `GET` | `/health` | Health check probe: `{"status": "ok", "node": "<id>"}`. | `200 OK` |
| `POST` | `/partition` | Simulates network partition. Body: `{"blocked": ["node2"]}` or `{"blocked": ["*"]}`. | `200 OK`, `400 Bad Request` |
| `DELETE` | `/partition` | Heals partition, clearing the blocked peers set. | `200 OK` |
| `OPTIONS`| `/*` | CORS preflight handler. | `204 No Content` |

---

## Starting a 3-Node Cluster in PowerShell

You can manually start a 3-node cluster by opening **3 separate PowerShell terminals** in the `crdt-tracker/` directory:

### Terminal 1 (Node 1 on port 5001)
```powershell
python -m node --node-id node1 --port 5001 --peers "node2=http://127.0.0.1:5002,node3=http://127.0.0.1:5003" --data-dir ./data/node1 --gossip-interval 1.0
```

### Terminal 2 (Node 2 on port 5002)
```powershell
python -m node --node-id node2 --port 5002 --peers "node1=http://127.0.0.1:5001,node3=http://127.0.0.1:5003" --data-dir ./data/node2 --gossip-interval 1.0
```

### Terminal 3 (Node 3 on port 5003)
```powershell
python -m node --node-id node3 --port 5003 --peers "node1=http://127.0.0.1:5001,node2=http://127.0.0.1:5002" --data-dir ./data/node3 --gossip-interval 1.0
```

---

## Example API Calls (PowerShell)

> **Note**: Always invoke `curl.exe` in PowerShell (to avoid invoking the built-in PowerShell `Invoke-WebRequest` alias).

### 1. Health Checks
```powershell
curl.exe -s http://127.0.0.1:5001/health
```

### 2. Record Traffic Visits
```powershell
# Record 100 page views on node1
curl.exe -s -X POST "http://127.0.0.1:5001/visit?metric=page_views&n=100"

# Record 25 clicks on node2
curl.exe -s -X POST "http://127.0.0.1:5002/visit?metric=clicks&n=25"

# Record 50 page views on node3
curl.exe -s -X POST "http://127.0.0.1:5003/visit?metric=page_views&n=50"
```

### 3. Inspect Converged State
Wait ~1-2 seconds for gossip rounds, then query any node to observe identical totals:
```powershell
curl.exe -s http://127.0.0.1:5001/state
curl.exe -s http://127.0.0.1:5002/state
curl.exe -s http://127.0.0.1:5003/state
```

### 4. Simulate Network Partition
Isolate `node3` from `node1` and `node2`:
```powershell
# Isolate node3
curl.exe -s -X POST http://127.0.0.1:5003/partition -H "Content-Type: application/json" -d "{\"blocked\": [\"node1\", \"node2\"]}"
curl.exe -s -X POST http://127.0.0.1:5001/partition -H "Content-Type: application/json" -d "{\"blocked\": [\"node3\"]}"
curl.exe -s -X POST http://127.0.0.1:5002/partition -H "Content-Type: application/json" -d "{\"blocked\": [\"node3\"]}"

# Add visits while partitioned
curl.exe -s -X POST "http://127.0.0.1:5001/visit?metric=page_views&n=30"
curl.exe -s -X POST "http://127.0.0.1:5003/visit?metric=page_views&n=70"

# Check state on node1 vs node3: notice divergent state_hash and totals
curl.exe -s http://127.0.0.1:5001/state
curl.exe -s http://127.0.0.1:5003/state

# Heal partition
curl.exe -s -X DELETE http://127.0.0.1:5001/partition
curl.exe -s -X DELETE http://127.0.0.1:5002/partition
curl.exe -s -X DELETE http://127.0.0.1:5003/partition

# Observe re-convergence
curl.exe -s http://127.0.0.1:5001/state
curl.exe -s http://127.0.0.1:5003/state
```

### 5. Inspect Gossip Telemetry
```powershell
curl.exe -s http://127.0.0.1:5001/metrics
```

---

## Running the Test Suite

```powershell
# Run all tests (unit, property-based, simulation, and integration)
pytest -q

# Run verbose test report
pytest -v

# Run only API unit tests
pytest tests/test_node_api.py -v

# Run 3-node cluster integration tests
pytest tests/test_node_integration.py -v
```


---

## Dashboard

The live dashboard is a host-side Flask app (not containerised) that polls all
cluster nodes in parallel, computes CRDT-merged global totals, and provides
control endpoints that invoke Docker CLI commands directly.

### Why host-side?

Running the dashboard inside a container would require Docker-in-Docker (DinD)
to call `docker exec` or `docker network disconnect`.  DinD requires
`--privileged`, introduces a second daemon, and is fragile in demo settings.
On the host the Docker socket is available natively, and the dashboard can
reach both the cluster's published ports and run `docker exec` for
network-isolated containers that have lost their published ports.

### How global totals are computed (CRDT merge)

Each node's `/state` response includes a `per_node` dict of the form
`{metric: {node_id: count}}`.  This is the full G-Counter vector as seen by
that node.  The dashboard does **not** sum node totals (which would
double-count in a converged cluster).  Instead it computes the LUB:

```
merged[metric][node_id] = max over all reachable nodes of per_node[metric][node_id]
global[metric]          = sum(merged[metric].values())
```

This is identical to merging all GCounters with `.merge()` and reading
`.value()`.  It is correct during partitions (each partition's contribution is
visible separately) and after healing (the max collapses duplicates).

### Exec fallback for isolated nodes

`docker network disconnect crdt-net nodeX` removes the container from the
overlay network, which also drops its published port mapping.  The dashboard
detects this: when an HTTP fetch to `127.0.0.1:PORT` fails it checks whether
the container is running (`docker inspect --format '{{.State.Running}}' nodeX`)
and, if so, calls
`docker exec nodeX python -c 'urllib.request.urlopen("http://localhost:5000/state")'`
to read state from inside the container.  The node is then shown with
`status: "isolated"` and `source: "exec"` in the UI.

### Running the dashboard

```powershell
# 1. Start a 6-node cluster
python cluster/ctl.py up --nodes 6

# 2. Start the dashboard (host Python, port 8080)
python -m dashboard

# 3. Open http://127.0.0.1:8080 in your browser

# 4. Tear down
python cluster/ctl.py down --purge-data
```

Options:

```
python -m dashboard --port 8080 --compose docker-compose.yml
```

### Dashboard layout

| Area | Description |
|------|-------------|
| **Header** | Cluster status badge (CONVERGED / DIVERGED), last convergence time, global totals |
| **Node cards** | Per-node status, per-metric totals, 8-char state_hash chip (colour-coded), "via exec" tag |
| **Stacked bar chart** | Each node's contribution per metric (metric selector dropdown) |
| **Line chart** | Per-node `page_views` total over the last 60 seconds (divergence & reconvergence visible) |
| **Control panel** | Send visit, load test, partition (checkbox groups), heal, isolate/rejoin, kill/revive |
| **Event log** | Last 100 control actions with timestamp and result |

<!-- Screenshot placeholder -->
> **Screenshot**: _add `docs/dashboard-screenshot.png` after a live demo_

### Dashboard API reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/cluster` | Poll all nodes; CRDT-merged global totals |
| GET | `/api/history` | Last 300 cluster snapshots |
| GET | `/api/events` | Last 100 control events |
| POST | `/api/visit` | `{node, metric, n}` |
| POST | `/api/load` | `{rate, duration, metric}` — background, non-blocking |
| POST | `/api/partition` | `{groups: [[...],[...]]}` |
| POST | `/api/heal` | Clear all partition rules |
| POST | `/api/isolate` | `{node}` — docker network disconnect |
| POST | `/api/rejoin` | `{node}` — docker network connect |
| POST | `/api/kill` | `{node}` — SIGKILL |
| POST | `/api/revive` | `{node}` — docker start |

### Running tests

```powershell
# All tests (no Docker needed for dashboard tests)
python -m pytest -q

# Dashboard tests only
python -m pytest tests/test_dashboard_api.py -v
```
