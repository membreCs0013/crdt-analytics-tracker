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
