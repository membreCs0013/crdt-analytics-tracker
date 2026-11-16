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

---

## Project Structure

```
crdt-tracker/
├── crdt/
│   ├── __init__.py        # Public package exports
│   ├── gcounter.py        # Grow-only counter (G-Counter)
│   ├── pncounter.py       # Positive-Negative counter (PN-Counter)
│   └── store.py           # Multi-metric container (MetricsStore)
├── tests/
│   ├── __init__.py
│   ├── test_gcounter.py   # Unit & Hypothesis property tests for GCounter
│   ├── test_pncounter.py  # Unit & Hypothesis property tests for PNCounter
│   ├── test_store.py      # Unit & Hypothesis property tests for MetricsStore
│   └── test_convergence_sim.py # Multi-replica simulation under network chaos
├── pyproject.toml         # Pytest configuration
├── requirements.txt       # Development dependencies (pytest, hypothesis)
├── .gitignore             # Git ignore rules
└── README.md              # Project documentation
```

---

## Setup & Running Tests

### 1. Requirements
- Python 3.11+ (tested on Python 3.11–3.13)
- `pytest` and `hypothesis`

### 2. Installation
```bash
pip install -r requirements.txt
```

### 3. Run the Test Suite
From within the `crdt-tracker/` directory:

```bash
# Run quiet test summary
pytest -q

# Run verbose test suite
pytest -v

# Run with hypothesis statistics
pytest --hypothesis-show-statistics
```

---

## Test Verification

The test suite validates:
- **Semilattice Axioms**: Automated property-based tests via Hypothesis demonstrating commutativity, associativity, idempotency, and monotonicity across thousands of randomized state combinations.
- **State Mutation Tracking**: Ensuring `merge()` returns `True` if and only if local state advanced.
- **JSON Serialization**: Full round-trip fidelity `from_dict(to_dict(x)) == x`.
- **Chaotic Convergence Simulation**: Simulation of 4 to 10 nodes undergoing concurrent updates, packet drops (25%), packet duplication (20%), and arbitrary arrival order. After gossip synchronization, all nodes reach 100% identical state and exact ground-truth values.
