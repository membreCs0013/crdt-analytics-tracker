"""Convergence simulation under chaotic network conditions.

Simulates N replicas (N in 4..10) performing concurrent updates with:
- Random pairwise state gossip
- Dropped messages (network loss)
- Duplicated messages (at-least-once delivery)
- Out-of-order message arrivals

Verifies that after a final full-synchronization round, ALL replicas reach
identical state and their totals match the exact ground-truth increments.
A fixed random seed is used for deterministic reproducibility.
"""

import random
from collections import defaultdict
from typing import NamedTuple

import pytest

from crdt.store import MetricsStore


class GossipMessage(NamedTuple):
    sender_id: str
    receiver_id: str
    payload: dict[str, dict[str, int]]


@pytest.mark.parametrize("num_replicas", [4, 6, 8, 10])
def test_network_convergence_simulation(num_replicas: int) -> None:
    """Simulate distributed nodes converging despite partitions, loss, and reordering."""
    # Fixed seed for guaranteed determinism
    rng = random.Random(42 + num_replicas)

    node_ids = [f"node-{i:02d}" for i in range(num_replicas)]
    metrics = ["page_views", "clicks", "shares"]

    # Each replica has its own MetricsStore
    replicas: dict[str, MetricsStore] = {
        nid: MetricsStore(metrics) for nid in node_ids
    }

    # Ground truth tracker to verify correctness against theoretical expectation
    ground_truth_totals: dict[str, int] = defaultdict(int)
    ground_truth_per_node: dict[str, dict[str, int]] = {
        m: defaultdict(int) for m in metrics
    }

    # Network in-flight message pool
    network_pool: list[GossipMessage] = []

    # Phase 1: Chaotic concurrency phase (increments + lossy gossip)
    num_rounds = 40
    for _ in range(num_rounds):
        # 1. Random local increments
        for nid in node_ids:
            if rng.random() < 0.6:  # 60% chance of increment in this round
                metric = rng.choice(metrics)
                amount = rng.randint(1, 10)
                replicas[nid].increment(metric, nid, amount)
                ground_truth_totals[metric] += amount
                ground_truth_per_node[metric][nid] += amount

        # 2. Random pairwise gossip transmissions
        for sender_id in node_ids:
            if rng.random() < 0.5:  # 50% chance to initiate gossip
                # Pick a random peer
                peers = [nid for nid in node_ids if nid != sender_id]
                receiver_id = rng.choice(peers)

                # Snapshot sender's state
                payload = replicas[sender_id].to_dict()
                msg = GossipMessage(sender_id, receiver_id, payload)

                # Simulate network behaviors:
                drop_prob = rng.random()
                if drop_prob < 0.25:
                    # Message dropped (network packet loss)
                    continue
                elif drop_prob < 0.45:
                    # Message duplicated (packet retransmission / at-least-once)
                    network_pool.append(msg)
                    network_pool.append(msg)
                else:
                    network_pool.append(msg)

        # 3. Deliver a random subset of in-flight messages out of order
        if network_pool:
            rng.shuffle(network_pool)
            deliver_count = rng.randint(1, max(1, len(network_pool) // 2))
            to_deliver = [network_pool.pop() for _ in range(deliver_count)]
            for msg in to_deliver:
                remote_store = MetricsStore.from_dict(msg.payload)
                replicas[msg.receiver_id].merge(remote_store)

    # Deliver any remaining messages in the network pool
    rng.shuffle(network_pool)
    while network_pool:
        msg = network_pool.pop()
        remote_store = MetricsStore.from_dict(msg.payload)
        replicas[msg.receiver_id].merge(remote_store)

    # Phase 2: Final full-sync round (gossip until convergence)
    # Perform all-to-all synchronization round until no state changes occur
    sync_rounds = 0
    max_sync_rounds = 10
    changed = True
    while changed and sync_rounds < max_sync_rounds:
        changed = False
        sync_rounds += 1
        for sender_id in node_ids:
            sender_dict = replicas[sender_id].to_dict()
            for receiver_id in node_ids:
                if sender_id != receiver_id:
                    remote = MetricsStore.from_dict(sender_dict)
                    if replicas[receiver_id].merge(remote):
                        changed = True

    # Phase 3: Assertions - Strong Eventual Consistency (SEC)
    first_node_id = node_ids[0]
    canonical_store = replicas[first_node_id]
    canonical_dict = canonical_store.to_dict()
    canonical_totals = canonical_store.totals()

    # 1. All replicas must have strictly identical state
    for nid in node_ids[1:]:
        assert replicas[nid] == canonical_store, f"{nid} did not converge with {first_node_id}"
        assert replicas[nid].to_dict() == canonical_dict
        assert replicas[nid].totals() == canonical_totals

    # 2. Replicas must match ground truth values exactly (no lost or duplicated updates)
    for m in metrics:
        assert canonical_totals[m] == ground_truth_totals[m], (
            f"Metric {m} total {canonical_totals[m]} != expected {ground_truth_totals[m]}"
        )
        for nid in node_ids:
            actual_node_val = canonical_store.per_node(m).get(nid, 0)
            expected_node_val = ground_truth_per_node[m][nid]
            assert actual_node_val == expected_node_val, (
                f"Metric {m} for {nid}: {actual_node_val} != expected {expected_node_val}"
            )
