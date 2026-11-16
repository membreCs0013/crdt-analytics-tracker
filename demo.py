"""Interactive demonstration of the CRDT distributed tracker (Step 1).

Demonstrates:
1. 4 distributed tracker nodes recording metrics independently (concurrent updates).
2. Divergence: Each node initially only sees its local increments.
3. Peer-to-peer gossip & state merge: Joining states via the join-semilattice.
4. Strong Eventual Consistency (SEC): All nodes converge to identical totals.
5. Idempotence: Duplicate merges produce 0 changes.
"""

import json
import time
from crdt.store import MetricsStore


def print_banner(title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")


def main() -> None:
    nodes = ["node-paris", "node-london", "node-tokyo", "node-newyork"]
    metrics = ["page_views", "checkout_clicks", "signups"]

    print_banner("1. INITIALIZING DISTRIBUTED ANALYTICS NODES")
    stores: dict[str, MetricsStore] = {nid: MetricsStore(metrics) for nid in nodes}
    for nid in nodes:
        print(f"  [+] Node initialized: {nid:<15} (tracking: {', '.join(metrics)})")

    print_banner("2. SIMULATING CONCURRENT INDEPENDENT TRAFFIC")
    # Each node increments its own metrics independently (no coordination, no locks)
    traffic = {
        "node-paris": [("page_views", 1450), ("checkout_clicks", 120), ("signups", 35)],
        "node-london": [("page_views", 2100), ("checkout_clicks", 195), ("signups", 60)],
        "node-tokyo": [("page_views", 3200), ("checkout_clicks", 310), ("signups", 90)],
        "node-newyork": [("page_views", 4800), ("checkout_clicks", 450), ("signups", 140)],
    }

    for nid, events in traffic.items():
        for metric, count in events:
            stores[nid].increment(metric, nid, count)
        print(f"  Traffic recorded on {nid:<15}: {stores[nid].totals()}")

    print_banner("3. STATE DIVERGENCE BEFORE GOSSIP (LOCAL ISOLATION)")
    print("  Notice that each node only sees its own data locally:\n")
    for nid, store in stores.items():
        print(f"  [{nid}] Total views: {store.value('page_views'):<5} | Checkouts: {store.value('checkout_clicks'):<4} | Signups: {store.value('signups')}")

    print_banner("4. SIMULATING GOSSIP SYNCHRONIZATION ROUNDS")
    print("  Exchanging JSON payloads peer-to-peer across nodes...\n")

    sync_round = 0
    all_converged = False
    while not all_converged and sync_round < 5:
        sync_round += 1
        any_change = False
        print(f"  --- Gossip Round {sync_round} ---")
        for sender in nodes:
            payload = stores[sender].to_dict()
            for receiver in nodes:
                if sender != receiver:
                    remote_store = MetricsStore.from_dict(payload)
                    changed = stores[receiver].merge(remote_store)
                    if changed:
                        any_change = True
                        print(f"    [MERGE] {sender} -> {receiver}: local state ADVANCED")

        if not any_change:
            all_converged = True
            print(f"  ==> Replicas reached complete convergence after {sync_round} rounds!")

    print_banner("5. CONVERGED GLOBAL STATE ACROSS ALL NODES")
    print("  All nodes now possess identical global totals and per-node breakdowns:\n")
    for nid, store in stores.items():
        print(f"  [{nid}]")
        print(f"    Totals: {store.totals()}")
        print(f"    Per-node 'page_views': {store.per_node('page_views')}")

    print_banner("6. IDEMPOTENCE VERIFICATION")
    print("  Sending duplicate gossip messages from Tokyo to Paris...")
    tokyo_payload = stores["node-tokyo"].to_dict()
    changed_again = stores["node-paris"].merge(MetricsStore.from_dict(tokyo_payload))
    print(f"  Result of merging identical state: changed = {changed_again} (Idempotent: No duplicates!)")

    print_banner("DEMO COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
