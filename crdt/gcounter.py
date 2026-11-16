"""Grow-only Counter (G-Counter) CRDT implementation.

Mathematical Foundations (Join-Semilattice):
A state-based Convergent Replicated Data Type (CvRDT) relies on a bounded
join-semilattice (S, <=, v) where:
- S is the set of valid states (here, vectors in N^k indexed by node_id).
- <= is a partial order representing causal progress: A <= B iff for all i, A[i] <= B[i].
- v (join / merge) computes the least upper bound (LUB): (A v B)[i] = max(A[i], B[i]).

Merge Properties:
1. Commutative: A v B = B v A.
   Since max(A[i], B[i]) == max(B[i], A[i]), message delivery order across
   distributed nodes does not affect the merged state.
2. Associative: (A v B) v C = A v (B v C).
   Since max(max(a, b), c) == max(a, max(b, c)), intermediate gossip aggregation,
   batching, or arbitrary network topologies produce identical convergence.
3. Idempotent: A v A = A.
   Since max(a, a) == a, duplicate message transmissions over lossy or at-least-once
   networks cause no state drift or duplicate counts.
4. Monotonic: A <= (A v B).
   The state progresses strictly upward in the semilattice; counter values never decrease.
"""

from __future__ import annotations

from typing import Any, Mapping


class GCounter:
    """State-based Grow-only Counter (G-Counter).

    Each participating node is identified by a unique `node_id` and is only
    permitted to increment its own entry. Replicas synchronize state by merging
    their vectors using component-wise maximum.
    """

    __slots__ = ("_state",)

    def __init__(self, state: Mapping[str, int] | None = None) -> None:
        """Initialize GCounter with optional initial state mapping."""
        self._state: dict[str, int] = {}
        if state is not None:
            for node, count in state.items():
                if not isinstance(node, str) or not node:
                    raise TypeError("node_id must be a non-empty string")
                if isinstance(count, bool) or not isinstance(count, int):
                    raise TypeError(f"Counter values must be integers, got {type(count).__name__}")
                if count < 0:
                    raise ValueError(f"Counter values cannot be negative, got {count}")
                if count > 0:
                    self._state[node] = count

    def increment(self, node_id: str, n: int = 1) -> None:
        """Increment the entry belonging to `node_id` by `n`.

        Args:
            node_id: Unique identifier of the node performing the increment.
            n: Amount to increment (must be a strictly positive integer).

        Raises:
            TypeError: If node_id is not a string or n is not an integer.
            ValueError: If node_id is empty or n <= 0.
        """
        if not isinstance(node_id, str):
            raise TypeError(f"node_id must be a string, got {type(node_id).__name__}")
        if not node_id:
            raise ValueError("node_id cannot be empty")
        if isinstance(n, bool) or not isinstance(n, int):
            raise TypeError(f"Increment step n must be an integer, got {type(n).__name__}")
        if n <= 0:
            raise ValueError(f"Increment step n must be strictly positive (n > 0), got {n}")

        self._state[node_id] = self._state.get(node_id, 0) + n

    def value(self) -> int:
        """Return the current aggregate value of the counter across all nodes."""
        return sum(self._state.values())

    def merge(self, other: GCounter) -> bool:
        """Merge state from `other` counter into `self` using component-wise max.

        This computes the least upper bound (LUB) of the two states in the
        join-semilattice.

        Args:
            other: Another GCounter instance to merge into this one.

        Returns:
            bool: True if the local state was updated (at least one component
                  increased), False if the local state was already subsuming
                  `other` (no change).

        Raises:
            TypeError: If other is not an instance of GCounter.
        """
        if not isinstance(other, GCounter):
            raise TypeError(f"Cannot merge GCounter with {type(other).__name__}")

        changed = False
        for node_id, remote_val in other._state.items():
            local_val = self._state.get(node_id, 0)
            if remote_val > local_val:
                self._state[node_id] = remote_val
                changed = True

        return changed

    def to_dict(self) -> dict[str, int]:
        """Serialize counter state to a plain dictionary suitable for JSON transport."""
        return dict(self._state)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> GCounter:
        """Deserialize a dictionary into a GCounter instance.

        Args:
            d: Mapping of node_id to counter value.

        Returns:
            A new GCounter instance.
        """
        if not isinstance(d, Mapping):
            raise TypeError(f"Expected a mapping, got {type(d).__name__}")

        validated: dict[str, int] = {}
        for k, v in d.items():
            if not isinstance(k, str) or not k:
                raise TypeError("node_id keys must be non-empty strings")
            if isinstance(v, bool) or not isinstance(v, int):
                raise TypeError(f"Counter values must be integers, got {type(v).__name__}")
            if v < 0:
                raise ValueError(f"Counter values cannot be negative, got {v}")
            if v > 0:
                validated[k] = v

        return cls(validated)

    def copy(self) -> GCounter:
        """Create an independent copy of this GCounter."""
        return GCounter(self._state)

    def __eq__(self, other: object) -> bool:
        """Check semantic equality between two GCounters.

        Two GCounters are equal iff they assign identical counts to all nodes
        (treating missing node entries as 0).
        """
        if not isinstance(other, GCounter):
            return False

        all_keys = set(self._state.keys()) | set(other._state.keys())
        return all(self._state.get(k, 0) == other._state.get(k, 0) for k in all_keys)

    def __repr__(self) -> str:
        """Return developer-friendly string representation."""
        return f"GCounter({self._state!r})"
