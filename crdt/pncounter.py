"""Positive-Negative Counter (PN-Counter) CRDT implementation.

Mathematical Foundations:
The PN-Counter is a composite state-based CRDT constructed as the direct product
of two independent join-semilattices:
    PN = (G_P, G_N) in S_P x S_N

- G_P (Positive counter): tracks increments across all nodes.
- G_N (Negative counter): tracks decrements across all nodes.
- Value evaluation: value(PN) = value(G_P) - value(G_N).

Merge Properties:
Because the product of two join-semilattices (S_P x S_N, <=, v) with
component-wise join:
    (P_1, N_1) v (P_2, N_2) = (P_1 v P_2, N_1 v N_2)
is itself a valid join-semilattice:
1. Commutative: (P_1 v P_2, N_1 v N_2) = (P_2 v P_1, N_2 v N_1).
2. Associative: ((P_1 v P_2) v P_3, (N_1 v N_2) v N_3) = (P_1 v (P_2 v P_3), N_1 v (N_2 v N_3)).
3. Idempotent: (P v P, N v N) = (P, N).
4. Monotonic: Both P and N grow monotonically; while the net difference (P - N)
   can increase or decrease, the underlying CRDT states strictly advance.
"""

from __future__ import annotations

from typing import Any, Mapping

from crdt.gcounter import GCounter


class PNCounter:
    """State-based Positive-Negative Counter (PN-Counter).

    Supports both increments and decrements by maintaining two separate
    GCounters: one for positive increments (P) and one for decrements (N).
    The net value is P.value() - N.value().
    """

    __slots__ = ("_p", "_n")

    def __init__(
        self,
        p: GCounter | Mapping[str, int] | None = None,
        n: GCounter | Mapping[str, int] | None = None,
    ) -> None:
        """Initialize PNCounter with optional positive and negative state."""
        if isinstance(p, GCounter):
            self._p = p.copy()
        elif p is not None:
            self._p = GCounter(p)
        else:
            self._p = GCounter()

        if isinstance(n, GCounter):
            self._n = n.copy()
        elif n is not None:
            self._n = GCounter(n)
        else:
            self._n = GCounter()

    @property
    def p(self) -> GCounter:
        """Access positive GCounter (read-only reference to copy/state)."""
        return self._p

    @property
    def n(self) -> GCounter:
        """Access negative GCounter (read-only reference to copy/state)."""
        return self._n

    def increment(self, node_id: str, n: int = 1) -> None:
        """Increment the counter for `node_id` by `n`.

        Args:
            node_id: Identifier of the modifying node.
            n: Amount to increment (must be strictly positive integer).
        """
        self._p.increment(node_id, n)

    def decrement(self, node_id: str, n: int = 1) -> None:
        """Decrement the counter for `node_id` by `n`.

        Args:
            node_id: Identifier of the modifying node.
            n: Amount to decrement (must be strictly positive integer).
        """
        self._n.increment(node_id, n)

    def value(self) -> int:
        """Return the current net value (sum(P) - sum(N))."""
        return self._p.value() - self._n.value()

    def merge(self, other: PNCounter) -> bool:
        """Merge another PNCounter into this one.

        Args:
            other: PNCounter instance to merge.

        Returns:
            bool: True if either internal GCounter changed state, False otherwise.

        Raises:
            TypeError: If other is not an instance of PNCounter.
        """
        if not isinstance(other, PNCounter):
            raise TypeError(f"Cannot merge PNCounter with {type(other).__name__}")

        p_changed = self._p.merge(other._p)
        n_changed = self._n.merge(other._n)
        return p_changed or n_changed

    def to_dict(self) -> dict[str, dict[str, int]]:
        """Serialize PNCounter state to a dictionary."""
        return {
            "p": self._p.to_dict(),
            "n": self._n.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> PNCounter:
        """Deserialize dictionary into a PNCounter instance.

        Args:
            d: Mapping containing 'p' and 'n' sub-dictionaries.

        Returns:
            A new PNCounter instance.
        """
        if not isinstance(d, Mapping):
            raise TypeError(f"Expected a mapping, got {type(d).__name__}")

        p_data = d.get("p", {})
        n_data = d.get("n", {})

        p_counter = GCounter.from_dict(p_data)
        n_counter = GCounter.from_dict(n_data)
        return cls(p=p_counter, n=n_counter)

    def copy(self) -> PNCounter:
        """Create an independent copy of this PNCounter."""
        return PNCounter(p=self._p.copy(), n=self._n.copy())

    def __eq__(self, other: object) -> bool:
        """Check semantic equality between two PNCounters."""
        if not isinstance(other, PNCounter):
            return False
        return self._p == other._p and self._n == other._n

    def __repr__(self) -> str:
        """Return developer-friendly string representation."""
        return f"PNCounter(p={self._p!r}, n={self._n!r})"
