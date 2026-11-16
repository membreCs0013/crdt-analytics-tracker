"""MetricsStore CRDT container.

Mathematical Foundations:
The MetricsStore models a mapping from MetricId to GCounter semilattices:
    M: MetricId -> S_G

Where missing keys are implicitly mapped to the bottom element of the GCounter
semilattice:
    bottom = GCounter() with value 0.

Point-wise Join:
For two stores M_1 and M_2, the join (merge) is defined point-wise:
    (M_1 v M_2)(m) = M_1(m) v M_2(m)

Because the point-wise product/mapping of join-semilattices is itself a
valid join-semilattice:
1. Commutative: (M_1 v M_2)(m) = M_1(m) v M_2(m) = M_2(m) v M_1(m) = (M_2 v M_1)(m).
2. Associative: ((M_1 v M_2) v M_3)(m) = (M_1 v (M_2 v M_3))(m).
3. Idempotent: (M v M)(m) = M(m) v M(m) = M(m).
4. Monotonic: For all metrics m, M(m) <= (M v M')(m).
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from crdt.gcounter import GCounter


class MetricsStore:
    """Multi-metric storage backed by state-based G-Counters.

    Metrics can be declared upfront or created lazily on first access.
    Supports concurrent incrementation across distributed nodes and safe
    peer-to-peer state merging.
    """

    __slots__ = ("_metrics",)

    def __init__(self, metrics: Iterable[str] | None = None) -> None:
        """Initialize MetricsStore with optional pre-declared metric names."""
        self._metrics: dict[str, GCounter] = {}
        if metrics is not None:
            for metric in metrics:
                if not isinstance(metric, str) or not metric:
                    raise TypeError("Metric name must be a non-empty string")
                self._metrics[metric] = GCounter()

    def increment(self, metric: str, node_id: str, n: int = 1) -> None:
        """Increment a named metric for a specific node.

        If the metric does not yet exist, it is created lazily.

        Args:
            metric: The name of the metric (e.g., 'page_views', 'button_clicks').
            node_id: The ID of the node recording the increment.
            n: Amount to increment (must be strictly positive integer).

        Raises:
            TypeError: If metric or node_id is invalid.
            ValueError: If metric or node_id is empty, or n <= 0.
        """
        if not isinstance(metric, str):
            raise TypeError(f"Metric name must be a string, got {type(metric).__name__}")
        if not metric:
            raise ValueError("Metric name cannot be empty")

        if metric not in self._metrics:
            self._metrics[metric] = GCounter()

        self._metrics[metric].increment(node_id, n)

    def value(self, metric: str) -> int:
        """Return the current aggregated value of a given metric.

        Returns 0 if the metric does not exist.
        """
        counter = self._metrics.get(metric)
        return counter.value() if counter is not None else 0

    def totals(self) -> dict[str, int]:
        """Return a mapping of all recorded metrics to their global total values."""
        return {metric: counter.value() for metric, counter in self._metrics.items()}

    def per_node(self, metric: str) -> dict[str, int]:
        """Return a node-by-node breakdown of counts for a given metric.

        Returns an empty dict if the metric has no recorded counts.
        """
        counter = self._metrics.get(metric)
        return counter.to_dict() if counter is not None else {}

    def merge(self, other: MetricsStore) -> bool:
        """Merge state from `other` store into `self`.

        Handles asymmetric metrics (present in only one store or both).

        Args:
            other: MetricsStore to merge into this instance.

        Returns:
            bool: True if any local metric was added or updated, False if no change.

        Raises:
            TypeError: If other is not an instance of MetricsStore.
        """
        if not isinstance(other, MetricsStore):
            raise TypeError(f"Cannot merge MetricsStore with {type(other).__name__}")

        changed = False
        for metric, remote_counter in other._metrics.items():
            if metric not in self._metrics:
                # Metric exists on remote but not locally
                self._metrics[metric] = remote_counter.copy()
                if remote_counter.value() > 0 or remote_counter.to_dict():
                    changed = True
            else:
                # Metric exists on both sides; join via GCounter.merge
                if self._metrics[metric].merge(remote_counter):
                    changed = True

        return changed

    def to_dict(self) -> dict[str, dict[str, int]]:
        """Serialize entire store into a dictionary suitable for JSON serialization."""
        return {metric: counter.to_dict() for metric, counter in self._metrics.items()}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> MetricsStore:
        """Deserialize dictionary into a MetricsStore instance.

        Args:
            d: Mapping of metric name to GCounter dictionary.

        Returns:
            A new MetricsStore instance.
        """
        if not isinstance(d, Mapping):
            raise TypeError(f"Expected a mapping, got {type(d).__name__}")

        store = cls()
        for metric, counter_dict in d.items():
            if not isinstance(metric, str) or not metric:
                raise TypeError("Metric name must be a non-empty string")
            store._metrics[metric] = GCounter.from_dict(counter_dict)

        return store

    def copy(self) -> MetricsStore:
        """Create an independent deep copy of this store."""
        new_store = MetricsStore()
        for metric, counter in self._metrics.items():
            new_store._metrics[metric] = counter.copy()
        return new_store

    def __eq__(self, other: object) -> bool:
        """Check semantic equality between two MetricsStores.

        Missing metrics are treated as equivalent to an empty GCounter (value 0).
        """
        if not isinstance(other, MetricsStore):
            return False

        all_metrics = set(self._metrics.keys()) | set(other._metrics.keys())
        empty = GCounter()
        for m in all_metrics:
            c1 = self._metrics.get(m, empty)
            c2 = other._metrics.get(m, empty)
            if c1 != c2:
                return False
        return True

    def __repr__(self) -> str:
        """Return developer-friendly string representation."""
        return f"MetricsStore({self._metrics!r})"
