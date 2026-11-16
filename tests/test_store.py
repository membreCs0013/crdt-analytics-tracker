"""Tests for MetricsStore container."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from crdt.gcounter import GCounter
from crdt.store import MetricsStore

metric_name_st = st.sampled_from(["page_views", "clicks", "signups", "downloads"])
node_id_st = st.sampled_from(["node-1", "node-2", "node-3", "node-4"])
count_st = st.integers(min_value=1, max_value=200)

store_st = st.builds(
    MetricsStore.from_dict,
    d=st.dictionaries(
        keys=metric_name_st,
        values=st.dictionaries(keys=node_id_st, values=count_st, max_size=3),
        max_size=3,
    ),
)


class TestMetricsStoreProperties:
    """Property-based verification of MetricsStore operations."""

    @given(s1=store_st, s2=store_st)
    def test_merge_commutativity(self, s1: MetricsStore, s2: MetricsStore) -> None:
        """Commutativity: S1 v S2 == S2 v S1."""
        a = s1.copy()
        b = s2.copy()
        a.merge(s2)
        b.merge(s1)
        assert a == b
        assert a.totals() == b.totals()

    @given(s1=store_st, s2=store_st, s3=store_st)
    def test_merge_associativity(
        self, s1: MetricsStore, s2: MetricsStore, s3: MetricsStore
    ) -> None:
        """Associativity: (S1 v S2) v S3 == S1 v (S2 v S3)."""
        left = s1.copy()
        left.merge(s2)
        left.merge(s3)

        right = s2.copy()
        right.merge(s3)
        right_combined = s1.copy()
        right_combined.merge(right)

        assert left == right_combined
        assert left.totals() == right_combined.totals()

    @given(s=store_st)
    def test_merge_idempotency(self, s: MetricsStore) -> None:
        """Idempotency: S v S == S (and returns False)."""
        replica = s.copy()
        changed = replica.merge(s)
        assert changed is False
        assert replica == s
        assert replica.totals() == s.totals()

    @given(s1=store_st, s2=store_st)
    def test_merge_monotonicity(self, s1: MetricsStore, s2: MetricsStore) -> None:
        """Monotonicity: totals of merged store >= initial totals for all metrics."""
        initial_totals = s1.totals()
        merged = s1.copy()
        merged.merge(s2)
        merged_totals = merged.totals()

        for metric, initial_val in initial_totals.items():
            assert merged_totals.get(metric, 0) >= initial_val

    @given(s=store_st)
    def test_serialization_roundtrip(self, s: MetricsStore) -> None:
        """Serialization roundtrip: from_dict(to_dict(x)) == x."""
        serialized = s.to_dict()
        restored = MetricsStore.from_dict(serialized)
        assert restored == s
        assert restored.totals() == s.totals()


class TestMetricsStoreUnits:
    """Unit tests for MetricsStore."""

    def test_empty_store(self) -> None:
        store = MetricsStore()
        assert store.totals() == {}
        assert store.value("non_existent") == 0
        assert store.per_node("non_existent") == {}

    def test_predeclared_metrics(self) -> None:
        store = MetricsStore(["page_views", "clicks"])
        assert store.totals() == {"page_views": 0, "clicks": 0}
        assert store.value("page_views") == 0

    def test_lazy_metric_creation_and_increment(self) -> None:
        store = MetricsStore()
        store.increment("page_views", "node-1", 10)
        store.increment("page_views", "node-2", 15)
        store.increment("clicks", "node-1", 3)

        assert store.value("page_views") == 25
        assert store.value("clicks") == 3
        assert store.totals() == {"page_views": 25, "clicks": 3}
        assert store.per_node("page_views") == {"node-1": 10, "node-2": 15}

    def test_asymmetric_merge(self) -> None:
        """Merge must correctly handle metrics present in only one store or both."""
        store_a = MetricsStore()
        store_a.increment("metric_a", "node-1", 5)
        store_a.increment("shared", "node-1", 10)

        store_b = MetricsStore()
        store_b.increment("metric_b", "node-2", 8)
        store_b.increment("shared", "node-2", 20)

        # Merge B into A
        changed = store_a.merge(store_b)
        assert changed is True

        assert store_a.value("metric_a") == 5
        assert store_a.value("metric_b") == 8
        assert store_a.value("shared") == 30
        assert store_a.per_node("shared") == {"node-1": 10, "node-2": 20}

        # Second merge changes nothing
        assert store_a.merge(store_b) is False

    def test_validation(self) -> None:
        store = MetricsStore()
        with pytest.raises(ValueError):
            store.increment("", "node-1", 1)
        with pytest.raises(ValueError):
            store.increment("metric", "node-1", 0)
        with pytest.raises(TypeError):
            store.increment(123, "node-1", 1)  # type: ignore[arg-type]
