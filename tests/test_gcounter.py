"""Tests for GCounter CRDT, including property-based semilattice verification."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from crdt.gcounter import GCounter

# Hypothesis strategies for generating GCounters
node_id_st = st.sampled_from(["node-A", "node-B", "node-C", "node-D", "node-E"])
count_st = st.integers(min_value=1, max_value=500)
gcounter_st = st.builds(
    GCounter,
    state=st.dictionaries(keys=node_id_st, values=count_st, max_size=5),
)


class TestGCounterProperties:
    """Property-based tests verifying the join-semilattice axioms."""

    @given(c1=gcounter_st, c2=gcounter_st)
    def test_merge_commutativity(self, c1: GCounter, c2: GCounter) -> None:
        """Commutativity: A v B == B v A."""
        a = c1.copy()
        b = c2.copy()
        a.merge(c2)
        b.merge(c1)
        assert a == b
        assert a.value() == b.value()
        assert a.to_dict() == b.to_dict()

    @given(c1=gcounter_st, c2=gcounter_st, c3=gcounter_st)
    def test_merge_associativity(self, c1: GCounter, c2: GCounter, c3: GCounter) -> None:
        """Associativity: (A v B) v C == A v (B v C)."""
        # Left grouping: (c1 v c2) v c3
        left = c1.copy()
        left.merge(c2)
        left.merge(c3)

        # Right grouping: c1 v (c2 v c3)
        right = c2.copy()
        right.merge(c3)
        right_combined = c1.copy()
        right_combined.merge(right)

        assert left == right_combined
        assert left.value() == right_combined.value()

    @given(c=gcounter_st)
    def test_merge_idempotency(self, c: GCounter) -> None:
        """Idempotency: A v A == A (and returns False for state change)."""
        replica = c.copy()
        changed = replica.merge(c)
        assert changed is False
        assert replica == c
        assert replica.value() == c.value()

    @given(c1=gcounter_st, c2=gcounter_st)
    def test_merge_monotonicity(self, c1: GCounter, c2: GCounter) -> None:
        """Monotonicity: value(A v B) >= value(A) and value(A v B) >= value(B)."""
        val_c1_before = c1.value()
        val_c2_before = c2.value()

        merged = c1.copy()
        merged.merge(c2)

        assert merged.value() >= val_c1_before
        assert merged.value() >= val_c2_before

    @given(c=gcounter_st)
    def test_serialization_roundtrip(self, c: GCounter) -> None:
        """Serialization: from_dict(to_dict(x)) == x."""
        serialized = c.to_dict()
        restored = GCounter.from_dict(serialized)
        assert restored == c
        assert restored.value() == c.value()
        assert restored.to_dict() == serialized


class TestGCounterUnits:
    """Unit tests for GCounter operations and input validation."""

    def test_empty_counter(self) -> None:
        c = GCounter()
        assert c.value() == 0
        assert c.to_dict() == {}
        assert repr(c) == "GCounter({})"

    def test_single_node_increments(self) -> None:
        c = GCounter()
        c.increment("node-1")
        assert c.value() == 1
        c.increment("node-1", 4)
        assert c.value() == 5
        assert c.to_dict() == {"node-1": 5}

    def test_multi_node_increments(self) -> None:
        c = GCounter()
        c.increment("node-1", 10)
        c.increment("node-2", 20)
        c.increment("node-3", 30)
        assert c.value() == 60
        assert c.to_dict() == {"node-1": 10, "node-2": 20, "node-3": 30}

    def test_reject_non_positive_increments(self) -> None:
        c = GCounter()
        with pytest.raises(ValueError, match="strictly positive"):
            c.increment("node-1", 0)

        with pytest.raises(ValueError, match="strictly positive"):
            c.increment("node-1", -5)

    def test_reject_invalid_types(self) -> None:
        c = GCounter()
        with pytest.raises(TypeError):
            c.increment(123, 1)  # type: ignore[arg-type]

        with pytest.raises(ValueError):
            c.increment("", 1)

        with pytest.raises(TypeError):
            c.increment("node-1", True)  # booleans are not valid steps

        with pytest.raises(TypeError):
            c.increment("node-1", 1.5)  # type: ignore[arg-type]

    def test_merge_tracks_changes(self) -> None:
        c1 = GCounter()
        c2 = GCounter()
        c2.increment("node-1", 5)

        # First merge updates state -> returns True
        changed = c1.merge(c2)
        assert changed is True
        assert c1.value() == 5

        # Second merge with identical state -> returns False
        changed_second = c1.merge(c2)
        assert changed_second is False
        assert c1.value() == 5

    def test_concurrent_increments_converge(self) -> None:
        """Two replicas incrementing concurrently converge after mutual merges."""
        r1 = GCounter()
        r2 = GCounter()

        # Replica 1 records increments
        r1.increment("node-1", 10)
        r1.increment("node-1", 5)

        # Replica 2 records increments concurrently
        r2.increment("node-2", 7)
        r2.increment("node-2", 8)

        # Both merge each other's state
        r1_changed = r1.merge(r2)
        r2_changed = r2.merge(r1)

        assert r1_changed is True
        assert r2_changed is True
        assert r1 == r2
        assert r1.value() == 30
        assert r2.value() == 30
        assert r1.to_dict() == {"node-1": 15, "node-2": 15}

    def test_equality_with_empty_or_other_types(self) -> None:
        c1 = GCounter()
        c2 = GCounter()
        assert c1 == c2
        assert c1 != "not-a-gcounter"
        assert c1 != 0
