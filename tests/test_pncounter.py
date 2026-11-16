"""Tests for PNCounter CRDT, including property-based verification."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from crdt.gcounter import GCounter
from crdt.pncounter import PNCounter

node_id_st = st.sampled_from(["node-A", "node-B", "node-C", "node-D"])
count_st = st.integers(min_value=1, max_value=300)

pncounter_st = st.builds(
    PNCounter,
    p=st.builds(GCounter, state=st.dictionaries(keys=node_id_st, values=count_st, max_size=4)),
    n=st.builds(GCounter, state=st.dictionaries(keys=node_id_st, values=count_st, max_size=4)),
)


class TestPNCounterProperties:
    """Property-based verification of PN-Counter semilattice laws."""

    @given(c1=pncounter_st, c2=pncounter_st)
    def test_merge_commutativity(self, c1: PNCounter, c2: PNCounter) -> None:
        """Commutativity: A v B == B v A."""
        a = c1.copy()
        b = c2.copy()
        a.merge(c2)
        b.merge(c1)
        assert a == b
        assert a.value() == b.value()
        assert a.to_dict() == b.to_dict()

    @given(c1=pncounter_st, c2=pncounter_st, c3=pncounter_st)
    def test_merge_associativity(self, c1: PNCounter, c2: PNCounter, c3: PNCounter) -> None:
        """Associativity: (A v B) v C == A v (B v C)."""
        left = c1.copy()
        left.merge(c2)
        left.merge(c3)

        right = c2.copy()
        right.merge(c3)
        right_combined = c1.copy()
        right_combined.merge(right)

        assert left == right_combined
        assert left.value() == right_combined.value()

    @given(c=pncounter_st)
    def test_merge_idempotency(self, c: PNCounter) -> None:
        """Idempotency: A v A == A (returns False on duplicate merge)."""
        replica = c.copy()
        changed = replica.merge(c)
        assert changed is False
        assert replica == c
        assert replica.value() == c.value()

    @given(c1=pncounter_st, c2=pncounter_st)
    def test_internal_monotonicity(self, c1: PNCounter, c2: PNCounter) -> None:
        """Both P and N grow monotonically during a merge."""
        p1_val = c1.p.value()
        n1_val = c1.n.value()

        merged = c1.copy()
        merged.merge(c2)

        assert merged.p.value() >= p1_val
        assert merged.n.value() >= n1_val
        assert merged.p.value() >= c2.p.value()
        assert merged.n.value() >= c2.n.value()

    @given(c=pncounter_st)
    def test_serialization_roundtrip(self, c: PNCounter) -> None:
        """Serialization roundtrip: from_dict(to_dict(x)) == x."""
        serialized = c.to_dict()
        restored = PNCounter.from_dict(serialized)
        assert restored == c
        assert restored.value() == c.value()
        assert restored.to_dict() == serialized


class TestPNCounterUnits:
    """Unit tests for PN-Counter functionality."""

    def test_initial_state(self) -> None:
        c = PNCounter()
        assert c.value() == 0
        assert c.to_dict() == {"p": {}, "n": {}}

    def test_increment_and_decrement(self) -> None:
        c = PNCounter()
        c.increment("node-1", 10)
        assert c.value() == 10
        c.decrement("node-1", 4)
        assert c.value() == 6
        c.decrement("node-2", 10)
        assert c.value() == -4  # PN counters support negative net values

    def test_validation(self) -> None:
        c = PNCounter()
        with pytest.raises(ValueError):
            c.increment("node-1", 0)
        with pytest.raises(ValueError):
            c.decrement("node-1", -1)
        with pytest.raises(TypeError):
            c.increment(123, 1)  # type: ignore[arg-type]

    def test_concurrent_increments_and_decrements_converge(self) -> None:
        """Concurrent increments and decrements converge to identical value."""
        r1 = PNCounter()
        r2 = PNCounter()

        # Replica 1 increments on node-1, decrements on node-1
        r1.increment("node-1", 15)
        r1.decrement("node-1", 5)

        # Replica 2 increments on node-2, decrements on node-2
        r2.increment("node-2", 20)
        r2.decrement("node-2", 12)

        # Mutual merge
        r1.merge(r2)
        r2.merge(r1)

        assert r1 == r2
        # (15 + 20) - (5 + 12) = 35 - 17 = 18
        assert r1.value() == 18
        assert r2.value() == 18

    def test_merge_returns_change_status(self) -> None:
        r1 = PNCounter()
        r2 = PNCounter()
        r2.decrement("node-1", 5)

        assert r1.merge(r2) is True
        assert r1.merge(r2) is False
