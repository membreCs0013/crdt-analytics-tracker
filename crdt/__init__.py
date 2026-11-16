"""CRDT core library for distributed analytics tracking.

Implements state-based Convergent Replicated Data Types (CvRDT):
- GCounter: Grow-only counter based on component-wise maximum.
- PNCounter: Positive-Negative counter backed by two GCounters.
- MetricsStore: Multi-metric container for tracking named counters across nodes.
"""

from crdt.gcounter import GCounter
from crdt.pncounter import PNCounter
from crdt.store import MetricsStore

__all__ = ["GCounter", "PNCounter", "MetricsStore"]
