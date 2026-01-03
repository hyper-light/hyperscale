"""
Routing module for distributed job assignment.

Provides consistent hashing for deterministic job-to-node mapping,
enabling stable ownership and efficient failover.
"""

from .consistent_hash import ConsistentHashRing

__all__ = ["ConsistentHashRing"]
