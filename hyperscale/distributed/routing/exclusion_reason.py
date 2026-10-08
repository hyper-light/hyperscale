"""
Why a datacenter cannot take a job (AD-36 Part 2).
"""

from enum import Enum


class ExclusionReason(str, Enum):
    """Why a datacenter is kept out of a routing decision: an AD-36 hard
    exclude, or a constraint of the job's it fails (D-62)."""

    UNHEALTHY_STATUS = "unhealthy_status"
    INITIALIZING = "initializing"
    NO_REGISTERED_MANAGERS = "no_registered_managers"
    ALL_MANAGERS_CIRCUIT_OPEN = "all_managers_circuit_open"
    # D-62: the job set a dispatch latency budget this datacenter's p95
    # exceeds, while enough others meet it.
    OVER_DISPATCH_LATENCY_BUDGET = "over_dispatch_latency_budget"
