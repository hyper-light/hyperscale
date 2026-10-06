"""
Why a datacenter cannot take a job (AD-36 Part 2).
"""

from enum import Enum


class ExclusionReason(str, Enum):
    """The hard exclude that keeps a datacenter out of a routing decision."""

    UNHEALTHY_STATUS = "unhealthy_status"
    INITIALIZING = "initializing"
    NO_REGISTERED_MANAGERS = "no_registered_managers"
    ALL_MANAGERS_CIRCUIT_OPEN = "all_managers_circuit_open"
