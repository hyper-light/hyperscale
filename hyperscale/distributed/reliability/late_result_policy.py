"""
What a datacenter result arriving after its best-effort job completed does (AD-44).
"""

from enum import StrEnum


class LateResultPolicy(StrEnum):
    """AD-44 "Late DC Results" (``Env.BEST_EFFORT_LATE_RESULT_POLICY``).

    ``LOG``: the late result is logged and not aggregated. ``UPDATE``: a job
    completed on reaching its ``min_dcs`` keeps its unreported datacenters
    running until they report or its deadline passes, and each late result
    updates the job's result.
    """

    LOG = "log"
    UPDATE = "update"
