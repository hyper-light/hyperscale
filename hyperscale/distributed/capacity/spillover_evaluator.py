"""
Spillover evaluation logic for capacity-aware routing (AD-43).
"""

from __future__ import annotations


from hyperscale.distributed.env.env import Env

from .datacenter_capacity import DatacenterCapacity
from .spillover_config import SpilloverConfig
from .spillover_decision import SpilloverDecision

from hyperscale.distributed.runtime import Clock


class SpilloverEvaluator:
    """
    Evaluate whether a job should spillover to another datacenter.
    """

    def __init__(self, config: SpilloverConfig, clock: Clock) -> None:
        """``clock`` is the one capacity reports are stamped with (the
        aggregator's): staleness is measured on it."""
        self._config = config
        self._clock = clock

    @classmethod
    def from_env(cls, env: Env, clock: Clock):
        """
        Build a SpilloverEvaluator using environment configuration.
        """
        return cls(SpilloverConfig.from_env(env), clock)

    def evaluate(
        self,
        job_cores_required: int,
        primary_capacity: DatacenterCapacity,
        fallback_capacities: list[tuple[DatacenterCapacity, float]],
        primary_rtt_ms: float,
    ) -> SpilloverDecision:
        """
        Evaluate the spillover decision for a job.
        """
        primary_wait = primary_capacity.estimated_wait_for_cores(job_cores_required)
        if not self._config.spillover_enabled:
            return self._no_spillover(
                reason="spillover_disabled",
                primary_capacity=primary_capacity,
                primary_wait=primary_wait,
            )

        if self._is_capacity_stale(primary_capacity):
            return self._no_spillover(
                reason="capacity_stale",
                primary_capacity=primary_capacity,
                primary_wait=primary_wait,
            )

        if primary_capacity.can_serve_immediately(job_cores_required):
            return self._no_spillover(
                reason="primary_has_capacity",
                primary_capacity=primary_capacity,
                primary_wait=0.0,
            )

        if primary_wait <= self._config.max_wait_seconds:
            return self._no_spillover(
                reason="primary_wait_acceptable",
                primary_capacity=primary_capacity,
                primary_wait=primary_wait,
            )

        candidate = self._select_spillover_candidate(
            job_cores_required=job_cores_required,
            fallback_capacities=fallback_capacities,
            primary_rtt_ms=primary_rtt_ms,
            primary_job_cores=max(min(job_cores_required, primary_capacity.total_cores), 1),
        )
        if candidate is None:
            return self._no_spillover(
                reason="no_spillover_with_capacity",
                primary_capacity=primary_capacity,
                primary_wait=primary_wait,
            )

        spillover_capacity, spillover_rtt, latency_penalty = candidate
        spillover_wait = spillover_capacity.estimated_wait_for_cores(job_cores_required)
        if spillover_wait > primary_wait * self._config.min_improvement_ratio:
            return SpilloverDecision(
                should_spillover=False,
                reason="improvement_insufficient",
                primary_dc=primary_capacity.datacenter_id,
                spillover_dc=spillover_capacity.datacenter_id,
                primary_wait_seconds=primary_wait,
                spillover_wait_seconds=spillover_wait,
                latency_penalty_ms=latency_penalty,
            )

        return SpilloverDecision(
            should_spillover=True,
            reason="spillover_improves_wait_time",
            primary_dc=primary_capacity.datacenter_id,
            spillover_dc=spillover_capacity.datacenter_id,
            primary_wait_seconds=primary_wait,
            spillover_wait_seconds=spillover_wait,
            latency_penalty_ms=latency_penalty,
        )

    def _select_spillover_candidate(
        self,
        job_cores_required: int,
        fallback_capacities: list[tuple[DatacenterCapacity, float]],
        primary_rtt_ms: float,
        primary_job_cores: int,
    ) -> tuple[DatacenterCapacity, float, float] | None:
        """
        The nearest fresh fallback with every core the job would use there
        free now, where that is at least what the primary would give it
        (``primary_job_cores``: the job's requirement capped at the
        primary's cores, and at least one). A datacenter serves a job
        larger than itself "immediately" when all its cores are free;
        without the second bound an idle datacenter smaller than the cores
        the primary already had free took the job for good.
        """
        best_candidate: tuple[DatacenterCapacity, float, float] | None = None
        best_score = float("inf")
        for capacity, rtt_ms in fallback_capacities:
            if not (
                capacity.available_cores
                >= min(job_cores_required, capacity.total_cores)
                >= primary_job_cores
            ):
                continue
            if self._is_capacity_stale(capacity):
                continue

            latency_penalty = rtt_ms - primary_rtt_ms
            if latency_penalty > self._config.max_latency_penalty_ms:
                continue

            if latency_penalty < best_score:
                best_score = latency_penalty
                best_candidate = (capacity, rtt_ms, latency_penalty)
        return best_candidate

    def _no_spillover(
        self,
        reason: str,
        primary_capacity: DatacenterCapacity,
        primary_wait: float,
    ) -> SpilloverDecision:
        return SpilloverDecision(
            should_spillover=False,
            reason=reason,
            primary_dc=primary_capacity.datacenter_id,
            spillover_dc=None,
            primary_wait_seconds=primary_wait,
            spillover_wait_seconds=0.0,
            latency_penalty_ms=0.0,
        )

    def _is_capacity_stale(self, capacity: DatacenterCapacity) -> bool:
        now = self._clock.monotonic()
        return capacity.is_stale(now, self._config.capacity_staleness_threshold_seconds)
