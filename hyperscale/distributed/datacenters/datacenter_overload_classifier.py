"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass
from hyperscale.distributed.datacenters.datacenter_overload_config import (
    DatacenterOverloadConfig,
    DatacenterOverloadState,
    OVERLOAD_STATE_ORDER,
)

from .datacenter_overload_result import DatacenterOverloadResult
from .datacenter_overload_signals import DatacenterOverloadSignals


class DatacenterOverloadClassifier:
    def __init__(self, config: DatacenterOverloadConfig | None = None) -> None:
        self._config = config or DatacenterOverloadConfig()

    def classify(self, signals: DatacenterOverloadSignals) -> DatacenterOverloadResult:
        worker_overload_ratio = self._calculate_worker_overload_ratio(signals)
        manager_unhealthy_ratio = self._calculate_manager_unhealthy_ratio(signals)
        manager_overload_ratio = self._calculate_manager_overload_ratio(signals)
        capacity_utilization = self._calculate_capacity_utilization(signals)
        leader_overloaded = signals.leader_health_state == "overloaded"

        worker_state = self._classify_by_worker_overload(worker_overload_ratio)
        manager_state = self._classify_by_manager_health(manager_unhealthy_ratio)
        manager_overload_state = self._classify_by_manager_overload(
            manager_overload_ratio, leader_overloaded
        )
        capacity_state = self._classify_by_capacity(capacity_utilization)

        final_state = self._get_worst_state(
            [worker_state, manager_state, manager_overload_state, capacity_state]
        )

        if signals.total_managers == 0 or signals.total_workers == 0:
            final_state = DatacenterOverloadState.UNHEALTHY

        health_severity_weight = self._get_health_severity_weight(final_state)

        return DatacenterOverloadResult(
            state=final_state,
            worker_overload_ratio=worker_overload_ratio,
            manager_unhealthy_ratio=manager_unhealthy_ratio,
            manager_overload_ratio=manager_overload_ratio,
            capacity_utilization=capacity_utilization,
            health_severity_weight=health_severity_weight,
            leader_overloaded=leader_overloaded,
        )

    def _calculate_worker_overload_ratio(
        self, signals: DatacenterOverloadSignals
    ) -> float:
        if signals.total_workers == 0:
            return 0.0
        return signals.overloaded_workers / signals.total_workers

    def _calculate_manager_unhealthy_ratio(
        self, signals: DatacenterOverloadSignals
    ) -> float:
        if signals.total_managers == 0:
            return 1.0
        unhealthy_managers = signals.total_managers - signals.alive_managers
        return unhealthy_managers / signals.total_managers

    def _calculate_manager_overload_ratio(
        self, signals: DatacenterOverloadSignals
    ) -> float:
        if signals.alive_managers == 0:
            return 0.0
        return signals.overloaded_managers / signals.alive_managers

    def _calculate_capacity_utilization(
        self, signals: DatacenterOverloadSignals
    ) -> float:
        if signals.total_cores == 0:
            return 1.0
        used_cores = signals.total_cores - signals.available_cores
        return used_cores / signals.total_cores

    def _classify_by_worker_overload(self, ratio: float) -> DatacenterOverloadState:
        config = self._config
        if ratio >= config.worker_overload_unhealthy_threshold:
            return DatacenterOverloadState.UNHEALTHY
        if ratio >= config.worker_overload_degraded_threshold:
            return DatacenterOverloadState.DEGRADED
        if ratio >= config.worker_overload_busy_threshold:
            return DatacenterOverloadState.BUSY
        return DatacenterOverloadState.HEALTHY

    def _classify_by_manager_health(self, ratio: float) -> DatacenterOverloadState:
        config = self._config
        if ratio >= config.manager_unhealthy_unhealthy_threshold:
            return DatacenterOverloadState.UNHEALTHY
        if ratio >= config.manager_unhealthy_degraded_threshold:
            return DatacenterOverloadState.DEGRADED
        if ratio >= config.manager_unhealthy_busy_threshold:
            return DatacenterOverloadState.BUSY
        return DatacenterOverloadState.HEALTHY

    def _classify_by_manager_overload(
        self,
        ratio: float,
        leader_overloaded: bool,
    ) -> DatacenterOverloadState:
        if leader_overloaded:
            return DatacenterOverloadState.DEGRADED
        if ratio >= 0.5:
            return DatacenterOverloadState.DEGRADED
        if ratio >= 0.3:
            return DatacenterOverloadState.BUSY
        return DatacenterOverloadState.HEALTHY

    def _classify_by_capacity(self, utilization: float) -> DatacenterOverloadState:
        config = self._config
        if utilization >= config.capacity_utilization_unhealthy_threshold:
            return DatacenterOverloadState.UNHEALTHY
        if utilization >= config.capacity_utilization_degraded_threshold:
            return DatacenterOverloadState.DEGRADED
        if utilization >= config.capacity_utilization_busy_threshold:
            return DatacenterOverloadState.BUSY
        return DatacenterOverloadState.HEALTHY

    def _get_worst_state(
        self,
        states: list[DatacenterOverloadState],
    ) -> DatacenterOverloadState:
        return max(states, key=lambda state: OVERLOAD_STATE_ORDER[state])

    def _get_health_severity_weight(self, state: DatacenterOverloadState) -> float:
        config = self._config
        weight_map = {
            DatacenterOverloadState.HEALTHY: config.health_severity_weight_healthy,
            DatacenterOverloadState.BUSY: config.health_severity_weight_busy,
            DatacenterOverloadState.DEGRADED: config.health_severity_weight_degraded,
            DatacenterOverloadState.UNHEALTHY: float("inf"),
        }
        return weight_map.get(state, config.health_severity_weight_degraded)

    def calculate_health_severity_weight(
        self,
        health_bucket: str,
        worker_overload_ratio: float = 0.0,
    ) -> float:
        base_weight = {
            "HEALTHY": self._config.health_severity_weight_healthy,
            "BUSY": self._config.health_severity_weight_busy,
            "DEGRADED": self._config.health_severity_weight_degraded,
            "UNHEALTHY": float("inf"),
        }.get(health_bucket.upper(), self._config.health_severity_weight_degraded)

        overload_adjustment = 1.0 + (worker_overload_ratio * 0.5)

        return base_weight * overload_adjustment

_REHOMED = (
    DatacenterOverloadSignals,
    DatacenterOverloadResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
