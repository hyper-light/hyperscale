"""
Circuit Breaker Manager for Gate-to-Manager connections.

Manages per-manager circuit breakers to isolate failures and prevent
cascading failures when a manager becomes unhealthy.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from typing import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from hyperscale.distributed.swim.core import ErrorStats, CircuitState
from hyperscale.distributed.env import Env

from .circuit_breaker_config import CircuitBreakerConfig


class CircuitBreakerManager:
    """
    Manages circuit breakers for gate-to-manager connections.

    Each manager has its own circuit breaker so that failures to one
    manager don't affect dispatch to other managers.
    """

    __slots__ = ("_circuits", "_config", "_lock", "_incarnations", "_is_peer_suspected")

    def __init__(self, env: Env, is_peer_suspected: Callable[[tuple[str, int]], bool]):
        """
        Args:
            env: Circuit breaker thresholds
            is_peer_suspected: Whether phi accrual on this node's edge to a
                peer is past its threshold (AD-52 section 8): a suspected
                peer's circuit counts OPEN -- requests to it fail fast and
                routing passes it by -- for as long as the suspicion lasts,
                however few errors its requests have hit yet.
        """
        self._is_peer_suspected = is_peer_suspected
        cb_config = env.get_circuit_breaker_config()
        self._config = CircuitBreakerConfig(
            max_errors=cb_config["max_errors"],
            window_seconds=cb_config["window_seconds"],
            half_open_after=cb_config["half_open_after"],
        )
        self._circuits: dict[tuple[str, int], ErrorStats] = {}
        self._incarnations: dict[tuple[str, int], int] = {}
        self._lock = asyncio.Lock()

    async def get_circuit(self, manager_addr: tuple[str, int]) -> ErrorStats:
        async with self._lock:
            if manager_addr not in self._circuits:
                self._circuits[manager_addr] = ErrorStats(
                    max_errors=self._config.max_errors,
                    window_seconds=self._config.window_seconds,
                    half_open_after=self._config.half_open_after,
                )
            return self._circuits[manager_addr]

    async def is_circuit_open(self, manager_addr: tuple[str, int]) -> bool:
        async with self._lock:
            if self._is_peer_suspected(manager_addr):
                return True
            circuit = self._circuits.get(manager_addr)
            if not circuit:
                return False
            return circuit.circuit_state == CircuitState.OPEN

    def count_open_circuits(self, manager_addrs: Iterable[tuple[str, int]]) -> int:
        """How many of ``manager_addrs`` have an OPEN circuit right now --
        tripped by errors, or suspected by phi accrual. A half-open circuit
        admits a probe, so its manager still counts as usable -- the same
        rule dispatch applies (``is_circuit_open``)."""
        return sum(
            1
            for manager_addr in manager_addrs
            if self._is_peer_suspected(manager_addr)
            or (
                (circuit := self._circuits.get(manager_addr)) is not None
                and circuit.circuit_state == CircuitState.OPEN
            )
        )

    def get_circuit_status(self, manager_addr: tuple[str, int]) -> dict | None:
        """
        Get circuit breaker status for a specific manager.

        Args:
            manager_addr: (host, port) tuple for the manager.

        Returns:
            Dict with circuit status, or None if manager has no circuit breaker.
        """
        circuit = self._circuits.get(manager_addr)
        if not circuit:
            return None
        return {
            "manager_addr": f"{manager_addr[0]}:{manager_addr[1]}",
            "circuit_state": circuit.circuit_state.name,
            "error_count": circuit.error_count,
            "error_rate": circuit.error_rate,
        }

    def get_all_circuit_status(self) -> dict:
        """
        Get circuit breaker status for all managers.

        Returns:
            Dict with all manager circuit statuses and list of open circuits.
        """
        return {
            "managers": {
                f"{addr[0]}:{addr[1]}": self.get_circuit_status(addr)
                for addr in self._circuits.keys()
            },
            # ``is_circuit_open`` is async; calling it here returned a
            # COROUTINE (always truthy), so every known manager was
            # reported open and every call leaked a "never awaited"
            # warning. This is a sync method, so read the circuit state
            # directly — exactly what the sibling ``get_circuit_status``
            # above does.
            "open_circuits": [
                f"{addr[0]}:{addr[1]}"
                for addr, circuit in self._circuits.items()
                if circuit.circuit_state == CircuitState.OPEN
            ],
        }

    def record_success(self, manager_addr: tuple[str, int]) -> None:
        circuit = self._circuits.get(manager_addr)
        if circuit:
            circuit.record_success()

    async def record_failure(self, manager_addr: tuple[str, int]) -> None:
        circuit = await self.get_circuit(manager_addr)
        circuit.record_failure()

    async def remove_circuit(self, manager_addr: tuple[str, int]) -> None:
        async with self._lock:
            self._circuits.pop(manager_addr, None)

    def clear_all(self) -> None:
        self._circuits.clear()
        self._incarnations.clear()

    async def update_incarnation(
        self, manager_addr: tuple[str, int], incarnation: int
    ) -> bool:
        async with self._lock:
            current_incarnation = self._incarnations.get(manager_addr, 0)
            if incarnation > current_incarnation:
                self._incarnations[manager_addr] = incarnation
                circuit = self._circuits.get(manager_addr)
                if circuit:
                    circuit.reset()
                return True
            return False

_REHOMED = (
    CircuitBreakerConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
