"""
Worker registry module.

Handles manager registration, health tracking, and peer management.
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import ManagerInfo
from hyperscale.distributed.swim.core import ErrorStats, CircuitState

from hyperscale.distributed.runtime import Clock, RealClock

from .models.manager_circuit_lookup_error import ManagerCircuitLookupError
from .models.manager_circuit_status import ManagerCircuitStatus
from .models.manager_circuit_summary import ManagerCircuitSummary


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class WorkerRegistry:
    """
    Manages manager registration and health tracking for worker.

    Handles registration with managers, tracks health status,
    and manages circuit breakers for communication failures.
    """

    def __init__(
        self,
        logger: "Logger",
        recovery_jitter_min: float = 0.0,
        recovery_jitter_max: float = 1.0,
        recovery_semaphore_size: int = 5,
        *,
        select_manager: Callable[[set[str]], str | None],
        circuit_breaker_config: dict[str, int | float],
        forget_manager_backpressure: Callable[[str], None],
    ) -> None:
        """
        Initialize worker registry.

        Args:
            logger: Logger instance for logging
            recovery_jitter_min: Minimum jitter for recovery operations
            recovery_jitter_max: Maximum jitter for recovery operations
            recovery_semaphore_size: Concurrent recovery limit
            select_manager: AD-28 selection over a set of healthy manager
                ids (weighted rendezvous + power of two choices + EWMA);
                returns the chosen id or None when it cannot choose
            circuit_breaker_config: The configured breaker for each manager
                link (``Env.get_circuit_breaker_config``)
            forget_manager_backpressure: Drops a removed manager's AD-23
                backpressure signal (``WorkerState.remove_manager_backpressure``),
                so a dead manager's last level never pins the worker
        """
        self._logger: "Logger" = logger
        self._select_manager: Callable[[set[str]], str | None] = select_manager
        self._circuit_breaker_config = circuit_breaker_config
        self._forget_manager_backpressure: Callable[[str], None] = forget_manager_backpressure
        self._recovery_jitter_min: float = recovery_jitter_min
        self._recovery_jitter_max: float = recovery_jitter_max
        self._recovery_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            recovery_semaphore_size
        )

        # Hook the cluster-connection lifecycle owner. Set by the
        # worker server after both registry and connection exist
        # (chicken/egg: connection needs ``_get_healthy_manager_count``
        # which reads this registry). Called sync after every mutation
        # of ``_healthy_manager_ids`` so the connection state stays in
        # sync with the registry's view.
        self._on_healthy_set_changed: Callable[[], None] | None = None

        # Manager tracking
        self._known_managers: dict[str, ManagerInfo] = {}
        # The manager id directly confirmed at each TCP address. A
        # restarted manager comes back at the same address under a new
        # id; only direct evidence (its registration exchange, its own
        # heartbeat) moves the address to the new id, so hearsay manager
        # lists cannot resurrect a dead incarnation.
        self._manager_id_by_addr: dict[tuple[str, int], str] = {}
        self._manager_leave_udp_addrs: dict[tuple[str, int], None] = {}
        self._healthy_manager_ids: set[str] = set()
        self._primary_manager_id: str | None = None
        self._manager_unhealthy_since: dict[str, float] = {}

        # Circuit breakers per manager
        self._manager_circuits: dict[str, ErrorStats] = {}
        self._manager_addr_circuits: dict[tuple[str, int], ErrorStats] = {}

        # State management
        self._manager_state_locks: dict[str, asyncio.Lock] = {}
        self._manager_state_epoch: dict[str, int] = {}

        # Lock for creating per-resource locks
        self._resource_creation_lock: asyncio.Lock = asyncio.Lock()

        # Counter protection lock
        self._counter_lock: asyncio.Lock = asyncio.Lock()

    def add_manager(self, manager_id: str, manager_info: ManagerInfo) -> None:
        """Add or update a manager learned second-hand (a peer's manager
        list). Ignored when its address is directly confirmed as another
        manager -- the listing describes an incarnation that address no
        longer runs."""
        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        confirmed_id = self._manager_id_by_addr.get(manager_addr)
        if confirmed_id is not None and confirmed_id != manager_id:
            return
        self._known_managers[manager_id] = manager_info
        self._record_manager_leave_udp_addr(manager_info)

    def is_confirmed_at(self, manager_id: str, manager_addr: tuple[str, int]) -> bool:
        """Whether ``manager_id`` is the directly confirmed manager at
        ``manager_addr``."""
        return self._manager_id_by_addr.get(manager_addr) == manager_id

    def confirm_manager(self, manager_id: str, manager_info: ManagerInfo) -> None:
        """Record a manager from direct evidence (its registration
        exchange or its own heartbeat). Any other manager id known at the
        same TCP address is a previous incarnation of that process: it is
        superseded -- its breakers, health, locks and info dropped, and the
        primary role handed over if it held it -- so sends to the address
        resolve to the live incarnation."""
        manager_addr = (manager_info.tcp_host, manager_info.tcp_port)
        superseded_ids = [
            known_id
            for known_id, known_manager in self._known_managers.items()
            if known_id != manager_id
            and (known_manager.tcp_host, known_manager.tcp_port) == manager_addr
        ]
        for superseded_id in superseded_ids:
            was_primary = self._primary_manager_id == superseded_id
            self.remove_manager_state(superseded_id, manager_addr)
            if was_primary:
                self._primary_manager_id = manager_id
        self._manager_id_by_addr[manager_addr] = manager_id
        self._known_managers[manager_id] = manager_info
        self._record_manager_leave_udp_addr(manager_info)

    def _record_manager_leave_udp_addr(
        self,
        manager_info: ManagerInfo,
    ) -> None:
        """Remember a manager UDP address for future voluntary worker LEAVE."""
        udp_host = manager_info.udp_host
        udp_port = manager_info.udp_port
        if udp_host and udp_port:
            self._manager_leave_udp_addrs[(udp_host, udp_port)] = None

    def get_manager(self, manager_id: str) -> ManagerInfo | None:
        """Get manager info by ID."""
        return self._known_managers.get(manager_id)

    def get_known_manager_values(self) -> list[ManagerInfo]:
        """Return a snapshot of all known managers."""
        return list(self._known_managers.values())

    def get_manager_leave_udp_addrs(self) -> list[tuple[str, int]]:
        """Return durable manager UDP targets for worker voluntary LEAVE.

        Voluntary worker shutdown is manager-authoritative: workers notify
        managers, and managers drive local registry detach plus SWIM gossip.
        This cache intentionally survives health downgrades and manager-state
        reaping so shutdown does not fall back to broadcasting LEAVE at every
        SWIM peer when the live-manager set is temporarily empty.
        """
        for manager_info in self._known_managers.values():
            self._record_manager_leave_udp_addr(manager_info)

        return list(self._manager_leave_udp_addrs.keys())

    def get_manager_by_addr(self, addr: tuple[str, int]) -> ManagerInfo | None:
        """Get manager info by TCP address (the directly confirmed
        incarnation when there is one)."""
        if (confirmed_id := self._manager_id_by_addr.get(addr)) is not None:
            if (confirmed := self._known_managers.get(confirmed_id)) is not None:
                return confirmed
        for manager in self._known_managers.values():
            if (manager.tcp_host, manager.tcp_port) == addr:
                return manager
        return None

    async def mark_manager_healthy(self, manager_id: str) -> None:
        async with self._counter_lock:
            self._healthy_manager_ids.add(manager_id)
            self._manager_unhealthy_since.pop(manager_id, None)
        self._signal_healthy_set_changed()

    async def mark_manager_unhealthy(self, manager_id: str) -> None:
        async with self._counter_lock:
            self._healthy_manager_ids.discard(manager_id)
            if manager_id not in self._manager_unhealthy_since:
                self._manager_unhealthy_since[manager_id] = _DEFAULT_CLOCK.monotonic()
        self._signal_healthy_set_changed()

    def _signal_healthy_set_changed(self) -> None:
        """Notify the cluster-connection owner of a healthy-set mutation.

        Wrapper exists so both async mutation paths (mark_healthy /
        mark_unhealthy under the counter lock) and sync paths
        (``remove_manager_state``) share the same notification. The
        callback runs *outside* the counter lock so it can safely
        read other registry state.
        """
        if self._on_healthy_set_changed is not None:
            self._on_healthy_set_changed()

    def is_manager_healthy(self, manager_id: str) -> bool:
        """Check if a manager is healthy."""
        return manager_id in self._healthy_manager_ids

    def get_healthy_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of all healthy managers."""
        return [
            (manager.tcp_host, manager.tcp_port)
            for manager_id in self._healthy_manager_ids
            if (manager := self._known_managers.get(manager_id))
        ]

    def get_primary_manager_tcp_addr(self) -> tuple[str, int] | None:
        """Get TCP address of the primary manager."""
        if not self._primary_manager_id:
            return None
        if manager := self._known_managers.get(self._primary_manager_id):
            return (manager.tcp_host, manager.tcp_port)
        return None

    def set_primary_manager(self, manager_id: str | None) -> None:
        """Set the primary manager."""
        self._primary_manager_id = manager_id

    def get_or_create_manager_lock(self, manager_id: str) -> asyncio.Lock:
        """Get or create a state lock for a manager."""
        return self._manager_state_locks.setdefault(manager_id, asyncio.Lock())

    def increment_manager_epoch(self, manager_id: str) -> int:
        """Increment and return the epoch for a manager."""
        current = self._manager_state_epoch.get(manager_id, 0)
        self._manager_state_epoch[manager_id] = current + 1
        return self._manager_state_epoch[manager_id]

    def get_manager_epoch(self, manager_id: str) -> int:
        """Get current epoch for a manager."""
        return self._manager_state_epoch.get(manager_id, 0)

    def remove_manager_state(
        self, manager_id: str, manager_addr: tuple[str, int] | None
    ) -> None:
        """Remove all per-manager tracking when a manager is reaped.

        Drops the per-manager lock, epoch counter, circuit breaker, address
        circuit breaker, backpressure signal, and health/registry entries. Without this cleanup the
        per-manager state dicts would grow unbounded under manager churn.
        """
        removed_manager = self._known_managers.pop(manager_id, None)
        self._forget_confirmed_addr(manager_id, removed_manager)
        self._healthy_manager_ids.discard(manager_id)
        self._manager_unhealthy_since.pop(manager_id, None)
        self._manager_circuits.pop(manager_id, None)
        self._manager_state_locks.pop(manager_id, None)
        self._manager_state_epoch.pop(manager_id, None)
        self._forget_manager_backpressure(manager_id)
        if manager_addr is not None:
            self._manager_addr_circuits.pop(manager_addr, None)
        self._signal_healthy_set_changed()

    def _forget_confirmed_addr(
        self, manager_id: str, removed_manager: ManagerInfo | None
    ) -> None:
        """Drop the removed manager's address confirmation if it still names it."""
        if removed_manager is not None:
            removed_addr = (removed_manager.tcp_host, removed_manager.tcp_port)
            if self._manager_id_by_addr.get(removed_addr) == manager_id:
                del self._manager_id_by_addr[removed_addr]

    def get_or_create_circuit(self, manager_id: str) -> ErrorStats:
        """Get or create the configured circuit breaker for a manager."""
        if manager_id not in self._manager_circuits:
            self._manager_circuits[manager_id] = ErrorStats(**self._circuit_breaker_config)
        return self._manager_circuits[manager_id]

    def get_or_create_circuit_by_addr(self, addr: tuple[str, int]) -> ErrorStats:
        """Get or create the configured circuit breaker by manager address."""
        if addr not in self._manager_addr_circuits:
            self._manager_addr_circuits[addr] = ErrorStats(**self._circuit_breaker_config)
        return self._manager_addr_circuits[addr]

    def is_circuit_open(self, manager_id: str) -> bool:
        """Check if a manager's circuit breaker is open."""
        if circuit := self._manager_circuits.get(manager_id):
            return circuit.circuit_state == CircuitState.OPEN
        return False

    def is_circuit_open_by_addr(self, addr: tuple[str, int]) -> bool:
        """Check if a manager's circuit breaker is open by address."""
        if circuit := self._manager_addr_circuits.get(addr):
            return circuit.circuit_state == CircuitState.OPEN
        return False

    def get_circuit_status(
        self, manager_id: str | None = None
    ) -> ManagerCircuitStatus | ManagerCircuitLookupError | ManagerCircuitSummary:
        """Get circuit breaker status for a specific manager or summary."""
        if manager_id:
            return self._manager_circuit_status(manager_id)

        return {
            "managers": {
                mid: {
                    "circuit_state": cb.circuit_state.name,
                    "error_count": cb.error_count,
                }
                for mid, cb in self._manager_circuits.items()
            },
            "open_circuits": self._open_circuit_manager_ids(),
            "healthy_managers": len(self._healthy_manager_ids),
            "primary_manager": self._primary_manager_id,
        }

    def _manager_circuit_status(
        self, manager_id: str
    ) -> ManagerCircuitStatus | ManagerCircuitLookupError:
        """One manager's circuit breaker status, or an error without one."""
        if not (circuit := self._manager_circuits.get(manager_id)):
            return {"error": f"No circuit breaker for manager {manager_id}"}
        return {
            "manager_id": manager_id,
            "circuit_state": circuit.circuit_state.name,
            "error_count": circuit.error_count,
            "error_rate": circuit.error_rate,
        }

    def _open_circuit_manager_ids(self) -> list[str]:
        """Ids of managers whose circuit breaker is OPEN."""
        return [
            mid
            for mid, cb in self._manager_circuits.items()
            if cb.circuit_state == CircuitState.OPEN
        ]

    async def select_new_primary_manager(self) -> str | None:
        """
        Select a new primary manager from healthy managers.

        Prefers the leader if known, otherwise picks any healthy manager.

        Returns:
            Selected manager ID or None
        """
        # Prefer the leader if we know one
        if (leader_id := self._select_known_leader()) is not None:
            return leader_id

        return self._select_healthy_primary()

    def _select_known_leader(self) -> str | None:
        """Make the first known healthy leader primary and return it, if any."""
        for manager_id in self._healthy_manager_ids:
            if self._is_known_leader(manager_id):
                self._primary_manager_id = manager_id
                return manager_id
        return None

    def _is_known_leader(self, manager_id: str) -> bool:
        """Whether a manager is known and reports itself leader."""
        manager = self._known_managers.get(manager_id)
        return bool(manager and manager.is_leader)

    def _select_healthy_primary(self) -> str | None:
        """Choose and set a primary among healthy managers via AD-28 selection."""
        # Otherwise let AD-28 selection choose: rendezvous ranking spreads
        # workers across managers deterministically and the EWMA latency
        # comparison steers away from slow ones. If it cannot choose
        # (e.g. a manager not yet known to discovery), fall back to a
        # deterministic pick rather than set-iteration order.
        healthy_manager_ids = set(self._healthy_manager_ids)
        if not healthy_manager_ids:
            self._primary_manager_id = None
            return None

        selected = self._select_manager(healthy_manager_ids)
        if selected not in healthy_manager_ids:
            selected = min(healthy_manager_ids)

        self._primary_manager_id = selected
        return selected

    def find_manager_by_udp_addr(self, udp_addr: tuple[str, int]) -> str | None:
        """Find manager ID by UDP address."""
        for manager_id, manager in self._known_managers.items():
            if (manager.udp_host, manager.udp_port) == udp_addr:
                return manager_id
        return None
