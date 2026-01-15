"""
Health Probes - Liveness and Readiness probe implementations.

This module provides standardized health probe implementations for
distributed nodes, following Kubernetes-style health check semantics.

Probe Types:
- Liveness: Is the process running and responsive?
  - Failure triggers restart/replacement
  - Should be simple and fast

- Readiness: Can the node accept work?
  - Failure removes from load balancer/routing
  - Can be more complex, check dependencies

- Startup: Has the node finished initializing?
  - Delays liveness/readiness until startup complete
  - Prevents premature failure during slow startup

Each probe can be configured with:
- Timeout: How long to wait for response
- Period: How often to check
- Failure threshold: Consecutive failures before unhealthy
- Success threshold: Consecutive successes before healthy
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable, Protocol


class ProbeResult(Enum):
    """Result of a health probe."""

    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    ERROR = "error"


@dataclass(slots=True)
class ProbeResponse:
    """Response from a health probe."""

    result: ProbeResult
    message: str = ""
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.monotonic)
    details: dict = field(default_factory=dict)


@dataclass(slots=True)
class ProbeConfig:
    """Configuration for a health probe."""

    timeout_seconds: float = 1.0
    period_seconds: float = 10.0
    failure_threshold: int = 3
    success_threshold: int = 1
    initial_delay_seconds: float = 0.0


class ProbeCheck(Protocol):
    """Protocol for probe check functions."""

    async def __call__(self) -> tuple[bool, str]: ...


@dataclass(slots=True)
class ProbeState:
    """Current state of a probe."""

    healthy: bool = True
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    last_check: float = 0.0
    last_result: ProbeResult = ProbeResult.SUCCESS
    last_message: str = ""
    total_checks: int = 0
    total_failures: int = 0


class HealthProbe:
    """
    A configurable health probe with threshold-based state transitions.

    Example usage:
        async def check_database() -> tuple[bool, str]:
            try:
                await db.ping()
                return True, "Database responsive"
            except Exception as e:
                return False, str(e)

        probe = HealthProbe(
            name="database",
            check=check_database,
            config=ProbeConfig(
                timeout_seconds=2.0,
                failure_threshold=3,
            ),
        )

        # Run a single check
        response = await probe.check()
        if not probe.is_healthy():
            # Take action

        # Or run periodic checks
        await probe.start_periodic()
    """

    def __init__(
        self,
        name: str,
        check: ProbeCheck,
        config: ProbeConfig | None = None,
    ):
        """
        Initialize HealthProbe.

        Args:
            name: Name of this probe (for logging/metrics).
            check: Async function that returns (success, message).
            config: Probe configuration.
        """
        self._name = name
        self._check = check
        self._config = config or ProbeConfig()
        self._state = ProbeState()
        self._started = False
        self._periodic_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        """Get probe name."""
        return self._name

    def is_healthy(self) -> bool:
        """Check if probe is currently healthy."""
        return self._state.healthy

    def get_state(self) -> ProbeState:
        """Get current probe state."""
        return self._state

    async def check(self) -> ProbeResponse:
        """
        Run a single probe check.

        Returns:
            ProbeResponse with result and details.
        """
        start_time = time.monotonic()
        self._state.total_checks += 1

        try:
            # Run check with timeout
            success, message = await asyncio.wait_for(
                self._check(),
                timeout=self._config.timeout_seconds,
            )

            latency_ms = (time.monotonic() - start_time) * 1000

            if success:
                result = ProbeResult.SUCCESS
                self._record_success(message)
            else:
                result = ProbeResult.FAILURE
                self._record_failure(message)

            return ProbeResponse(
                result=result,
                message=message,
                latency_ms=latency_ms,
            )

        except asyncio.TimeoutError:
            latency_ms = (time.monotonic() - start_time) * 1000
            message = f"Probe timed out after {self._config.timeout_seconds}s"
            self._record_failure(message)

            return ProbeResponse(
                result=ProbeResult.TIMEOUT,
                message=message,
                latency_ms=latency_ms,
            )

        except Exception as exception:
            latency_ms = (time.monotonic() - start_time) * 1000
            message = f"Probe error: {exception}"
            self._record_failure(message)

            return ProbeResponse(
                result=ProbeResult.ERROR,
                message=message,
                latency_ms=latency_ms,
            )

    def _record_success(self, message: str) -> None:
        """Record a successful check."""
        self._state.consecutive_successes += 1
        self._state.consecutive_failures = 0
        self._state.last_check = time.monotonic()
        self._state.last_result = ProbeResult.SUCCESS
        self._state.last_message = message

        # Transition to healthy if threshold met
        if self._state.consecutive_successes >= self._config.success_threshold:
            self._state.healthy = True

    def _record_failure(self, message: str) -> None:
        """Record a failed check."""
        self._state.consecutive_failures += 1
        self._state.consecutive_successes = 0
        self._state.last_check = time.monotonic()
        self._state.last_result = ProbeResult.FAILURE
        self._state.last_message = message
        self._state.total_failures += 1

        # Transition to unhealthy if threshold met
        if self._state.consecutive_failures >= self._config.failure_threshold:
            self._state.healthy = False

    async def start_periodic(self) -> None:
        """Start periodic probe checks."""
        if self._started:
            return

        self._started = True

        # Initial delay
        if self._config.initial_delay_seconds > 0:
            await asyncio.sleep(self._config.initial_delay_seconds)

        self._periodic_task = asyncio.create_task(self._periodic_loop())

    async def stop_periodic(self) -> None:
        """Stop periodic probe checks."""
        self._started = False
        if self._periodic_task:
            self._periodic_task.cancel()
            try:
                await self._periodic_task
            except asyncio.CancelledError:
                pass
            self._periodic_task = None

    async def _periodic_loop(self) -> None:
        """Internal loop for periodic checks."""
        while self._started:
            await self.check()
            await asyncio.sleep(self._config.period_seconds)

    def reset(self) -> None:
        """Reset probe state."""
        self._state = ProbeState()


class LivenessProbe(HealthProbe):
    """
    Liveness probe - checks if the process is running.

    Liveness probes should be simple and fast. They check if the
    process itself is responsive, not if dependencies are available.

    Example:
        probe = LivenessProbe(
            name="process",
            check=lambda: (True, "Process alive"),
        )
    """

    def __init__(
        self,
        name: str = "liveness",
        check: ProbeCheck | None = None,
        config: ProbeConfig | None = None,
    ):
        # Default liveness check just returns True
        if check is None:

            async def default_check() -> tuple[bool, str]:
                return True, "Process alive"

            check = default_check

        # Liveness probes should be fast with low thresholds
        if config is None:
            config = ProbeConfig(
                timeout_seconds=1.0,
                period_seconds=10.0,
                failure_threshold=3,
                success_threshold=1,
            )

        super().__init__(name=name, check=check, config=config)


class ReadinessProbe(HealthProbe):
    """
    Readiness probe - checks if the node can accept work.

    Readiness probes can be more complex, checking dependencies
    like database connections, required services, etc.

    Example:
        async def check_ready() -> tuple[bool, str]:
            if not db_connected:
                return False, "Database not connected"
            if queue_depth > 1000:
                return False, "Queue too deep"
            return True, "Ready to accept work"

        probe = ReadinessProbe(
            name="service",
            check=check_ready,
        )
    """

    def __init__(
        self,
        name: str = "readiness",
        check: ProbeCheck | None = None,
        config: ProbeConfig | None = None,
    ):
        if check is None:

            async def default_check() -> tuple[bool, str]:
                return True, "Ready"

            check = default_check

        # Readiness probes can have slightly longer timeouts
        if config is None:
            config = ProbeConfig(
                timeout_seconds=2.0,
                period_seconds=10.0,
                failure_threshold=3,
                success_threshold=1,
            )

        super().__init__(name=name, check=check, config=config)


class StartupProbe(HealthProbe):
    """
    Startup probe - checks if initialization is complete.

    Startup probes run during node initialization and delay
    liveness/readiness probes until startup is complete.

    Example:
        async def check_startup() -> tuple[bool, str]:
            if not config_loaded:
                return False, "Loading configuration"
            if not cache_warmed:
                return False, "Warming cache"
            return True, "Startup complete"

        probe = StartupProbe(
            name="init",
            check=check_startup,
        )
    """

    def __init__(
        self,
        name: str = "startup",
        check: ProbeCheck | None = None,
        config: ProbeConfig | None = None,
    ):
        if check is None:

            async def default_check() -> tuple[bool, str]:
                return True, "Started"

            check = default_check

        # Startup probes have higher thresholds for slow startups
        if config is None:
            config = ProbeConfig(
                timeout_seconds=5.0,
                period_seconds=5.0,
                failure_threshold=30,  # Allow 30 failures (150s startup)
                success_threshold=1,
            )

        super().__init__(name=name, check=check, config=config)


class CompositeProbe:
    """
    Composite probe that combines multiple probes.

    Useful for checking multiple conditions for readiness.

    Example:
        composite = CompositeProbe(name="service")
        composite.add_probe(database_probe)
        composite.add_probe(cache_probe)
        composite.add_probe(queue_probe)

        if composite.is_healthy():
            # All probes healthy
            pass
    """

    def __init__(self, name: str = "composite"):
        self._name = name
        self._probes: list[HealthProbe] = []

    @property
    def name(self) -> str:
        return self._name

    def add_probe(self, probe: HealthProbe) -> None:
        """Add a probe to the composite."""
        self._probes.append(probe)

    def remove_probe(self, name: str) -> HealthProbe | None:
        """Remove a probe by name."""
        for i, probe in enumerate(self._probes):
            if probe.name == name:
                return self._probes.pop(i)
        return None

    def is_healthy(self) -> bool:
        """Check if all probes are healthy."""
        return all(probe.is_healthy() for probe in self._probes)

    def get_unhealthy_probes(self) -> list[str]:
        """Get names of unhealthy probes."""
        return [probe.name for probe in self._probes if not probe.is_healthy()]

    async def check_all(self) -> dict[str, ProbeResponse]:
        """Run all probes and return responses."""
        results: dict[str, ProbeResponse] = {}
        for probe in self._probes:
            results[probe.name] = await probe.check()
        return results

    async def start_all(self) -> None:
        """Start periodic checks for all probes."""
        for probe in self._probes:
            await probe.start_periodic()

    async def stop_all(self) -> None:
        """Stop periodic checks for all probes."""
        for probe in self._probes:
            await probe.stop_periodic()

    def get_status(self) -> dict:
        """Get status of all probes."""
        return {
            "name": self._name,
            "healthy": self.is_healthy(),
            "probes": {
                probe.name: {
                    "healthy": probe.is_healthy(),
                    "consecutive_failures": probe.get_state().consecutive_failures,
                    "last_result": probe.get_state().last_result.value,
                    "last_message": probe.get_state().last_message,
                }
                for probe in self._probes
            },
        }
