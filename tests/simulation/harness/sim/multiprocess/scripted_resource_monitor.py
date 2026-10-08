"""
``ScriptedResourceMonitor`` -- the host resource readings a SIM node's
overload detector is fed, scripted on virtual time.

SIM has no CPU: handling a request costs no virtual time, so no amount of
traffic moves a real resource reading, and the production sampler
(``ProcessResourceMonitor``, psutil on a worker thread) cannot run on the
``SimulationLoop`` at all. A scenario that needs a node's AD-18/AD-22
overload state to move -- a host saturated by a storm of submissions --
scripts the host's readings instead, as ``(at, cpu_percent)`` steps on
the node's virtual timeline. Everything downstream of the reading (the
detector, its hysteresis, the load shedder and the health-gated rate
limiter) is the production code.
"""

from hyperscale.distributed.resources.resource_metrics import ResourceMetrics


class ScriptedResourceMonitor:
    """Answers ``sample`` with the CPU reading of the latest
    ``(at, cpu_percent)`` step at or before now, and a constant memory
    reading -- the ``ProcessResourceMonitor`` surface a node samples."""

    __slots__ = ("_loop", "_cpu_steps", "_memory_percent", "_last_metrics")

    def __init__(self, loop, cpu_steps: list[tuple[float, float]], memory_percent: float) -> None:
        self._loop = loop
        self._cpu_steps = sorted(cpu_steps)
        self._memory_percent = memory_percent
        self._last_metrics: ResourceMetrics | None = None

    def cpu_percent_at(self, at_time: float) -> float:
        """The scripted CPU reading in force at ``at_time``."""
        reading = 0.0
        for step_at, cpu_percent in self._cpu_steps:
            if step_at > at_time:
                break
            reading = cpu_percent
        return reading

    async def sample(self) -> ResourceMetrics:
        """The scripted reading now, exact (no measurement noise)."""
        self._last_metrics = ResourceMetrics(
            cpu_percent=self.cpu_percent_at(self._loop.time()),
            cpu_uncertainty=0.0,
            memory_bytes=0,
            memory_uncertainty=0.0,
            memory_percent=self._memory_percent,
            file_descriptor_count=0,
        )
        return self._last_metrics

    def get_last_metrics(self) -> ResourceMetrics | None:
        """The last reading ``sample`` returned."""
        return self._last_metrics
