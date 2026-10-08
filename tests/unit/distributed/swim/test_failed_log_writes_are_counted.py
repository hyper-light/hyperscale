"""
When the logger itself fails, the lost record is counted where the
component's stats show it -- never silently dropped, and never raised
into the protocol path that only wanted to log.

Driven through the real components with a logger whose every write fails.
"""

from hyperscale.distributed.swim.core.error_handler import ErrorHandler
from hyperscale.distributed.swim.core.metrics import Metrics
from hyperscale.distributed.swim.detection.incarnation_tracker import IncarnationTracker
from hyperscale.distributed.swim.health.graceful_degradation import GracefulDegradation
from hyperscale.distributed.swim.health.health_monitor import EventLoopHealthMonitor
from hyperscale.distributed.swim.leadership import LocalLeaderElection


class BrokenLogger:
    """A logger whose sink is gone: every write raises."""

    def __init__(self) -> None:
        self.writes_attempted = 0

    async def log(self, entry: object) -> None:
        self.writes_attempted += 1
        raise OSError("log sink unavailable")


async def test_a_failed_election_log_write_is_counted_in_its_status() -> None:
    logger = BrokenLogger()
    election = LocalLeaderElection(dc_id="dc-1")
    election.set_logger(logger, "127.0.0.1", 9001, 1)

    await election._log_debug("first")
    await election._log_debug("second")

    assert logger.writes_attempted == 2
    assert election.get_status()["log_write_failures"] == 2


async def test_a_failed_flapping_detector_log_write_is_counted_in_its_stats() -> None:
    logger = BrokenLogger()
    election = LocalLeaderElection(dc_id="dc-1")
    election.set_logger(logger, "127.0.0.1", 9001, 1)

    await election.flapping_detector._log_debug("change")

    assert logger.writes_attempted == 1
    assert election.flapping_detector.get_stats()["log_write_failures"] == 1


async def test_a_failed_error_handler_log_write_is_counted() -> None:
    logger = BrokenLogger()
    handler = ErrorHandler(logger=logger, node_id="node-1")

    await handler._log_internal("internal")

    assert logger.writes_attempted == 1
    assert handler.log_write_failures == 1


async def test_failed_log_writes_of_the_node_logged_swim_components_are_counted() -> None:
    """The components the SWIM server now hands its logger to."""
    logger = BrokenLogger()
    incarnation_tracker = IncarnationTracker()
    degradation = GracefulDegradation()
    health_monitor = EventLoopHealthMonitor()
    metrics = Metrics()
    for component in (incarnation_tracker, degradation, health_monitor, metrics):
        component.set_logger(logger, "127.0.0.1", 9001, "node-1")

    await incarnation_tracker._log_debug("incarnation")
    await degradation._log_debug("degradation")
    await health_monitor._log_debug("health")
    metrics._saturated_counters.add("probes_sent")
    await metrics.log_saturation_warnings()

    assert logger.writes_attempted == 4
    assert incarnation_tracker.get_stats()["log_write_failures"] == 1
    assert degradation.get_stats()["log_write_failures"] == 1
    assert health_monitor.get_stats()["log_write_failures"] == 1
    assert metrics.to_dict()["log_write_failures"] == 1
