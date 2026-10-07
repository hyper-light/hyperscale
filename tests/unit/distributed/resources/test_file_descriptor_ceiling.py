"""
AD-41 file-descriptor ceiling, measured on this test process for real.

Rules pinned: the limit is this process's RLIMIT_NOFILE soft limit read at
runtime (None where the platform reports none); the measure is the largest
single process's descriptor count, as ``ProcessResourceMonitor`` samples
it; reaching the kill line starts refusing new work (and reports the
violation once), refusal holds between the warning and kill lines, and
ends only under the warning line; a worker refusing new work drains.
"""

import os
from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import WorkerState
from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.distributed.resources.file_descriptor_ceiling import FileDescriptorCeiling
from hyperscale.distributed.resources.process_resource_monitor import ProcessResourceMonitor
from hyperscale.distributed.resources.resource_violation_type import ResourceViolationType
from hyperscale.distributed.swim.health.graceful_degradation import GracefulDegradation

try:
    import resource
except ImportError:
    resource = None

WARNING_THRESHOLD = 0.8
KILL_THRESHOLD = 1.0
CEILING_HEADROOM_DESCRIPTORS = 40


def test_limit_is_this_process_soft_rlimit_nofile() -> None:
    detected_limit = FileDescriptorCeiling.detect_descriptor_limit()
    if resource is None:
        assert detected_limit is None
        return
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert detected_limit == (None if soft_limit == resource.RLIM_INFINITY else soft_limit)


async def _largest_process_descriptors(monitor: ProcessResourceMonitor) -> int:
    return (await monitor.sample()).largest_process_file_descriptor_count


def _open_pipes(pipe_count: int) -> list[tuple[int, int]]:
    return [os.pipe() for _ in range(pipe_count)]


def _close_pipes(pipes: list[tuple[int, int]]) -> None:
    for read_descriptor, write_descriptor in pipes:
        os.close(read_descriptor)
        os.close(write_descriptor)


async def test_real_descriptors_drive_refusal_with_hysteresis() -> None:
    monitor = ProcessResourceMonitor(root_pid=os.getpid())
    baseline_descriptors = await _largest_process_descriptors(monitor)
    if baseline_descriptors == 0:
        pytest.skip("this platform reports no per-process descriptor counts")
    ceiling = FileDescriptorCeiling(
        descriptor_limit=baseline_descriptors + CEILING_HEADROOM_DESCRIPTORS,
        warning_threshold=WARNING_THRESHOLD,
        kill_threshold=KILL_THRESHOLD,
    )
    assert ceiling.observe(baseline_descriptors) is None
    assert not ceiling.refusing_new_work

    # Each pipe is two descriptors: 20 pipes reach the kill line.
    open_pipes = _open_pipes(CEILING_HEADROOM_DESCRIPTORS // 2)
    try:
        at_ceiling = await _largest_process_descriptors(monitor)
        assert at_ceiling >= ceiling.descriptor_limit
        assert ceiling.observe(at_ceiling) is ResourceViolationType.FILE_DESCRIPTORS_EXCEEDED
        assert ceiling.refusing_new_work
        assert ceiling.observe(at_ceiling) is None, "the violation is reported once per refusal"

        # Two pipes fewer: under the kill line, still above the warning
        # line (the band is 20% of a limit of at least 40): still refusing.
        _close_pipes(open_pipes[:2])
        open_pipes = open_pipes[2:]
        between_lines = await _largest_process_descriptors(monitor)
        assert ceiling.descriptor_limit * WARNING_THRESHOLD <= between_lines < ceiling.descriptor_limit
        ceiling.observe(between_lines)
        assert ceiling.refusing_new_work

        # Every pipe closed, back at the baseline under the warning line:
        # takes new work again.
        _close_pipes(open_pipes)
        open_pipes = []
        ceiling.observe(await _largest_process_descriptors(monitor))
        assert not ceiling.refusing_new_work
    finally:
        _close_pipes(open_pipes)


def test_unreported_limit_never_refuses() -> None:
    ceiling = FileDescriptorCeiling(
        descriptor_limit=None,
        warning_threshold=WARNING_THRESHOLD,
        kill_threshold=KILL_THRESHOLD,
    )
    assert ceiling.observe(10_000_000) is None
    assert not ceiling.refusing_new_work


def test_worker_drains_while_its_ceiling_refuses_new_work() -> None:
    ceiling = FileDescriptorCeiling(descriptor_limit=100, warning_threshold=WARNING_THRESHOLD, kill_threshold=KILL_THRESHOLD)
    worker = SimpleNamespace(_degradation=GracefulDegradation(), _file_descriptor_ceiling=ceiling)

    assert WorkerServer._worker_state_for_degradation(worker) is WorkerState.HEALTHY
    ceiling.observe(100)
    assert WorkerServer._worker_state_for_degradation(worker) is WorkerState.DRAINING
    ceiling.observe(79)
    assert WorkerServer._worker_state_for_degradation(worker) is WorkerState.HEALTHY
