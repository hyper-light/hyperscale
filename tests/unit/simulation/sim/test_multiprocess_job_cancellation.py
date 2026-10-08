"""
Client-initiated cancellation of a running job, end to end over the
multi-process coordinator (``cancel_job_demo``): one manager, one worker,
a client that cancels its job once it is running.

Pinned: the cancellation is accepted, the job ends ``cancelled``, and the
client is told the cancellation completed EXACTLY once. The manager used
to tell it twice -- its workflow-cancellation-complete handler pushed the
completion from the live finalize path and then again from the
cancellation coordinator, which found the pending set already drained.

Replay-deterministic.
"""

from tests.simulation.harness.sim.multiprocess.cancel_job_demo import run_cancel_job

_SEED = 61
_CANCEL_AFTER_RUNNING_SECONDS = 0.5
# Long enough after the terminal for any duplicate push to land.
_OBSERVE_SECONDS = 5.0
_CEILING = 40.0


def _run() -> dict:
    return run_cancel_job(_CEILING, _CANCEL_AFTER_RUNNING_SECONDS, _OBSERVE_SECONDS, _SEED)


def _entries(log: list, tag: str) -> list:
    return [entry for entry in log if entry[0] == tag]


def test_a_cancelled_job_is_reported_complete_exactly_once():
    client_log = _run()["client"]

    (cancel_response,) = _entries(client_log, "cancel-response")
    assert cancel_response[1] is True, client_log
    (finished,) = _entries(client_log, "job-finished")
    assert finished[1] == "cancelled", client_log
    (observed,) = _entries(client_log, "observed-until")
    assert observed[1] - finished[2] >= _OBSERVE_SECONDS, client_log

    pushes = _entries(client_log, "cancellation-push")
    assert [push[1] for push in pushes] == [True], client_log


def test_job_cancellation_is_replay_deterministic():
    assert _run() == _run()
