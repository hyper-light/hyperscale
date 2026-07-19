"""
The GATE-FAULT client child: a real ``HyperscaleClient`` configured with
the WHOLE gate tier (every gate address, production round-robin +
sticky-target selection) submitting a PARAMETERIZABLE-DURATION workflow —
the picklable client entry the gate-cluster fault scenarios drive.

Differences from ``job_dispatch_demo``'s single-target entries (which
stay untouched so their pinned schedules never move):

* ``gate_tcp_addresses`` is a LIST: submission retries cycle every gate
  (the client redirect/failover path a gate kill must exercise), and
  the post-acceptance status-poll fallback can reach survivors.
* The workflow's ``duration`` (and ``vus``) are ENTRY ARGS, so faults
  can be scheduled to provably overlap live execution instead of always
  landing on an idle system (the stock 2s ping workflow drains before
  most fault windows open).
* ``start_at`` delays the whole client to that virtual instant — a
  second job submitted mid-chaos, or a post-quiesce convergence probe.
* Loud terminal milestones for every way a wait can end: a client-side
  wait expiry logs ``("wait-timeout", t)`` and bounded submission
  refusal logs ``("submit-abandoned", t)`` — silence is never a legal
  outcome, and the invariants assert on these tags directly.

Milestones (``(tag, virtual_time[, small-value])`` ONLY — never node
ids, snowflakes, or error text, so identical-seed runs compare equal):

* ``("submit-target", gate_index, t)`` — index INTO THE CONFIGURED GATE
  LIST of the gate that accepted the submission (which gate accepts is
  part of the deterministic schedule; the index makes "kill the
  submission gate" scenarios assertable without recording identities).
* ``("job-submitted", t)`` / ``("submit-rejected", ExcName, t)`` /
  ``("submit-abandoned", t)``
* ``("status-seen", status, t)`` / ``("job-finished", status, t)`` /
  ``("wait-timeout", t)``
* ``("client-error", ExcName, t)`` — any unexpected failure of the
  client flow, recorded before re-raising so the run result stays loud
  even though child logging is disabled under SIM.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname; the workflow class is built INSIDE the
entry (duration/vus baked in from args) and cloudpickled by value
across the submission path exactly as a user's script-defined workflow
travels.
"""

import asyncio
import os
import sys

import cloudpickle

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


# By-value pickling for everything this module defines (the workflow
# factory's classes are function-local and therefore by-value already;
# registering the module keeps any future module-level helpers safe) —
# same rationale as ``job_dispatch_demo``: the manager's restricted
# unpickler admits hyperscale.* by reference only, so a tests-tree
# module must travel by value, reproducing the production shape.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def _build_sustained_load_workflow(
    workflow_duration_seconds: float, workflow_vus: int
) -> type[Workflow]:
    """Build a deterministic multi-step workflow that holds the job
    in-flight for ``workflow_duration_seconds`` VIRTUAL seconds.

    Two chained ACTION steps (``load_step_two`` depends on
    ``load_step_one``), each awaiting half the requested duration on
    the virtual clock, so the one-shot DAG pass keeps executors mid-run
    for the whole window — probed: client-visible completion lands at
    ``dispatch + duration + push latency``, which is what lets fault
    windows provably intersect live execution instead of an idle tier.

    Deliberately NOT a TEST (duration-governed VU) workflow: probing
    one under SIM dies with ``SimulationConstraintError`` — production
    ``WorkflowRunner._generate`` busy-waits via ``asyncio.sleep(0)``
    while virtual ``loop.time()`` is frozen at the duration boundary
    (wall clocks advance through CPU spins; the virtual clock only
    advances on timers). Until that liveness gap gets a production fix,
    parameterized ACTION sleeps are the virtual-time-safe way to pin
    execution length; the class ``duration`` is set to match so the
    worker-side active-workflow bookkeeping window agrees.
    """
    step_sleep_seconds = workflow_duration_seconds / 2.0

    class SimSustainedLoadWorkflow(Workflow):
        vus = workflow_vus
        duration = f"{workflow_duration_seconds}s"

        @step()
        async def load_step_one(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"stage": "one"}

        @step("load_step_one")
        async def load_step_two(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"stage": "two"}

    return SimSustainedLoadWorkflow


def multi_gate_load_client_entry(
    context,
    host,
    port,
    gate_tcp_addresses,
    workflow_duration_seconds=6.0,
    workflow_vus=2,
    job_timeout_seconds=30.0,
    wait_timeout_seconds=60.0,
    start_at=0.0,
    max_submit_attempts=0,
) -> None:
    """Client child: submit a sustained-load workflow through the gate
    TIER (all gates configured; production target cycling) and await
    completion, logging loud milestones for every outcome.

    ``max_submit_attempts=0`` retries submission forever (each retry is
    a logged ``submit-rejected`` milestone, so the retry count is
    pinned by replay); a positive bound logs ``submit-abandoned`` after
    that many rejections and stops — the retries-exhausted loud path.
    ``start_at`` delays the whole client (start + submit) to that
    virtual instant, exactly like ``worker_entry``'s late-join knob.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[tuple(gate_address) for gate_address in gate_tcp_addresses],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    workflow_class = _build_sustained_load_workflow(
        workflow_duration_seconds, workflow_vus
    )
    configured_gates = [tuple(gate_address) for gate_address in gate_tcp_addresses]

    async def submit_until_accepted() -> str | None:
        rejection_count = 0
        while True:
            try:
                return await client.submit_job(
                    workflows=[([], workflow_class())],
                    vus=workflow_vus,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                # Production behavior: gates reject while the tier is
                # forming (no quorum, DC unhealthy, replication not
                # ready) — retry on virtual time. Log the exception
                # TYPE only: rejection text can embed per-run values.
                rejection_count += 1
                log.append(
                    (
                        "submit-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                if max_submit_attempts and rejection_count >= max_submit_attempts:
                    log.append(
                        ("submit-abandoned", round(context.loop.time(), 6))
                    )
                    return None
                await asyncio.sleep(1.0)

    async def run() -> None:
        try:
            await client.start()

            job_id = await submit_until_accepted()
            if job_id is None:
                return

            accepted_target = client._state.get_job_target(job_id)
            if accepted_target not in configured_gates:
                raise ValueError(
                    "accepted target is not a configured gate: "
                    f"{accepted_target!r}"
                )
            log.append(
                (
                    "submit-target",
                    configured_gates.index(accepted_target),
                    round(context.loop.time(), 6),
                )
            )
            log.append(("job-submitted", round(context.loop.time(), 6)))

            async def watch_status() -> None:
                last_status: str | None = None
                while True:
                    job_result = client.get_job_status(job_id)
                    status = (
                        job_result.status if job_result is not None else None
                    )
                    if status != last_status:
                        last_status = status
                        log.append(
                            (
                                "status-seen",
                                status,
                                round(context.loop.time(), 6),
                            )
                        )
                    await asyncio.sleep(0.5)

            status_watcher = context.loop.create_task(watch_status())
            try:
                result = await client.wait_for_job(
                    job_id, timeout=wait_timeout_seconds
                )
            except asyncio.TimeoutError:
                # The LOUD client-observed expiry: the wait ended with
                # no terminal state. Scenarios that strand pushes (the
                # accepting gate restarted with no durable tier) assert
                # THIS milestone — never silence.
                log.append(("wait-timeout", round(context.loop.time(), 6)))
                return
            finally:
                # Guarded: at process teardown the coordinator STOP can
                # close the loop while ``run`` is still parked on the
                # wait; cancelling against a closed loop raises inside
                # generator close (pure stderr noise, results already
                # collected) — skip it, the process is exiting.
                if not context.loop.is_closed():
                    status_watcher.cancel()
            log.append(
                ("job-finished", result.status, round(context.loop.time(), 6))
            )
        except asyncio.CancelledError:
            raise
        except Exception as client_error:
            # Child logging is disabled under SIM: without this entry
            # an unexpected failure would surface only as an opaque
            # never-retrieved task exception. Type name only (stable
            # across replays), then re-raise — never swallow.
            log.append(
                (
                    "client-error",
                    type(client_error).__name__,
                    round(context.loop.time(), 6),
                )
            )
            raise

    # ``start_at=0.0`` keeps immediate-start scheduling; a positive
    # start delays the whole client lifecycle to that virtual instant
    # (mid-chaos second submitter, post-quiesce convergence probe).
    if start_at > 0.0:
        context.loop.call_at(start_at, lambda: context.loop.create_task(run()))
    else:
        context.loop.create_task(run())
