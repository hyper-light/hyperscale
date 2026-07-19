"""
A parameterizable-DURATION workflow dispatched through a gate — the
picklable client entries the multi-DC fault program drives.

``job_dispatch_demo``'s ``SimPingWorkflow`` finishes in ~2 virtual
seconds, so scheduled faults mostly land on an IDLE cluster. The soak
entries here submit ``SimSoakWorkflow`` with an entry-arg duration
(several virtual seconds, multi-step), so dc_loss / dc_partition /
manager-restart / storage windows can be aimed INSIDE live execution —
probed, then pinned.

The entries extend the milestone vocabulary of ``job_dispatch_demo``
(kept value-shaped for the replay contract — ``(tag, virtual_time[,
small-value])`` only, never node ids or snowflakes):

* ``("submit-rejected", ExcName, t)`` — each refused submission
* ``("job-submitted", t)`` — acceptance
* ``("status-seen", status, t)`` — every observed status transition
* ``("wait-timed-out", t)`` — ``wait_for_job`` hit its deadline; the
  entry keeps waiting (LOUD, never silent) so a late terminal still
  lands in the log
* ``("job-finished", status, t)`` — result delivery (exactly once)

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname; the workflow class is cloudpickled by value
across the submission path exactly as a user's script-defined workflow.
"""

import asyncio
import os
import sys

import cloudpickle

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


class SimSoakWorkflow(Workflow):
    """Deterministic multi-step soak workflow.

    Two chained half-virtual-second steps per VU iteration; ``duration``
    (and a proportionate ``timeout``) are overridden per INSTANCE by the
    client entry, so one class serves every probed execution window.
    """

    vus: int = 2
    duration: str = "4s"

    @step()
    async def soak_first_leg(self) -> dict[str, str]:
        await asyncio.sleep(0.5)
        return {"leg": "first"}

    @step("soak_first_leg")
    async def soak_second_leg(self) -> dict[str, str]:
        await asyncio.sleep(0.5)
        return {"leg": "second"}


# Ship the workflow class BY VALUE (same rationale as job_dispatch_demo:
# the manager's restricted unpickler refuses by-reference pickles of
# tests-tree modules; by-value reproduces the production user-script
# shape).
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def soak_gate_dispatch_client_entry(
    context,
    host,
    port,
    gate_tcp_address,
    duration_seconds,
    job_timeout_seconds,
    wait_timeout_seconds,
    submit_at=0.0,
    pinned_datacenters=None,
) -> None:
    """Client child: submit one ``SimSoakWorkflow`` of ``duration_seconds``
    through a gate and await completion.

    * ``job_timeout_seconds`` — the job-level timeout the gate's AD-34
      tracker enforces; sized per scenario so the INTENDED fault outcome
      (not an accidental timeout) decides the run.
    * ``wait_timeout_seconds`` — the client-side ``wait_for_job``
      deadline. Expiry is recorded as ``("wait-timed-out", t)`` and the
      entry then waits UNBOUNDED for the terminal: a stranded job stays
      visible in the log instead of killing the watcher.
    * ``submit_at`` — virtual instant the whole client lifecycle starts
      (0.0 = immediately): the after-quiesce submitter of the
      long-horizon scenarios.
    * ``pinned_datacenters`` — optional placement constraint
      (``datacenters=[...]``), how scenarios aim a job at a specific DC.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await client.start()

        workflow = SimSoakWorkflow()
        workflow.duration = f"{duration_seconds:g}s"
        workflow.timeout = f"{duration_seconds + 30.0:g}s"

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], workflow)],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                    datacenters=(
                        list(pinned_datacenters) if pinned_datacenters else None
                    ),
                )
            except Exception as submit_error:
                # Production behavior: gates/managers reject until they
                # can place the job — retry on virtual time. Type name
                # only: rejection texts can embed per-run values.
                log.append(
                    (
                        "submit-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append(("job-submitted", round(context.loop.time(), 6)))

        async def watch_status() -> None:
            last_status: str | None = None
            while True:
                job_result = client.get_job_status(job_id)
                status = job_result.status if job_result is not None else None
                if status != last_status:
                    last_status = status
                    log.append(
                        ("status-seen", status, round(context.loop.time(), 6))
                    )
                await asyncio.sleep(0.5)

        status_watcher = context.loop.create_task(watch_status())
        try:
            result = await client.wait_for_job(
                job_id, timeout=wait_timeout_seconds
            )
        except asyncio.TimeoutError:
            # LOUD, then keep waiting: the simulation ceiling bounds the
            # run, and a late terminal must still reach the log.
            log.append(("wait-timed-out", round(context.loop.time(), 6)))
            result = await client.wait_for_job(job_id)
        status_watcher.cancel()
        log.append(
            ("job-finished", result.status, round(context.loop.time(), 6))
        )

    if submit_at > 0.0:
        context.loop.call_at(submit_at, lambda: context.loop.create_task(run()))
    else:
        context.loop.create_task(run())
