"""
D-5 / D-68 telemetry over the multi-process coordinator -- the picklable
client entry for ``test_multiprocess_cluster_metrics``.

``telemetry_client_entry`` submits several jobs through a gate at once
(enough to occupy every worker), waits for each to finish, then asks every
node it was given -- gate, manager and workers -- for its metrics through
``HyperscaleClient.cluster_metrics``, the call ``hyperscale cluster
--metrics`` makes. A worker lets go of a finished workflow after the job
is done, and a gate's view of a datacenter's latency SLO arrives on the
manager's heartbeats, so the client asks again, one SWIM probe interval
apart, until each worker runs nothing and the gate's view carries samples.

Milestones (value-shaped, never node ids, so identical-seed runs compare
equal; a worker's id is its process name):

* ``("job-finished", status, t)`` for each job
* ``("metrics", node_name, reply_fields, t)``: the reply's shared-schema
  fields, with each worker id in ``dispatch_latency`` replaced by the
  name of the worker process that reported it
* ``("metrics-failed", node_name, error_type, t)``

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio

from hyperscale.distributed.cluster.models import ClusterMetricsReply
from hyperscale.distributed.nodes.client import HyperscaleClient

from .job_dispatch_demo import SimPingWorkflow, _env

REPLY_FIELDS = (
    "role",
    "node_state",
    "formation",
    "capacity",
    "workload",
    "resources",
    "dispatch_throughput",
    "dispatch_outcomes",
    "dispatch_latency",
    "slo",
)


def telemetry_client_entry(context, host, port, gate_tcp_address, metrics_targets, job_count) -> None:
    """Client child: run ``job_count`` jobs through the gate at once, then
    record every node's metrics reply.

    ``metrics_targets`` maps each node's process name to its TCP address;
    the gate's is named ``gate``, the manager's ``manager``.
    """
    settings = _env()
    client = HyperscaleClient(host=host, port=port, env=settings, gates=[gate_tcp_address], **context.sim_kwargs())
    log: list = []
    context.set_result(log)

    async def submit_until_accepted() -> str:
        while True:
            try:
                return await client.submit_job(workflows=[([], SimPingWorkflow())], vus=2, timeout_seconds=30.0)
            except Exception:
                await asyncio.sleep(1.0)

    async def run_job() -> None:
        job_id = await submit_until_accepted()
        result = await client.wait_for_job(job_id, timeout=60.0)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    async def metrics_of(node_name: str) -> ClusterMetricsReply | None:
        try:
            return await client.cluster_metrics(metrics_targets[node_name], settings.MANAGER_TCP_TIMEOUT_STANDARD)
        except Exception as metrics_error:
            log.append(("metrics-failed", node_name, type(metrics_error).__name__, round(context.loop.time(), 6)))
            return None

    async def metrics_once(node_name: str, settled) -> ClusterMetricsReply | None:
        while (reply := await metrics_of(node_name)) is not None and not settled(reply):
            await asyncio.sleep(settings.SWIM_UDP_POLL_INTERVAL)
        return reply

    def settled(reply: ClusterMetricsReply) -> bool:
        if reply.role == "worker":
            return not any(reply.workload.values())
        return reply.role != "gate" or bool(reply.slo)

    def recorded_fields(reply: ClusterMetricsReply, worker_names: dict[str, str]) -> dict:
        fields = {field_name: getattr(reply, field_name) for field_name in REPLY_FIELDS}
        fields["dispatch_latency"] = {
            worker_names.get(worker_id, "unknown-worker"): latency
            for worker_id, latency in reply.dispatch_latency.items()
        }
        return fields

    async def run() -> None:
        await client.start()
        await asyncio.gather(*(run_job() for _ in range(job_count)))
        replies = {node_name: await metrics_once(node_name, settled) for node_name in sorted(metrics_targets)}
        worker_names = {
            reply.member_id.partition("@")[0]: node_name
            for node_name, reply in replies.items()
            if reply is not None and reply.role == "worker"
        }
        for node_name, reply in sorted(replies.items()):
            if reply is not None:
                log.append(("metrics", node_name, recorded_fields(reply, worker_names), round(context.loop.time(), 6)))

    context.loop.create_task(run())
