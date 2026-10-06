"""
`helm test`: submit one workflow through the gates and wait for it to
complete. Exits 0 when the job completes, 1 otherwise.

Settings (environment):
  PROBE_HOST            this pod's IP; gates push job updates back to it
  PROBE_PORT            the TCP port to receive them on (UDP: PROBE_PORT + 1)
  PROBE_GATES           space-separated gate host:port addresses
  PROBE_TIMEOUT_SECONDS how long the job may take, including waiting for
                        the cluster to accept it
  MERCURY_SYNC_AUTH_SECRET  the cluster secret
"""

import asyncio
import os
import sys
import time

from hyperscale.commands.run.node_address import parse_node_address
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step

SUBMIT_RETRY_SECONDS = 1.0


class ClusterProbe(Workflow):
    """One action step that returns at once."""

    vus = 1

    @step()
    async def probe(self) -> dict[str, str]:
        return {"status": "ok"}


async def submit_until_accepted(client: HyperscaleClient, deadline: float, timeout_seconds: float) -> str:
    """The gates refuse work until a datacenter has reported capacity."""
    while True:
        try:
            return await client.submit_job(
                workflows=[([], ClusterProbe())],
                vus=1,
                timeout_seconds=timeout_seconds,
            )

        except Exception as submit_error:
            if time.monotonic() >= deadline:
                raise

            print(f"probe: not accepted yet ({submit_error}); retrying", flush=True)
            await asyncio.sleep(SUBMIT_RETRY_SECONDS)


async def main() -> int:
    timeout_seconds = float(os.environ["PROBE_TIMEOUT_SECONDS"])
    deadline = time.monotonic() + timeout_seconds
    client = HyperscaleClient(
        host=os.environ["PROBE_HOST"],
        port=int(os.environ["PROBE_PORT"]),
        env=Env(MERCURY_SYNC_AUTH_SECRET=os.environ["MERCURY_SYNC_AUTH_SECRET"]),
        gates=[parse_node_address(gate) for gate in os.environ["PROBE_GATES"].split()],
    )
    await client.start()

    try:
        job_id = await submit_until_accepted(client, deadline, timeout_seconds)
        print(f"probe: job {job_id} accepted", flush=True)
        result = await client.wait_for_job(job_id, timeout=max(1.0, deadline - time.monotonic()))
        print(f"probe: job {job_id} finished {result.status}", flush=True)
        return 0 if result.status == "completed" else 1

    finally:
        await client.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
