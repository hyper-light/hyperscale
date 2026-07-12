"""
Restart truth-telling — what a restarted manager DOES with jobs
recovered ACTIVE from its WAL.

A restarted manager has lost the in-flight state a resumed dispatch
would need (worker assignments, workflow progress, in-memory
callbacks), so pretending a recovered job is still running is a silent
strand. The contract pinned here:

* every recovered ACTIVE job transitions to FAILED durably (the record
  lands FIRST — a missed notification still leaves queries truthful);
* the client's recorded callback contact gets a best-effort final
  JobStatusPush;
* a notification failure is logged, never raised (the client may be
  gone — that cannot wedge manager start);
* the ``job_status`` query endpoint answers for recovered jobs from
  the ledger (mapping ledger-internal vocabulary onto the client's)
  and returns empty bytes for unknown jobs.

Built on a bare ``ManagerServer`` instance (``object.__new__`` + the
attributes the routines touch — the same pattern as the SWIM
receive-path tests) with a REAL JobLedger over SimFilesystem.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.models.distributed import (
    GlobalJobStatus,
    JobStatus,
    JobStatusPush,
)
from hyperscale.distributed.nodes.manager.server import ManagerServer
from tests.simulation.harness.sim import SimFilesystem


class _RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def log(self, model) -> None:
        self.messages.append(model.message)


class _NoJobManager:
    def get_job_by_id(self, job_id: str):
        return None


async def _open_ledger(filesystem: SimFilesystem) -> JobLedger:
    return await JobLedger.open(
        wal_path=Path("/manager/ledger/wal"),
        checkpoint_dir=Path("/manager/ledger/checkpoints"),
        archive_dir=Path("/manager/ledger/archive"),
        region_code="dc-east",
        gate_id="mgr-1",
        node_id=1,
        filesystem=filesystem,
    )


def _bare_manager(ledger: JobLedger, send_recorder: list) -> ManagerServer:
    manager = object.__new__(ManagerServer)
    manager._job_ledger = ledger
    manager._job_manager = _NoJobManager()
    manager._udp_logger = _RecordingLogger()
    manager._node_id = SimpleNamespace(short="mgr-1", datacenter="dc-east")
    manager._host = "127.0.0.1"
    manager._tcp_port = 9000

    async def send_tcp(addr, action, data, timeout=None):
        send_recorder.append((addr, action, data))
        return (b"ok", 0)

    manager.send_tcp = send_tcp
    return manager


async def _seed_active_job(
    ledger: JobLedger, job_id: str, requestor_id: str
) -> None:
    _job_id, create_result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id=requestor_id,
        durability=DurabilityLevel.LOCAL,
        job_id=job_id,
    )
    assert create_result.success
    accept_result = await ledger.accept_job(
        job_id,
        datacenter_id="dc-east",
        worker_count=2,
        durability=DurabilityLevel.LOCAL,
    )
    assert accept_result is not None and accept_result.success


@pytest.mark.asyncio
async def test_recovered_active_job_fails_durably_and_notifies() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    await _seed_active_job(ledger, "job-r1", "10.0.0.5:8500")
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    sent: list = []
    manager = _bare_manager(recovered, sent)

    await manager._fail_recovered_active_jobs()

    # Durable terminal record.
    job_state = recovered.get_job("job-r1")
    assert job_state is not None
    assert job_state.status == JobStatus.FAILED.value

    # Best-effort push to the recorded requestor contact.
    assert len(sent) == 1
    push_addr, push_action, push_data = sent[0]
    assert push_addr == ("10.0.0.5", 8500)
    assert push_action == "job_status_push"
    push = JobStatusPush.load(push_data)
    assert push.job_id == "job-r1"
    assert push.status == JobStatus.FAILED.value
    assert push.is_final

    await recovered.close()


@pytest.mark.asyncio
async def test_notification_failure_is_logged_never_raised() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    await _seed_active_job(ledger, "job-r2", "10.0.0.6:8500")
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    manager = _bare_manager(recovered, [])

    async def failing_send(addr, action, data, timeout=None):
        raise ConnectionRefusedError("client is gone")

    manager.send_tcp = failing_send

    await manager._fail_recovered_active_jobs()

    # The durable record still landed; the failure went to the log.
    job_state = recovered.get_job("job-r2")
    assert job_state is not None
    assert job_state.status == JobStatus.FAILED.value
    assert any(
        "notification" in message.lower()
        for message in manager._udp_logger.messages
    )

    await recovered.close()


@pytest.mark.asyncio
async def test_malformed_requestor_contact_skips_notification() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    await _seed_active_job(ledger, "job-r3", "")
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    sent: list = []
    manager = _bare_manager(recovered, sent)

    await manager._fail_recovered_active_jobs()

    assert sent == []
    job_state = recovered.get_job("job-r3")
    assert job_state is not None
    assert job_state.status == JobStatus.FAILED.value

    await recovered.close()


@pytest.mark.asyncio
async def test_job_status_query_answers_from_recovered_ledger() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    await _seed_active_job(ledger, "job-r4", "10.0.0.7:8500")
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    manager = _bare_manager(recovered, [])
    await manager._fail_recovered_active_jobs()

    response = await manager.job_status(("10.0.0.7", 40000), b"job-r4", 0)
    status = GlobalJobStatus.load(response)
    assert status.job_id == "job-r4"
    assert status.status == JobStatus.FAILED.value

    unknown = await manager.job_status(("10.0.0.7", 40000), b"job-nope", 0)
    assert unknown == b""

    await recovered.close()


@pytest.mark.asyncio
async def test_job_status_query_maps_ledger_vocabulary() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    # Created but never accepted: ledger-internal status "pending".
    _job_id, create_result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="10.0.0.8:8500",
        durability=DurabilityLevel.LOCAL,
        job_id="job-r5",
    )
    assert create_result.success

    manager = _bare_manager(ledger, [])
    response = await manager.job_status(("10.0.0.8", 40000), b"job-r5", 0)
    status = GlobalJobStatus.load(response)
    # "pending" is not client vocabulary; the boundary maps it.
    assert status.status == JobStatus.SUBMITTED.value

    await ledger.close()
