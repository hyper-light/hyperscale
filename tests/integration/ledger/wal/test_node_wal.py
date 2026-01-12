import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.wal import NodeWAL, WALEntryState
from hyperscale.logging.lsn import HybridLamportClock


@pytest.fixture
def temp_wal_directory():
    temp_dir = tempfile.mkdtemp(prefix="test_wal_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def clock():
    return HybridLamportClock(node_id=1)


class TestNodeWALBasicOperations:
    @pytest.mark.asyncio
    async def test_open_creates_new_wal(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"

        wal = await NodeWAL.open(path=wal_path, clock=clock)

        assert wal.next_lsn == 0
        assert wal.last_synced_lsn == -1
        assert wal.pending_count == 0
        assert not wal.is_closed

        await wal.close()

    @pytest.mark.asyncio
    async def test_append_single_entry(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=b"test payload",
        )

        assert entry.lsn == 0
        assert entry.state == WALEntryState.PENDING
        assert entry.payload == b"test payload"
        assert wal.next_lsn == 1
        assert wal.last_synced_lsn == 0
        assert wal.pending_count == 1

        await wal.close()

    @pytest.mark.asyncio
    async def test_append_multiple_entries(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entries = []
        for idx in range(10):
            entry = await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"payload_{idx}".encode(),
            )
            entries.append(entry)

        assert len(entries) == 10
        assert wal.next_lsn == 10
        assert wal.last_synced_lsn == 9
        assert wal.pending_count == 10

        for idx, entry in enumerate(entries):
            assert entry.lsn == idx

        await wal.close()


class TestNodeWALRecovery:
    @pytest.mark.asyncio
    async def test_recovery_reads_all_entries(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"

        wal = await NodeWAL.open(path=wal_path, clock=clock)
        for idx in range(5):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"entry_{idx}".encode(),
            )
        await wal.close()

        recovered_wal = await NodeWAL.open(path=wal_path, clock=clock)

        assert recovered_wal.next_lsn == 5
        assert recovered_wal.pending_count == 5

        pending = recovered_wal.get_pending_entries()
        assert len(pending) == 5

        await recovered_wal.close()

    @pytest.mark.asyncio
    async def test_recovery_handles_empty_file(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal_path.parent.mkdir(parents=True, exist_ok=True)
        wal_path.touch()

        wal = await NodeWAL.open(path=wal_path, clock=clock)

        assert wal.next_lsn == 0
        assert wal.pending_count == 0

        await wal.close()

    @pytest.mark.asyncio
    async def test_recovery_continues_lsn_sequence(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"

        wal = await NodeWAL.open(path=wal_path, clock=clock)
        for idx in range(3):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"first_batch_{idx}".encode(),
            )
        await wal.close()

        wal = await NodeWAL.open(path=wal_path, clock=clock)
        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=b"after_recovery",
        )

        assert entry.lsn == 3

        await wal.close()


class TestNodeWALStateTransitions:
    @pytest.mark.asyncio
    async def test_mark_regional(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=b"test",
        )

        await wal.mark_regional(entry.lsn)

        pending = wal.get_pending_entries()
        assert len(pending) == 1
        assert pending[0].state == WALEntryState.REGIONAL

        await wal.close()

    @pytest.mark.asyncio
    async def test_mark_global(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=b"test",
        )

        await wal.mark_regional(entry.lsn)
        await wal.mark_global(entry.lsn)

        pending = wal.get_pending_entries()
        assert len(pending) == 1
        assert pending[0].state == WALEntryState.GLOBAL

        await wal.close()

    @pytest.mark.asyncio
    async def test_mark_applied(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=b"test",
        )

        await wal.mark_regional(entry.lsn)
        await wal.mark_global(entry.lsn)
        await wal.mark_applied(entry.lsn)

        pending = wal.get_pending_entries()
        assert len(pending) == 0

        await wal.close()

    @pytest.mark.asyncio
    async def test_compact_removes_applied_entries(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        for idx in range(5):
            entry = await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"entry_{idx}".encode(),
            )
            if idx < 3:
                await wal.mark_regional(entry.lsn)
                await wal.mark_global(entry.lsn)
                await wal.mark_applied(entry.lsn)

        compacted = await wal.compact(up_to_lsn=2)

        assert compacted == 3
        assert wal.pending_count == 2

        await wal.close()


class TestNodeWALConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_appends(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        async def append_entries(prefix: str, count: int):
            entries = []
            for idx in range(count):
                entry = await wal.append(
                    event_type=JobEventType.JOB_CREATED,
                    payload=f"{prefix}_{idx}".encode(),
                )
                entries.append(entry)
            return entries

        results = await asyncio.gather(
            append_entries("task_a", 20),
            append_entries("task_b", 20),
            append_entries("task_c", 20),
        )

        all_entries = [entry for batch in results for entry in batch]
        all_lsns = [entry.lsn for entry in all_entries]

        assert len(all_lsns) == 60
        assert len(set(all_lsns)) == 60

        assert wal.next_lsn == 60
        assert wal.pending_count == 60

        await wal.close()

    @pytest.mark.asyncio
    async def test_concurrent_appends_and_state_transitions(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        entries_lock = asyncio.Lock()
        appended_entries: list[int] = []

        async def append_entries(count: int):
            for _ in range(count):
                entry = await wal.append(
                    event_type=JobEventType.JOB_CREATED,
                    payload=b"test",
                )
                async with entries_lock:
                    appended_entries.append(entry.lsn)

        async def transition_entries():
            await asyncio.sleep(0.001)
            for _ in range(50):
                async with entries_lock:
                    if appended_entries:
                        lsn = appended_entries[0]
                    else:
                        lsn = None

                if lsn is not None:
                    await wal.mark_regional(lsn)
                    await wal.mark_global(lsn)
                    await wal.mark_applied(lsn)
                    async with entries_lock:
                        if lsn in appended_entries:
                            appended_entries.remove(lsn)

                await asyncio.sleep(0.0001)

        await asyncio.gather(
            append_entries(30),
            transition_entries(),
        )

        await wal.close()

    @pytest.mark.asyncio
    async def test_high_concurrency_stress(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(
            path=wal_path,
            clock=clock,
            batch_max_entries=100,
        )

        async def writer(writer_id: int, count: int):
            for idx in range(count):
                await wal.append(
                    event_type=JobEventType.JOB_CREATED,
                    payload=f"writer_{writer_id}_entry_{idx}".encode(),
                )

        writers = [writer(idx, 50) for idx in range(10)]
        await asyncio.gather(*writers)

        assert wal.next_lsn == 500
        assert wal.pending_count == 500

        await wal.close()

        recovered = await NodeWAL.open(path=wal_path, clock=clock)
        assert recovered.next_lsn == 500
        assert recovered.pending_count == 500

        await recovered.close()


class TestNodeWALEdgeCases:
    @pytest.mark.asyncio
    async def test_append_after_close_raises(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        await wal.close()

        with pytest.raises(RuntimeError, match="WAL is closed"):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=b"should fail",
            )

    @pytest.mark.asyncio
    async def test_double_close_is_safe(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        await wal.close()
        await wal.close()

        assert wal.is_closed

    @pytest.mark.asyncio
    async def test_iter_from_reads_entries(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        for idx in range(10):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"entry_{idx}".encode(),
            )

        entries = []
        async for entry in wal.iter_from(start_lsn=5):
            entries.append(entry)

        assert len(entries) == 5
        assert entries[0].lsn == 5
        assert entries[-1].lsn == 9

        await wal.close()

    @pytest.mark.asyncio
    async def test_large_payload(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        large_payload = b"x" * (1024 * 100)

        entry = await wal.append(
            event_type=JobEventType.JOB_CREATED,
            payload=large_payload,
        )

        assert entry.payload == large_payload

        await wal.close()

        recovered = await NodeWAL.open(path=wal_path, clock=clock)
        pending = recovered.get_pending_entries()

        assert len(pending) == 1
        assert pending[0].payload == large_payload

        await recovered.close()

    @pytest.mark.asyncio
    async def test_mark_nonexistent_lsn_is_safe(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        await wal.mark_regional(999)
        await wal.mark_global(999)
        await wal.mark_applied(999)

        await wal.close()

    @pytest.mark.asyncio
    async def test_compact_with_no_applied_entries(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        for idx in range(5):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"entry_{idx}".encode(),
            )

        compacted = await wal.compact(up_to_lsn=10)

        assert compacted == 0
        assert wal.pending_count == 5

        await wal.close()


class TestNodeWALDurability:
    @pytest.mark.asyncio
    async def test_entries_survive_crash_simulation(
        self,
        temp_wal_directory: str,
        clock: HybridLamportClock,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        for idx in range(10):
            await wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=f"durable_entry_{idx}".encode(),
            )

        await wal.close()

        assert wal_path.exists()
        assert wal_path.stat().st_size > 0

        recovered = await NodeWAL.open(path=wal_path, clock=clock)

        assert recovered.next_lsn == 10
        pending = recovered.get_pending_entries()
        assert len(pending) == 10

        for idx, entry in enumerate(sorted(pending, key=lambda e: e.lsn)):
            assert entry.payload == f"durable_entry_{idx}".encode()

        await recovered.close()
