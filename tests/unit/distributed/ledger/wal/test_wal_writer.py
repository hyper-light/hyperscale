import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.wal.wal_writer import (
    WALWriter,
    WALWriterConfig,
    WriteRequest,
)
from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.distributed.reliability.robust_queue import QueueState


@pytest.fixture
def temp_wal_directory():
    temp_dir = tempfile.mkdtemp(prefix="test_wal_writer_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestWALWriterBasicOperations:
    @pytest.mark.asyncio
    async def test_start_and_stop(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        assert writer.is_running
        assert not writer.has_error

        await writer.stop()

        assert not writer.is_running

    @pytest.mark.asyncio
    async def test_write_single_entry(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        request = WriteRequest(
            data=b"test data",
            future=future,
        )

        writer.submit(request)

        await asyncio.wait_for(future, timeout=5.0)

        await writer.stop()

        assert wal_path.exists()
        with open(wal_path, "rb") as f:
            assert f.read() == b"test data"

    @pytest.mark.asyncio
    async def test_write_multiple_entries(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for idx in range(10):
            future: asyncio.Future[None] = loop.create_future()
            request = WriteRequest(
                data=f"entry_{idx}\n".encode(),
                future=future,
            )
            writer.submit(request)
            futures.append(future)

        await asyncio.gather(*futures)

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        for idx in range(10):
            assert f"entry_{idx}\n".encode() in content


class TestWALWriterBatching:
    @pytest.mark.asyncio
    async def test_batch_writes(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            batch_timeout_microseconds=10000,
            batch_max_entries=50,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for idx in range(100):
            future: asyncio.Future[None] = loop.create_future()
            request = WriteRequest(
                data=f"batch_entry_{idx}|".encode(),
                future=future,
            )
            writer.submit(request)
            futures.append(future)

        await asyncio.gather(*futures)

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        for idx in range(100):
            assert f"batch_entry_{idx}|".encode() in content

    @pytest.mark.asyncio
    async def test_batch_max_bytes_triggers_commit(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            batch_timeout_microseconds=1000000,
            batch_max_entries=1000,
            batch_max_bytes=1024,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        large_data = b"x" * 512
        for _ in range(4):
            future: asyncio.Future[None] = loop.create_future()
            request = WriteRequest(
                data=large_data,
                future=future,
            )
            writer.submit(request)
            futures.append(future)

        await asyncio.gather(*futures)

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        assert len(content) == 512 * 4


class TestWALWriterConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_submits(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()

        async def submit_entries(prefix: str, count: int):
            futures = []
            for idx in range(count):
                future: asyncio.Future[None] = loop.create_future()
                request = WriteRequest(
                    data=f"{prefix}_{idx}|".encode(),
                    future=future,
                )
                writer.submit(request)
                futures.append(future)
            await asyncio.gather(*futures)

        await asyncio.gather(
            submit_entries("task_a", 50),
            submit_entries("task_b", 50),
            submit_entries("task_c", 50),
        )

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        for prefix in ["task_a", "task_b", "task_c"]:
            for idx in range(50):
                assert f"{prefix}_{idx}|".encode() in content

    @pytest.mark.asyncio
    async def test_high_concurrency_stress(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(batch_max_entries=100)
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        all_futures = []

        async def submit_batch(batch_id: int, count: int):
            futures = []
            for idx in range(count):
                future: asyncio.Future[None] = loop.create_future()
                request = WriteRequest(
                    data=f"b{batch_id}_e{idx}|".encode(),
                    future=future,
                )
                writer.submit(request)
                futures.append(future)
            return futures

        for batch_id in range(20):
            batch_futures = await submit_batch(batch_id, 25)
            all_futures.extend(batch_futures)

        await asyncio.gather(*all_futures)

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        entry_count = content.count(b"|")
        assert entry_count == 500


class TestWALWriterErrorHandling:
    @pytest.mark.asyncio
    async def test_submit_before_start_fails_future(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        request = WriteRequest(
            data=b"should fail",
            future=future,
        )

        writer.submit(request)

        with pytest.raises(RuntimeError, match="not running"):
            await asyncio.wait_for(future, timeout=1.0)

    @pytest.mark.asyncio
    async def test_submit_after_stop_fails_future(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()
        await writer.stop()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        request = WriteRequest(
            data=b"should fail",
            future=future,
        )

        writer.submit(request)

        with pytest.raises(RuntimeError, match="not running"):
            await asyncio.wait_for(future, timeout=1.0)

    @pytest.mark.asyncio
    async def test_double_start_is_safe(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()
        await writer.start()

        assert writer.is_running

        await writer.stop()

    @pytest.mark.asyncio
    async def test_double_stop_is_safe(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()
        await writer.stop()
        await writer.stop()

        assert not writer.is_running


class TestWALWriterFutureResolution:
    @pytest.mark.asyncio
    async def test_futures_resolve_in_order_of_submission(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            batch_timeout_microseconds=100000,
            batch_max_entries=10,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        resolution_order = []

        async def track_resolution(idx: int, future: asyncio.Future[None]):
            await future
            resolution_order.append(idx)

        futures = []
        for idx in range(10):
            future: asyncio.Future[None] = loop.create_future()
            request = WriteRequest(
                data=f"entry_{idx}".encode(),
                future=future,
            )
            writer.submit(request)
            futures.append(track_resolution(idx, future))

        await asyncio.gather(*futures)

        await writer.stop()

        assert len(resolution_order) == 10

    @pytest.mark.asyncio
    async def test_cancelled_future_handled_gracefully(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(batch_timeout_microseconds=100000)
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()

        future1: asyncio.Future[None] = loop.create_future()
        future2: asyncio.Future[None] = loop.create_future()
        future3: asyncio.Future[None] = loop.create_future()

        writer.submit(WriteRequest(data=b"entry_1", future=future1))
        writer.submit(WriteRequest(data=b"entry_2", future=future2))
        writer.submit(WriteRequest(data=b"entry_3", future=future3))

        future2.cancel()

        await asyncio.wait_for(future1, timeout=5.0)
        await asyncio.wait_for(future3, timeout=5.0)

        await writer.stop()


class TestWALWriterFileCreation:
    @pytest.mark.asyncio
    async def test_creates_parent_directories(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "nested" / "deep" / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        writer.submit(WriteRequest(data=b"test", future=future))
        await future

        await writer.stop()

        assert wal_path.exists()
        assert wal_path.parent.exists()

    @pytest.mark.asyncio
    async def test_appends_to_existing_file(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"

        wal_path.parent.mkdir(parents=True, exist_ok=True)
        with open(wal_path, "wb") as f:
            f.write(b"existing_content|")

        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        writer.submit(WriteRequest(data=b"new_content", future=future))
        await future

        await writer.stop()

        with open(wal_path, "rb") as f:
            content = f.read()

        assert content == b"existing_content|new_content"


class TestWALWriterBackpressure:
    @pytest.mark.asyncio
    async def test_submit_returns_queue_put_result(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        result = writer.submit(WriteRequest(data=b"test", future=future))

        assert result.accepted is True
        assert result.dropped is False
        assert result.in_overflow is False
        assert result.queue_state == QueueState.HEALTHY
        assert result.backpressure.level == BackpressureLevel.NONE

        await future
        await writer.stop()

    @pytest.mark.asyncio
    async def test_backpressure_level_property(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        assert writer.backpressure_level == BackpressureLevel.NONE

        await writer.stop()

    @pytest.mark.asyncio
    async def test_queue_state_property(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        assert writer.queue_state == QueueState.HEALTHY

        await writer.stop()

    @pytest.mark.asyncio
    async def test_throttle_threshold_triggers_backpressure(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            queue_max_size=100,
            throttle_threshold=0.70,
            batch_timeout_microseconds=1000000,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(75):
            future: asyncio.Future[None] = loop.create_future()
            result = writer.submit(WriteRequest(data=b"x" * 10, future=future))
            futures.append(future)

        assert writer.backpressure_level >= BackpressureLevel.THROTTLE

        await asyncio.gather(*futures)
        await writer.stop()

    @pytest.mark.asyncio
    async def test_batch_threshold_triggers_batch_level(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            queue_max_size=100,
            throttle_threshold=0.70,
            batch_threshold=0.85,
            batch_timeout_microseconds=1000000,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(90):
            future: asyncio.Future[None] = loop.create_future()
            result = writer.submit(WriteRequest(data=b"x" * 10, future=future))
            futures.append(future)

        assert writer.backpressure_level >= BackpressureLevel.BATCH

        await asyncio.gather(*futures)
        await writer.stop()

    @pytest.mark.asyncio
    async def test_reject_threshold_rejects_writes(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            queue_max_size=100,
            overflow_size=10,
            reject_threshold=0.95,
            batch_timeout_microseconds=10000000,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        accepted_futures = []
        rejected_count = 0

        for _ in range(150):
            future: asyncio.Future[None] = loop.create_future()
            result = writer.submit(WriteRequest(data=b"x" * 10, future=future))
            if result.accepted:
                accepted_futures.append(future)
            else:
                rejected_count += 1

        assert rejected_count > 0
        assert writer.metrics.total_rejected > 0

        await asyncio.gather(*accepted_futures)
        await writer.stop()

    @pytest.mark.asyncio
    async def test_overflow_buffer_used_when_primary_full(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            queue_max_size=50,
            overflow_size=20,
            batch_timeout_microseconds=10000000,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []
        overflow_count = 0

        for _ in range(65):
            future: asyncio.Future[None] = loop.create_future()
            result = writer.submit(WriteRequest(data=b"x" * 10, future=future))
            if result.accepted:
                futures.append(future)
                if result.in_overflow:
                    overflow_count += 1

        assert overflow_count > 0
        assert writer.metrics.total_overflow > 0

        await asyncio.gather(*futures)
        await writer.stop()


class TestWALWriterStateChangeCallback:
    @pytest.mark.asyncio
    async def test_callback_invoked_on_state_change(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"

        state_changes: list[tuple[QueueState, BackpressureSignal]] = []

        async def on_state_change(
            queue_state: QueueState,
            backpressure: BackpressureSignal,
        ):
            state_changes.append((queue_state, backpressure))

        config = WALWriterConfig(
            queue_max_size=50,
            throttle_threshold=0.50,
            batch_timeout_microseconds=10000000,
        )
        writer = WALWriter(
            path=wal_path,
            config=config,
            state_change_callback=on_state_change,
        )

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(30):
            future: asyncio.Future[None] = loop.create_future()
            writer.submit(WriteRequest(data=b"x" * 10, future=future))
            futures.append(future)

        await asyncio.sleep(0.1)

        await asyncio.gather(*futures)
        await writer.stop()

        assert len(state_changes) > 0
        states = [change[0] for change in state_changes]
        assert QueueState.THROTTLED in states


class TestWALWriterMetrics:
    @pytest.mark.asyncio
    async def test_metrics_track_submissions(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(10):
            future: asyncio.Future[None] = loop.create_future()
            writer.submit(WriteRequest(data=b"test data", future=future))
            futures.append(future)

        await asyncio.gather(*futures)
        await writer.stop()

        metrics = writer.metrics
        assert metrics.total_submitted == 10
        assert metrics.total_written == 10
        assert metrics.total_batches >= 1
        assert metrics.total_bytes_written == 10 * len(b"test data")
        assert metrics.total_fsyncs >= 1

    @pytest.mark.asyncio
    async def test_get_queue_metrics_includes_all_data(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(5):
            future: asyncio.Future[None] = loop.create_future()
            writer.submit(WriteRequest(data=b"test", future=future))
            futures.append(future)

        await asyncio.gather(*futures)

        queue_metrics = writer.get_queue_metrics()

        assert "total_submitted" in queue_metrics
        assert "total_written" in queue_metrics
        assert "total_batches" in queue_metrics
        assert "total_bytes_written" in queue_metrics
        assert "total_fsyncs" in queue_metrics
        assert "total_rejected" in queue_metrics
        assert "total_overflow" in queue_metrics
        assert "peak_queue_size" in queue_metrics
        assert "peak_batch_size" in queue_metrics

        assert queue_metrics["total_submitted"] == 5
        assert queue_metrics["total_written"] == 5

        await writer.stop()

    @pytest.mark.asyncio
    async def test_peak_batch_size_tracked(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(
            batch_timeout_microseconds=100000,
            batch_max_entries=50,
        )
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(25):
            future: asyncio.Future[None] = loop.create_future()
            writer.submit(WriteRequest(data=b"x", future=future))
            futures.append(future)

        await asyncio.gather(*futures)
        await writer.stop()

        assert writer.metrics.peak_batch_size > 0
        assert writer.metrics.peak_batch_size <= 25


class TestWALWriterErrorRecovery:
    @pytest.mark.asyncio
    async def test_error_state_propagated_to_new_submissions(
        self,
        temp_wal_directory: str,
    ):
        wal_path = Path(temp_wal_directory) / "test.wal"
        writer = WALWriter(path=wal_path)

        await writer.start()

        loop = asyncio.get_running_loop()
        future1: asyncio.Future[None] = loop.create_future()
        writer.submit(WriteRequest(data=b"first", future=future1))
        await future1

        await writer.stop()

        future2: asyncio.Future[None] = loop.create_future()
        result = writer.submit(WriteRequest(data=b"after_stop", future=future2))

        assert result.accepted is False
        assert result.dropped is True
        assert result.queue_state == QueueState.SATURATED

    @pytest.mark.asyncio
    async def test_pending_requests_failed_on_stop(self, temp_wal_directory: str):
        wal_path = Path(temp_wal_directory) / "test.wal"
        config = WALWriterConfig(batch_timeout_microseconds=10000000)
        writer = WALWriter(path=wal_path, config=config)

        await writer.start()

        loop = asyncio.get_running_loop()
        futures = []

        for _ in range(5):
            future: asyncio.Future[None] = loop.create_future()
            writer.submit(WriteRequest(data=b"pending", future=future))
            futures.append(future)

        await writer.stop()

        completed_or_failed = 0
        for future in futures:
            if future.done():
                completed_or_failed += 1

        assert completed_or_failed == 5
