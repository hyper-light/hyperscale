import asyncio
import os

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream

from .conftest import create_mock_stream_writer


class TestConcurrentWrites:
    @pytest.mark.asyncio
    async def test_concurrent_writes_to_same_file(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        async def write_entries(start_idx: int, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"concurrent {start_idx + idx}")
                await json_logger_stream.log(entry)

        await asyncio.gather(
            write_entries(0, 10),
            write_entries(100, 10),
            write_entries(200, 10),
        )

        log_path = os.path.join(temp_log_directory, "test.json")
        entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 30

    @pytest.mark.asyncio
    async def test_concurrent_writes_binary_format(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        async def write_entries(prefix: str, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"{prefix}_{idx}")
                await binary_logger_stream.log(entry)

        await asyncio.gather(
            write_entries("alpha", 10),
            write_entries("beta", 10),
            write_entries("gamma", 10),
        )

        log_path = os.path.join(temp_log_directory, "test.wal")
        entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 30

    @pytest.mark.asyncio
    async def test_lsns_are_unique_under_concurrency(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        lsns = []
        lock = asyncio.Lock()

        async def write_and_collect(start_idx: int, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"unique test {start_idx + idx}")
                lsn = await json_logger_stream.log(entry)
                async with lock:
                    lsns.append(lsn)

        await asyncio.gather(
            write_and_collect(0, 20),
            write_and_collect(100, 20),
            write_and_collect(200, 20),
        )

        assert len(set(lsns)) == len(lsns), "Duplicate LSNs detected"


class TestConcurrentReadsAndWrites:
    @pytest.mark.asyncio
    async def test_read_while_writing(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="test_concurrent_rw",
            filename="concurrent_rw.json",
            directory=temp_log_directory,
            durability=DurabilityMode.FLUSH,
            log_format="json",
            enable_lsn=True,
            instance_id=1,
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        for idx in range(10):
            entry = sample_entry_factory(message=f"initial {idx}")
            await stream.log(entry)

        write_complete = asyncio.Event()
        read_results = []

        async def writer():
            for idx in range(10, 20):
                entry = sample_entry_factory(message=f"concurrent {idx}")
                await stream.log(entry)
                await asyncio.sleep(0.001)
            write_complete.set()

        async def reader():
            await asyncio.sleep(0.005)
            log_path = os.path.join(temp_log_directory, "concurrent_rw.json")
            async for offset, log, lsn in stream.read_entries(log_path):
                read_results.append(log)

        await asyncio.gather(writer(), reader())

        assert len(read_results) >= 10
        await stream.close()


class TestConcurrentBatchFsync:
    @pytest.mark.asyncio
    async def test_concurrent_batch_fsync_writes(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="test_batch_concurrent",
            filename="batch_concurrent.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        stream._batch_max_size = 20
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        async def write_batch(prefix: str, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"{prefix}_{idx}")
                await stream.log(entry)

        await asyncio.gather(
            write_batch("batch_a", 15),
            write_batch("batch_b", 15),
            write_batch("batch_c", 15),
        )

        await asyncio.sleep(0.05)

        log_path = os.path.join(temp_log_directory, "batch_concurrent.wal")
        entries = []
        async for offset, log, lsn in stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 45
        await stream.close()


class TestMultipleStreams:
    @pytest.mark.asyncio
    async def test_multiple_streams_different_files(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        streams = []
        for idx in range(3):
            stream = LoggerStream(
                name=f"stream_{idx}",
                filename=f"stream_{idx}.json",
                directory=temp_log_directory,
                durability=DurabilityMode.FLUSH,
                log_format="json",
                enable_lsn=True,
                instance_id=idx,
            )
            await stream.initialize(
                stdout_writer=create_mock_stream_writer(),
                stderr_writer=create_mock_stream_writer(),
            )
            streams.append(stream)

        async def write_to_stream(stream: LoggerStream, stream_idx: int, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"stream_{stream_idx}_msg_{idx}")
                await stream.log(entry)

        await asyncio.gather(
            *[write_to_stream(stream, idx, 10) for idx, stream in enumerate(streams)]
        )

        for idx, stream in enumerate(streams):
            log_path = os.path.join(temp_log_directory, f"stream_{idx}.json")
            entries = []
            async for offset, log, lsn in stream.read_entries(log_path):
                entries.append(log)
            assert len(entries) == 10

        for stream in streams:
            await stream.close()


class TestHighConcurrencyLoad:
    @pytest.mark.asyncio
    async def test_high_concurrency_writes(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="high_concurrency",
            filename="high_concurrency.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FLUSH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        async def write_entries(task_id: int, count: int):
            for idx in range(count):
                entry = sample_entry_factory(message=f"task_{task_id}_entry_{idx}")
                await stream.log(entry)

        tasks = [write_entries(task_id, 20) for task_id in range(10)]
        await asyncio.gather(*tasks)

        log_path = os.path.join(temp_log_directory, "high_concurrency.wal")
        entries = []
        async for offset, log, lsn in stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 200

        lsns = []
        async for offset, log, lsn in stream.read_entries(log_path):
            lsns.append(lsn)

        assert len(set(lsns)) == len(lsns), "Duplicate LSNs detected under high load"

        await stream.close()
