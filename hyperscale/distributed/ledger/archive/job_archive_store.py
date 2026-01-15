from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import msgspec

from ..job_state import JobState


class JobArchiveStore:
    __slots__ = ("_archive_dir", "_loop")

    def __init__(self, archive_dir: Path) -> None:
        self._archive_dir = archive_dir
        self._loop: asyncio.AbstractEventLoop | None = None

    async def initialize(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._loop.run_in_executor(
            None,
            self._initialize_sync,
        )

    def _initialize_sync(self) -> None:
        self._archive_dir.mkdir(parents=True, exist_ok=True)

    def _get_archive_path(self, job_id: str) -> Path:
        parts = job_id.split("-")
        if len(parts) >= 2:
            region = parts[0]
            timestamp_ms = parts[1]
            shard = timestamp_ms[:10] if len(timestamp_ms) >= 10 else timestamp_ms
            return self._archive_dir / region / shard / f"{job_id}.bin"

        return self._archive_dir / "unknown" / f"{job_id}.bin"

    async def write_if_absent(self, job_state: JobState) -> bool:
        loop = self._loop
        assert loop is not None

        archive_path = self._get_archive_path(job_state.job_id)

        return await loop.run_in_executor(
            None,
            self._write_if_absent_sync,
            job_state,
            archive_path,
        )

    def _write_if_absent_sync(self, job_state: JobState, archive_path: Path) -> bool:
        if archive_path.exists():
            return True

        archive_path.parent.mkdir(parents=True, exist_ok=True)

        data = msgspec.msgpack.encode(job_state.to_dict())

        temp_fd, temp_path_str = tempfile.mkstemp(
            dir=archive_path.parent,
            prefix=".tmp_",
            suffix=".bin",
        )

        try:
            with os.fdopen(temp_fd, "wb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())

            os.rename(temp_path_str, archive_path)

            dir_fd = os.open(archive_path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

            return True

        except FileExistsError:
            try:
                os.unlink(temp_path_str)
            except OSError:
                pass
            return True

        except Exception:
            try:
                os.unlink(temp_path_str)
            except OSError:
                pass
            raise

    async def read(self, job_id: str) -> JobState | None:
        loop = self._loop
        assert loop is not None

        archive_path = self._get_archive_path(job_id)

        return await loop.run_in_executor(
            None,
            self._read_sync,
            job_id,
            archive_path,
        )

    def _read_sync(self, job_id: str, archive_path: Path) -> JobState | None:
        if not archive_path.exists():
            return None

        try:
            with open(archive_path, "rb") as file:
                data = file.read()

            job_dict = msgspec.msgpack.decode(data)
            return JobState.from_dict(job_id, job_dict)

        except (OSError, msgspec.DecodeError):
            return None

    async def exists(self, job_id: str) -> bool:
        loop = self._loop
        assert loop is not None

        archive_path = self._get_archive_path(job_id)

        return await loop.run_in_executor(
            None,
            archive_path.exists,
        )

    async def delete(self, job_id: str) -> bool:
        loop = self._loop
        assert loop is not None

        archive_path = self._get_archive_path(job_id)

        return await loop.run_in_executor(
            None,
            self._delete_sync,
            archive_path,
        )

    def _delete_sync(self, archive_path: Path) -> bool:
        if not archive_path.exists():
            return False

        try:
            archive_path.unlink()
            return True
        except OSError:
            return False

    async def cleanup_older_than(self, max_age_ms: int, current_time_ms: int) -> int:
        loop = self._loop
        assert loop is not None

        return await loop.run_in_executor(
            None,
            self._cleanup_older_than_sync,
            max_age_ms,
            current_time_ms,
        )

    def _cleanup_older_than_sync(self, max_age_ms: int, current_time_ms: int) -> int:
        removed_count = 0

        if not self._archive_dir.exists():
            return removed_count

        for region_dir in self._archive_dir.iterdir():
            if not region_dir.is_dir():
                continue

            for shard_dir in region_dir.iterdir():
                if not shard_dir.is_dir():
                    continue

                try:
                    shard_timestamp = int(shard_dir.name) * 1000
                    if current_time_ms - shard_timestamp > max_age_ms:
                        for archive_file in shard_dir.iterdir():
                            try:
                                archive_file.unlink()
                                removed_count += 1
                            except OSError:
                                pass

                        try:
                            shard_dir.rmdir()
                        except OSError:
                            pass

                except ValueError:
                    continue

        return removed_count

    @property
    def archive_dir(self) -> Path:
        return self._archive_dir
