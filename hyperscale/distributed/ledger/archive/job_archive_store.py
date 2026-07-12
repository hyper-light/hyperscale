from __future__ import annotations

from pathlib import Path

import msgspec

from hyperscale.distributed.runtime import Filesystem, RealFilesystem

from ..job_state import JobState

# Module-level storage seam (Phase 7). The store BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so archive
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


class JobArchiveStore:
    __slots__ = ("_archive_dir", "_filesystem")

    def __init__(
        self,
        archive_dir: Path,
        filesystem: Filesystem | None = None,
    ) -> None:
        self._archive_dir = archive_dir
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )

    async def initialize(self) -> None:
        await self._filesystem.mkdir(
            self._archive_dir, parents=True, exist_ok=True
        )

    def _get_archive_path(self, job_id: str) -> Path:
        parts = job_id.split("-")
        if len(parts) >= 2:
            region = parts[0]
            timestamp_ms = parts[1]
            shard = timestamp_ms[:10] if len(timestamp_ms) >= 10 else timestamp_ms
            return self._archive_dir / region / shard / f"{job_id}.bin"

        return self._archive_dir / "unknown" / f"{job_id}.bin"

    async def write_if_absent(self, job_state: JobState) -> bool:
        archive_path = self._get_archive_path(job_state.job_id)

        if await self._filesystem.exists(archive_path):
            return True

        await self._filesystem.mkdir(
            archive_path.parent, parents=True, exist_ok=True
        )

        data = msgspec.msgpack.encode(job_state.to_dict())

        # The full crash-consistency sequence (temp file, flush, fsync,
        # atomic rename, parent-directory fsync) this class previously
        # hand-rolled now lives behind the seam as one operation.
        # write-if-absent stays idempotent: a concurrent writer landing
        # first is indistinguishable from us landing first — both leave
        # the same complete record.
        await self._filesystem.atomic_write(archive_path, data)
        return True

    async def read(self, job_id: str) -> JobState | None:
        archive_path = self._get_archive_path(job_id)

        if not await self._filesystem.exists(archive_path):
            return None

        try:
            data = await self._filesystem.read_bytes(archive_path)
            job_dict = msgspec.msgpack.decode(data)
            return JobState.from_dict(job_id, job_dict)

        except (OSError, msgspec.DecodeError):
            return None

    async def exists(self, job_id: str) -> bool:
        return await self._filesystem.exists(self._get_archive_path(job_id))

    async def delete(self, job_id: str) -> bool:
        archive_path = self._get_archive_path(job_id)

        if not await self._filesystem.exists(archive_path):
            return False

        try:
            await self._filesystem.remove(archive_path)
            return True
        except OSError:
            return False

    async def cleanup_older_than(
        self, max_age_ms: int, current_time_ms: int
    ) -> int:
        removed_count = 0

        if not await self._filesystem.exists(self._archive_dir):
            return removed_count

        for region_dir in await self._filesystem.list_subdirectories(
            self._archive_dir
        ):
            for shard_dir in await self._filesystem.list_subdirectories(
                region_dir
            ):
                try:
                    shard_timestamp = int(shard_dir.name) * 1000
                except ValueError:
                    continue

                if current_time_ms - shard_timestamp <= max_age_ms:
                    continue

                for archive_file in await self._filesystem.list_directory(
                    shard_dir, "*"
                ):
                    try:
                        await self._filesystem.remove(archive_file)
                        removed_count += 1
                    except OSError:
                        pass

                try:
                    await self._filesystem.remove_directory(shard_dir)
                except OSError:
                    pass

        return removed_count

    @property
    def archive_dir(self) -> Path:
        return self._archive_dir
