from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from hyperscale.distributed.runtime import Filesystem, RealFilesystem

from hyperscale.distributed.ledger.job_state import JobState
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    set_aside_unrecognized,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger

# Archived job records (AD-39 HLC components).
ARCHIVE_FORMAT = StorageFormat(b"HSJA", 1)

# Module-level storage seam (Phase 7). The store BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so archive
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


class JobArchiveStore:
    __slots__ = ("_archive_dir", "_filesystem", "_logger")

    def __init__(
        self,
        archive_dir: Path,
        filesystem: Filesystem | None = None,
        logger: "Logger | None" = None,
    ) -> None:
        self._archive_dir = archive_dir
        self._logger = logger
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )

    @property
    def archive_dir(self) -> Path:
        """Root directory of the archive (diagnostic/logging surface)."""
        return self._archive_dir

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

        data = ARCHIVE_FORMAT.encode(msgspec.msgpack.encode(job_state.to_dict()))

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

        data = await self._filesystem.read_bytes(archive_path)
        job_state, reason = self._decode_record(job_id, data)
        if reason is None:
            return job_state
        # Set aside, the path is free: the job reads as unarchived, and
        # the next archival of it (recovery's terminal sweep) lands.
        await self._set_aside(archive_path, data, reason)
        return None

    @staticmethod
    def _decode_record(job_id: str, data: bytes) -> tuple[JobState | None, str | None]:
        """The archived job, or why its record cannot be read."""
        try:
            return JobState.from_dict(job_id, msgspec.msgpack.decode(ARCHIVE_FORMAT.decode(data))), None
        except UnrecognizedStorageFormatError as format_error:
            return None, format_error.reason
        except (msgspec.DecodeError, ValueError, KeyError, TypeError) as corruption:
            return None, f"damaged archive record: {corruption!r}"

    async def _set_aside(self, path: Path, data: bytes, reason: str) -> None:
        """Preserve an unreadable record's bytes and free its path -- or,
        with no logger to report it through, refuse outright."""
        if self._logger is None:
            raise UnrecognizedStorageFormatError(reason)
        await set_aside_unrecognized(self._filesystem, path, data, reason, self._logger)

    async def exists(self, job_id: str) -> bool:
        return await self._filesystem.exists(self._get_archive_path(job_id))

    async def delete(self, job_id: str) -> bool:
        archive_path = self._get_archive_path(job_id)

        if not await self._filesystem.exists(archive_path):
            return False

        # False means there was nothing to delete; a failed removal raises.
        await self._filesystem.remove(archive_path)
        return True

    async def cleanup_older_than(
        self, max_age_ms: int, current_time_ms: int
    ) -> int:
        """Remove archive shards older than ``max_age_ms``; returns how many
        archive files were removed. Every shard is attempted; removals that
        failed then raise together, so a sweep never reports success over
        files it left behind. Directories not named by a shard timestamp
        are not ours and are left alone."""
        removed_count = 0
        removal_errors: list[OSError] = []

        if not await self._filesystem.exists(self._archive_dir):
            return removed_count

        for region_dir in await self._filesystem.list_subdirectories(
            self._archive_dir
        ):
            removed_count += await self._cleanup_region(
                region_dir, max_age_ms, current_time_ms, removal_errors
            )

        self._raise_removal_errors(removed_count, removal_errors)
        return removed_count

    async def _cleanup_region(
        self,
        region_dir: Path,
        max_age_ms: int,
        current_time_ms: int,
        removal_errors: list[OSError],
    ) -> int:
        """Sweep one region's expired shards: how many files went."""
        removed_count = 0
        for shard_dir in await self._filesystem.list_subdirectories(
            region_dir
        ):
            removed_count += await self._cleanup_shard(
                shard_dir, max_age_ms, current_time_ms, removal_errors
            )
        return removed_count

    async def _cleanup_shard(
        self,
        shard_dir: Path,
        max_age_ms: int,
        current_time_ms: int,
        removal_errors: list[OSError],
    ) -> int:
        """Remove an expired shard's files and then the shard itself; a
        directory not named by a shard timestamp is left alone."""
        if not self._shard_expired(shard_dir, max_age_ms, current_time_ms):
            return 0

        removed_count = await self._remove_shard_files(shard_dir, removal_errors)

        try:
            await self._filesystem.remove_directory(shard_dir)
        except OSError as removal_error:
            removal_errors.append(removal_error)
        return removed_count

    @staticmethod
    def _shard_expired(shard_dir: Path, max_age_ms: int, current_time_ms: int) -> bool:
        """Whether a shard directory is named by a timestamp older than
        ``max_age_ms``."""
        try:
            shard_timestamp = int(shard_dir.name) * 1000
        except ValueError:
            return False

        return not current_time_ms - shard_timestamp <= max_age_ms

    async def _remove_shard_files(self, shard_dir: Path, removal_errors: list[OSError]) -> int:
        """Remove every file in a shard, collecting failures: how many went."""
        removed_count = 0
        for archive_file in await self._filesystem.list_directory(
            shard_dir, "*"
        ):
            removed_count += await self._remove_archive_file(archive_file, removal_errors)
        return removed_count

    async def _remove_archive_file(self, archive_file: Path, removal_errors: list[OSError]) -> int:
        """Remove one archive file: 1 when it went, 0 when its failure was
        collected."""
        try:
            await self._filesystem.remove(archive_file)
            return 1
        except OSError as removal_error:
            removal_errors.append(removal_error)
            return 0

    @staticmethod
    def _raise_removal_errors(removed_count: int, removal_errors: list[OSError]) -> None:
        """Raise every collected removal failure together."""
        if removal_errors:
            raise ExceptionGroup(
                f"archive cleanup removed {removed_count} files but failed {len(removal_errors)} removals",
                removal_errors,
            )
