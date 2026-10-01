from __future__ import annotations

import asyncio
import struct
import zlib
from pathlib import Path
from typing import Any, TYPE_CHECKING

import msgspec

from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.logging.hyperscale_logging_models import CheckpointRetentionError
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    set_aside_unrecognized,
)
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

if TYPE_CHECKING:
    from hyperscale.logging import Logger

# Version 2: HLC fields are AD-39 hybrid logical clock timestamps.
CHECKPOINT_FORMAT = StorageFormat(b"HSCL", 2)
# Format header, then payload length and CRC32.
CHECKPOINT_HEADER_SIZE = CHECKPOINT_FORMAT.header_size + 8

# Module-level storage seam (Phase 7). The manager BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so checkpoint
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


class Checkpoint(msgspec.Struct, frozen=True):
    local_lsn: int
    regional_lsn: int
    global_lsn: int
    hlc: HLCTimestamp
    job_states: dict[str, dict[str, Any]]
    created_at_ms: int
    # The ledger's next fence token. Compaction drops the JOB_CREATED
    # entries replay used to advance it from, so without this a restart
    # re-issued fence tokens already held by live jobs. 0 = written
    # before this field existed (recovery then seeds from job states).
    next_fence_token: int = 0


class CheckpointManager:
    __slots__ = (
        "_checkpoint_dir",
        "_lock",
        "_latest_checkpoint",
        "_filesystem",
        "_logger",
    )

    def __init__(
        self,
        checkpoint_dir: Path,
        filesystem: Filesystem | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._checkpoint_dir = checkpoint_dir
        self._lock = asyncio.Lock()
        self._latest_checkpoint: Checkpoint | None = None
        self._logger = logger
        # Borrowed, never shut down here — see _DEFAULT_FILESYSTEM.
        self._filesystem = (
            filesystem if filesystem is not None else _DEFAULT_FILESYSTEM
        )

    async def initialize(self) -> None:
        await self._filesystem.mkdir(
            self._checkpoint_dir, parents=True, exist_ok=True
        )
        await self._load_latest()

    async def _load_latest(self) -> None:
        checkpoint_files = sorted(
            await self._filesystem.list_directory(
                self._checkpoint_dir, "checkpoint_*.bin"
            ),
            reverse=True,
        )

        # Newest first; a checkpoint that cannot be used is set aside,
        # loudly, and the next older one is tried.
        for checkpoint_file in checkpoint_files:
            data = await self._filesystem.read_bytes(checkpoint_file)
            try:
                self._latest_checkpoint = self._decode_checkpoint(data)
                return
            except UnrecognizedStorageFormatError as format_error:
                reason = format_error.reason
            except (ValueError, msgspec.DecodeError) as corruption:
                reason = f"damaged checkpoint: {corruption}"
            await self._set_aside(checkpoint_file, data, reason)

    async def _set_aside(self, path: Path, data: bytes, reason: str) -> None:
        """Preserve an unreadable checkpoint's bytes and free its path --
        or, with no logger to report it through, refuse outright."""
        if self._logger is None:
            raise UnrecognizedStorageFormatError(reason)
        await set_aside_unrecognized(self._filesystem, path, data, reason, self._logger)

    async def _read_checkpoint(self, path: Path) -> Checkpoint:
        data = await self._filesystem.read_bytes(path)
        return self._decode_checkpoint(data)

    @staticmethod
    def _decode_checkpoint(data: bytes) -> Checkpoint:
        CHECKPOINT_FORMAT.validate(data)
        if len(data) < CHECKPOINT_HEADER_SIZE:
            raise ValueError("Checkpoint file too small")

        format_end = CHECKPOINT_FORMAT.header_size
        data_length = struct.unpack(">I", data[format_end : format_end + 4])[0]
        stored_crc = struct.unpack(">I", data[format_end + 4 : format_end + 8])[0]

        payload = data[CHECKPOINT_HEADER_SIZE : CHECKPOINT_HEADER_SIZE + data_length]
        if len(payload) < data_length:
            raise ValueError("Checkpoint file truncated")

        computed_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != computed_crc:
            raise ValueError("Checkpoint CRC mismatch")

        return msgspec.msgpack.decode(payload, type=Checkpoint)

    async def save(self, checkpoint: Checkpoint) -> Path:
        final_path = (
            self._checkpoint_dir / f"checkpoint_{checkpoint.created_at_ms}.bin"
        )

        payload = msgspec.msgpack.encode(checkpoint)
        crc = zlib.crc32(payload) & 0xFFFFFFFF

        header = (
            CHECKPOINT_FORMAT.header
            + struct.pack(">I", len(payload))
            + struct.pack(">I", crc)
        )

        # The full crash-consistency sequence (temp file, flush, fsync,
        # atomic rename, parent-directory fsync) that this class
        # previously hand-rolled now lives behind the seam as one
        # operation — same bytes, same durability, storage-faultable
        # under SIM.
        await self._filesystem.atomic_write(final_path, header + payload)

        async with self._lock:
            if (
                self._latest_checkpoint is None
                or checkpoint.created_at_ms > self._latest_checkpoint.created_at_ms
            ):
                self._latest_checkpoint = checkpoint

        return final_path

    async def cleanup(self, keep_count: int = 3) -> int:
        """Prune all but the newest ``keep_count`` checkpoints.

        Retention is above one on purpose: ``_load_latest`` walks the
        files newest-first and skips any that fail to decode, so the
        older copies are the recovery fallback for a checkpoint torn by
        a crash mid-write. Pruning to one would make the newest file a
        single point of failure for the whole ledger's recovery.

        A delete that fails leaves a stale file behind and is retried by
        the next cleanup, so it does not abort the remaining deletes --
        but it IS logged, because silently failing deletes are how a
        checkpoint directory grows without bound while every metric
        says retention is working.
        """
        checkpoint_files = sorted(
            await self._filesystem.list_directory(
                self._checkpoint_dir, "checkpoint_*.bin"
            ),
            reverse=True,
        )

        removed_count = 0
        for checkpoint_file in checkpoint_files[keep_count:]:
            try:
                await self._filesystem.remove(checkpoint_file)
                removed_count += 1
            except OSError as removal_error:
                await self._log_retention_error(checkpoint_file, removal_error)

        return removed_count

    async def _log_retention_error(
        self, checkpoint_file: Path, removal_error: OSError
    ) -> None:
        if self._logger is not None:
            await self._logger.log(
                CheckpointRetentionError(
                    message=(
                        f"stale checkpoint {checkpoint_file.name} not "
                        f"removed ({type(removal_error).__name__}); it stays "
                        "on disk and the next cleanup retries it"
                    ),
                    path=str(checkpoint_file),
                    error_type=type(removal_error).__name__,
                )
            )

    @property
    def checkpoint_dir(self) -> Path:
        return self._checkpoint_dir

    @property
    def latest(self) -> Checkpoint | None:
        return self._latest_checkpoint

    @property
    def has_checkpoint(self) -> bool:
        return self._latest_checkpoint is not None
