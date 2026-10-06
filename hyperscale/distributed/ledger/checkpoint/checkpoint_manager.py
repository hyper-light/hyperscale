"""``CheckpointManager`` -- pickled under the namespace
``hyperscale.distributed.ledger.checkpoint.checkpoint`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
import asyncio
import struct
import zlib
from pathlib import Path
import msgspec
from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.logging.hyperscale_logging_models import CheckpointRetentionError
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    set_aside_unrecognized,
)

from .checkpoint_model import Checkpoint

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

    async def _newest_first(self) -> list[Path]:
        """Checkpoint files, newest first: by the LSN each holds through
        (monotonic within its WAL), then by creation time -- never by
        name, which orders LSNs as text ("99" after "250"), nor by wall
        time alone, which can step backwards. A name that does not carry
        its LSN (written before names did) is older than any that does."""
        def recency(checkpoint_file: Path) -> tuple[int, int]:
            name_parts = Path(checkpoint_file).stem.split("_")
            created_at_ms = int(name_parts[1]) if len(name_parts) >= 2 and name_parts[1].isdigit() else -1
            local_lsn = int(name_parts[2]) if len(name_parts) == 3 and name_parts[2].isdigit() else -1
            return (local_lsn, created_at_ms)

        return sorted(
            await self._filesystem.list_directory(self._checkpoint_dir, "checkpoint_*.bin"),
            key=recency,
            reverse=True,
        )

    async def _load_latest(self) -> None:
        checkpoint_files = await self._newest_first()

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
        # The name carries the LSN the checkpoint holds through, so the
        # WAL can be cut without decoding every retained checkpoint.
        final_path = (
            self._checkpoint_dir
            / f"checkpoint_{checkpoint.created_at_ms}_{checkpoint.local_lsn}.bin"
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
            if self._latest_checkpoint is None or (checkpoint.local_lsn, checkpoint.created_at_ms) > (
                self._latest_checkpoint.local_lsn,
                self._latest_checkpoint.created_at_ms,
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
        checkpoint_files = await self._newest_first()

        removed_count = 0
        for checkpoint_file in checkpoint_files[keep_count:]:
            try:
                await self._filesystem.remove(checkpoint_file)
                removed_count += 1
            except OSError as removal_error:
                await self._log_retention_error(checkpoint_file, removal_error)

        return removed_count

    async def lowest_retained_local_lsn(self) -> int:
        """The lowest LSN any checkpoint on disk holds through: the WAL
        must keep every frame after it, so that each one -- the newest's
        fallbacks too, and any a failed delete left behind -- can still
        replay from where it ends. A checkpoint whose name does not say
        (written before names carried it), or none at all, keeps the
        whole WAL: -1."""
        lowest_local_lsn: int | None = None
        for checkpoint_file in await self._filesystem.list_directory(
            self._checkpoint_dir, "checkpoint_*.bin"
        ):
            name_parts = Path(checkpoint_file).stem.split("_")
            if len(name_parts) != 3 or not name_parts[2].lstrip("-").isdigit():
                return -1
            local_lsn = int(name_parts[2])
            lowest_local_lsn = local_lsn if lowest_local_lsn is None else min(lowest_local_lsn, local_lsn)
        return -1 if lowest_local_lsn is None else lowest_local_lsn

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
