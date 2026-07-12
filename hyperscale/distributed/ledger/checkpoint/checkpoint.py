from __future__ import annotations

import asyncio
import struct
import zlib
from pathlib import Path
from typing import Any

import msgspec

from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.logging.lsn import LSN

CHECKPOINT_MAGIC = b"HSCL"
CHECKPOINT_VERSION = 1
CHECKPOINT_HEADER_SIZE = 16

# Module-level storage seam (Phase 7). The manager BORROWS this (or an
# injected instance) — it never shuts the filesystem down.
# ``swap_defaults`` rebinds it to the SIM filesystem so checkpoint
# persistence becomes deterministic and storage-faultable under replay.
_DEFAULT_FILESYSTEM: Filesystem = RealFilesystem()


class Checkpoint(msgspec.Struct, frozen=True):
    local_lsn: int
    regional_lsn: int
    global_lsn: int
    hlc: LSN
    job_states: dict[str, dict[str, Any]]
    created_at_ms: int


class CheckpointManager:
    __slots__ = ("_checkpoint_dir", "_lock", "_latest_checkpoint", "_filesystem")

    def __init__(
        self,
        checkpoint_dir: Path,
        filesystem: Filesystem | None = None,
    ) -> None:
        self._checkpoint_dir = checkpoint_dir
        self._lock = asyncio.Lock()
        self._latest_checkpoint: Checkpoint | None = None
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

        for checkpoint_file in checkpoint_files:
            try:
                checkpoint = await self._read_checkpoint(checkpoint_file)
                self._latest_checkpoint = checkpoint
                return
            except (ValueError, OSError):
                continue

    async def _read_checkpoint(self, path: Path) -> Checkpoint:
        data = await self._filesystem.read_bytes(path)
        return self._decode_checkpoint(data)

    @staticmethod
    def _decode_checkpoint(data: bytes) -> Checkpoint:
        if len(data) < CHECKPOINT_HEADER_SIZE:
            raise ValueError("Checkpoint file too small")

        magic = data[:4]
        if magic != CHECKPOINT_MAGIC:
            raise ValueError(f"Invalid checkpoint magic: {magic}")

        version = struct.unpack(">I", data[4:8])[0]
        if version != CHECKPOINT_VERSION:
            raise ValueError(f"Unsupported checkpoint version: {version}")

        data_length = struct.unpack(">I", data[8:12])[0]
        stored_crc = struct.unpack(">I", data[12:16])[0]

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
            CHECKPOINT_MAGIC
            + struct.pack(">I", CHECKPOINT_VERSION)
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
            except OSError:
                pass

        return removed_count

    @property
    def latest(self) -> Checkpoint | None:
        return self._latest_checkpoint

    @property
    def has_checkpoint(self) -> bool:
        return self._latest_checkpoint is not None
