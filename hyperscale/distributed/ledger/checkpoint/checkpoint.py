from __future__ import annotations

import asyncio
import struct
import zlib
from pathlib import Path
from typing import Any

import aiofiles
import msgspec

from hyperscale.logging.lsn import LSN


class Checkpoint(msgspec.Struct, frozen=True):
    """
    Snapshot of ledger state at a point in time.

    Enables efficient recovery without replaying entire WAL.
    """

    local_lsn: int
    regional_lsn: int
    global_lsn: int
    hlc: LSN
    job_states: dict[str, dict[str, Any]]
    created_at_ms: int


CHECKPOINT_MAGIC = b"HSCL"
CHECKPOINT_VERSION = 1


class CheckpointManager:
    __slots__ = ("_checkpoint_dir", "_lock", "_latest_checkpoint")

    def __init__(self, checkpoint_dir: Path) -> None:
        self._checkpoint_dir = checkpoint_dir
        self._lock = asyncio.Lock()
        self._latest_checkpoint: Checkpoint | None = None

    async def initialize(self) -> None:
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
        await self._load_latest()

    async def _load_latest(self) -> None:
        checkpoint_files = sorted(
            self._checkpoint_dir.glob("checkpoint_*.bin"),
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
        async with aiofiles.open(path, mode="rb") as file:
            header = await file.read(8)

            if len(header) < 8:
                raise ValueError("Checkpoint file too small")

            magic = header[:4]
            if magic != CHECKPOINT_MAGIC:
                raise ValueError(f"Invalid checkpoint magic: {magic}")

            version = struct.unpack(">I", header[4:8])[0]
            if version != CHECKPOINT_VERSION:
                raise ValueError(f"Unsupported checkpoint version: {version}")

            length_bytes = await file.read(4)
            data_length = struct.unpack(">I", length_bytes)[0]

            crc_bytes = await file.read(4)
            stored_crc = struct.unpack(">I", crc_bytes)[0]

            data = await file.read(data_length)
            computed_crc = zlib.crc32(data) & 0xFFFFFFFF

            if stored_crc != computed_crc:
                raise ValueError("Checkpoint CRC mismatch")

            return msgspec.msgpack.decode(data, type=Checkpoint)

    async def save(self, checkpoint: Checkpoint) -> Path:
        async with self._lock:
            filename = f"checkpoint_{checkpoint.created_at_ms}.bin"
            path = self._checkpoint_dir / filename

            data = msgspec.msgpack.encode(checkpoint)
            crc = zlib.crc32(data) & 0xFFFFFFFF

            header = CHECKPOINT_MAGIC + struct.pack(">I", CHECKPOINT_VERSION)
            length_bytes = struct.pack(">I", len(data))
            crc_bytes = struct.pack(">I", crc)

            async with aiofiles.open(path, mode="wb") as file:
                await file.write(header)
                await file.write(length_bytes)
                await file.write(crc_bytes)
                await file.write(data)

            self._latest_checkpoint = checkpoint
            return path

    async def cleanup(self, keep_count: int = 3) -> int:
        async with self._lock:
            checkpoint_files = sorted(
                self._checkpoint_dir.glob("checkpoint_*.bin"),
                reverse=True,
            )

            removed_count = 0
            for checkpoint_file in checkpoint_files[keep_count:]:
                try:
                    checkpoint_file.unlink()
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
