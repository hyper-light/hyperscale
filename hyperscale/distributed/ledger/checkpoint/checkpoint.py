from __future__ import annotations

import asyncio
import os
import struct
import tempfile
import zlib
from pathlib import Path
from typing import Any

import msgspec

from hyperscale.logging.lsn import LSN

CHECKPOINT_MAGIC = b"HSCL"
CHECKPOINT_VERSION = 1
CHECKPOINT_HEADER_SIZE = 16


class Checkpoint(msgspec.Struct, frozen=True):
    local_lsn: int
    regional_lsn: int
    global_lsn: int
    hlc: LSN
    job_states: dict[str, dict[str, Any]]
    created_at_ms: int


class CheckpointManager:
    __slots__ = ("_checkpoint_dir", "_lock", "_latest_checkpoint", "_loop")

    def __init__(self, checkpoint_dir: Path) -> None:
        self._checkpoint_dir = checkpoint_dir
        self._lock = asyncio.Lock()
        self._latest_checkpoint: Checkpoint | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def initialize(self) -> None:
        self._loop = asyncio.get_running_loop()

        await self._loop.run_in_executor(
            None,
            self._initialize_sync,
        )

        await self._load_latest()

    def _initialize_sync(self) -> None:
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)

    async def _load_latest(self) -> None:
        loop = self._loop
        assert loop is not None

        checkpoint_files = await loop.run_in_executor(
            None,
            self._list_checkpoint_files_sync,
        )

        for checkpoint_file in checkpoint_files:
            try:
                checkpoint = await self._read_checkpoint(checkpoint_file)
                self._latest_checkpoint = checkpoint
                return
            except (ValueError, OSError):
                continue

    def _list_checkpoint_files_sync(self) -> list[Path]:
        return sorted(
            self._checkpoint_dir.glob("checkpoint_*.bin"),
            reverse=True,
        )

    async def _read_checkpoint(self, path: Path) -> Checkpoint:
        loop = self._loop
        assert loop is not None

        return await loop.run_in_executor(
            None,
            self._read_checkpoint_sync,
            path,
        )

    def _read_checkpoint_sync(self, path: Path) -> Checkpoint:
        with open(path, "rb") as file:
            data = file.read()

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
        loop = self._loop
        assert loop is not None

        path = await loop.run_in_executor(
            None,
            self._save_sync,
            checkpoint,
        )

        async with self._lock:
            if (
                self._latest_checkpoint is None
                or checkpoint.created_at_ms > self._latest_checkpoint.created_at_ms
            ):
                self._latest_checkpoint = checkpoint

        return path

    def _save_sync(self, checkpoint: Checkpoint) -> Path:
        filename = f"checkpoint_{checkpoint.created_at_ms}.bin"
        final_path = self._checkpoint_dir / filename

        payload = msgspec.msgpack.encode(checkpoint)
        crc = zlib.crc32(payload) & 0xFFFFFFFF

        header = (
            CHECKPOINT_MAGIC
            + struct.pack(">I", CHECKPOINT_VERSION)
            + struct.pack(">I", len(payload))
            + struct.pack(">I", crc)
        )

        temp_fd, temp_path_str = tempfile.mkstemp(
            dir=self._checkpoint_dir,
            prefix=".tmp_checkpoint_",
            suffix=".bin",
        )

        try:
            with os.fdopen(temp_fd, "wb") as file:
                file.write(header)
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())

            os.rename(temp_path_str, final_path)

            dir_fd = os.open(self._checkpoint_dir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

            return final_path

        except Exception:
            try:
                os.unlink(temp_path_str)
            except OSError:
                pass
            raise

    async def cleanup(self, keep_count: int = 3) -> int:
        loop = self._loop
        assert loop is not None

        return await loop.run_in_executor(
            None,
            self._cleanup_sync,
            keep_count,
        )

    def _cleanup_sync(self, keep_count: int) -> int:
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
