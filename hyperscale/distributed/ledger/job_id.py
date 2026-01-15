from __future__ import annotations

import asyncio
import time


class JobIdGenerator:
    """
    Generates globally unique job IDs with region encoding.

    Format: {region_code}-{timestamp_ms}-{gate_id}-{sequence}
    Example: use1-1704931200000-gate42-00001

    Properties:
    - Lexicographically sortable by time
    - Instant routing to authoritative region
    - No coordination needed for ID generation
    """

    __slots__ = ("_region_code", "_gate_id", "_sequence", "_last_ms", "_lock")

    def __init__(self, region_code: str, gate_id: str) -> None:
        self._region_code = region_code
        self._gate_id = gate_id
        self._sequence = 0
        self._last_ms = 0
        self._lock = asyncio.Lock()

    async def generate(self) -> str:
        async with self._lock:
            current_ms = int(time.time() * 1000)

            if current_ms == self._last_ms:
                self._sequence += 1
            else:
                self._last_ms = current_ms
                self._sequence = 0

            return (
                f"{self._region_code}-{current_ms}-{self._gate_id}-{self._sequence:05d}"
            )

    @staticmethod
    def extract_region(job_id: str) -> str:
        return job_id.split("-")[0]

    @staticmethod
    def extract_timestamp_ms(job_id: str) -> int:
        return int(job_id.split("-")[1])

    @staticmethod
    def extract_gate_id(job_id: str) -> str:
        return job_id.split("-")[2]

    @property
    def region_code(self) -> str:
        return self._region_code

    @property
    def gate_id(self) -> str:
        return self._gate_id
