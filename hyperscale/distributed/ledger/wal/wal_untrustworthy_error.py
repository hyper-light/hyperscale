from __future__ import annotations

from pathlib import Path


class WALUntrustworthyError(Exception):
    """A job WAL holds damage that a power loss cannot explain: a damaged
    entry with written bytes, or a whole entry, after it. Entries past the
    damage may have been acknowledged, so the node refuses to start rather
    than resume without them; the file is left exactly as found for an
    operator to restore or move aside (AD-38 Part 3.2)."""

    def __init__(self, path: Path, damage_offset: int, bytes_after_damage: int) -> None:
        self.path = path
        self.damage_offset = damage_offset
        self.bytes_after_damage = bytes_after_damage
        super().__init__(
            f"WAL {path}: the entry at byte {damage_offset} is damaged with {bytes_after_damage} "
            "written bytes after it, which may hold acknowledged entries; refusing to start. "
            "Restore the WAL from a backup, or move it aside to start without its local history "
            "(AD-38 Part 3.2)"
        )
