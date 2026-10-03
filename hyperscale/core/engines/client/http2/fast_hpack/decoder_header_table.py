from collections import deque
from typing import Deque, Tuple

from .table import HeaderTable

# RFC 7541 4.1: an entry's size is its name and value octets plus 32.
ENTRY_OVERHEAD = 32

DecodedText = str | bytes
DecodedHeader = Tuple[DecodedText, DecodedText]

STATIC_ENTRIES: Tuple[DecodedHeader, ...] = tuple(
    (str(name, "utf-8"), str(value, "utf-8")) for name, value in HeaderTable.STATIC_TABLE
)
STATIC_LENGTH = len(STATIC_ENTRIES)


class DecoderHeaderTable:
    """
    The decoder's HPACK header table (RFC 7541 2.3): the static table, then
    the dynamic table newest entry first. Entries hold the decoded name and
    value, so an indexed header is a lookup; their sizes in octets are kept
    alongside for eviction (RFC 7541 4.4).

    A name or value that is not UTF-8 is kept as bytes, so the table stays
    in step with the encoder's; ``undecodable_entry_count`` counts those
    entries so the decoder only looks for them when one is present.
    """

    __slots__ = (
        "_maxsize",
        "_current_size",
        "_entries",
        "_entry_sizes",
        "undecodable_entry_count",
    )

    def __init__(self) -> None:
        self._maxsize = HeaderTable.DEFAULT_SIZE
        self._current_size = 0
        self._entries: Deque[DecodedHeader] = deque()
        self._entry_sizes: Deque[int] = deque()
        self.undecodable_entry_count = 0

    @property
    def entries(self) -> Deque[DecodedHeader]:
        """The dynamic table, newest entry first. Read only."""
        return self._entries

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @maxsize.setter
    def maxsize(self, new_maxsize: int) -> None:
        self._maxsize = int(new_maxsize)
        self._evict_to_fit()

    def get_by_index(self, index: int) -> DecodedHeader:
        if 0 < index <= STATIC_LENGTH:
            return STATIC_ENTRIES[index - 1]

        dynamic_index = index - STATIC_LENGTH - 1
        if 0 <= dynamic_index < len(self._entries):
            return self._entries[dynamic_index]

        raise Exception(f"Invalid table index {index}")

    def add(self, header: DecodedHeader, size: int) -> None:
        # RFC 7541 4.4: an entry larger than the table empties it.
        if size > self._maxsize:
            self._clear()
            return

        self._entries.appendleft(header)
        self._entry_sizes.appendleft(size)
        self._current_size += size

        if not (type(header[0]) is str and type(header[1]) is str):
            self.undecodable_entry_count += 1

        self._evict_to_fit()

    def _evict_to_fit(self) -> None:
        while self._current_size > self._maxsize and self._entries:
            name, value = self._entries.pop()
            self._current_size -= self._entry_sizes.pop()

            if not (type(name) is str and type(value) is str):
                self.undecodable_entry_count -= 1

    def _clear(self) -> None:
        self._entries.clear()
        self._entry_sizes.clear()
        self._current_size = 0
        self.undecodable_entry_count = 0
