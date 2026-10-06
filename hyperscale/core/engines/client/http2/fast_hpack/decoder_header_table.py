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

# The static names' lengths in octets: a literal that indexes one adds an
# entry whose size counts them.
STATIC_NAME_LENGTHS: Tuple[int, ...] = tuple(len(name) for name, _ in HeaderTable.STATIC_TABLE)


class DecoderHeaderTable:
    """
    The decoder's dynamic table (RFC 7541 2.3.2), newest entry first. Entries
    hold the decoded name and value, so an indexed header is a lookup; their
    sizes in octets are kept alongside for eviction (RFC 7541 4.4).

    The decoder adds entries itself, inline in its loop, keeping
    ``current_size`` within ``maximum_size``; a table size update goes
    through ``maxsize``.

    A name or value that is not UTF-8 is kept as bytes, so the table stays
    in step with the encoder's; ``undecodable_entry_count`` counts those
    entries so the decoder only looks for them when one is present.
    """

    __slots__ = (
        "maximum_size",
        "current_size",
        "entries",
        "entry_sizes",
        "undecodable_entry_count",
        "holds_malformed_entry",
    )

    def __init__(self) -> None:
        self.maximum_size = HeaderTable.DEFAULT_SIZE
        self.current_size = 0
        self.entries: Deque[DecodedHeader] = deque()
        self.entry_sizes: Deque[int] = deque()
        self.undecodable_entry_count = 0
        # Whether a literal whose field a response may not hold was added: an
        # indexed field may then be that entry, so the decoder checks every
        # field of its blocks until the table is emptied.
        self.holds_malformed_entry = False

    @property
    def maxsize(self) -> int:
        return self.maximum_size

    @maxsize.setter
    def maxsize(self, new_maxsize: int) -> None:
        self.maximum_size = int(new_maxsize)

        while self.current_size > self.maximum_size and self.entries:
            name, value = self.entries.pop()
            self.current_size -= self.entry_sizes.pop()

            if not (type(name) is str and type(value) is str):
                self.undecodable_entry_count -= 1

    def clear(self) -> None:
        """Empties the table, as adding an entry larger than it does (RFC 7541 4.4)."""
        self.entries.clear()
        self.entry_sizes.clear()
        self.current_size = 0
        self.undecodable_entry_count = 0
        self.holds_malformed_entry = False
