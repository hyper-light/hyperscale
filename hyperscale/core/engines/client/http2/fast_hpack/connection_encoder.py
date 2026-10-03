from collections import deque
from typing import Deque, Dict, Optional, Sequence, Tuple

from .huffman import HuffmanEncoder
from .table import HeaderTable

# RFC 7541 4.1: an entry's size is its name and value octets plus 32.
ENTRY_OVERHEAD = 32

# RFC 7541 6: the leading bits that select a header field representation.
INDEXED_FIELD = 0x80
LITERAL_WITH_INDEXING = 0x40
TABLE_SIZE_UPDATE = 0x20
LITERAL_NEVER_INDEXED = 0x10
# RFC 7541 5.2: the H bit of a string literal.
HUFFMAN_ENCODED = 0x80
# RFC 7541 5.1: each representation's integer prefix, as a mask.
INDEX_PREFIX = 0x7F
NAME_INDEX_WITH_INDEXING_PREFIX = 0x3F
TABLE_SIZE_PREFIX = 0x1F
NAME_INDEX_NEVER_INDEXED_PREFIX = 0x0F
STRING_LENGTH_PREFIX = 0x7F

STATIC_LENGTH = HeaderTable.STATIC_TABLE_LENGTH


def encode_integer(integer: int, prefix_mask: int, representation: int) -> bytes:
    """
    Encodes ``integer`` (RFC 7541 5.1) in the prefix under ``prefix_mask`` of
    a first byte that carries the ``representation`` bits.
    """
    if integer < prefix_mask:
        return bytes((representation | integer,))

    encoded = bytearray((representation | prefix_mask,))
    integer -= prefix_mask

    while integer >= 0x80:
        encoded.append((integer & 0x7F) | 0x80)
        integer >>= 7

    encoded.append(integer)
    return bytes(encoded)


class ConnectionEncoder:
    """
    An HPACK encoder (RFC 7541) for a single HTTP/2 connection.

    Its dynamic table mirrors the one the connection's peer decodes with, so a
    header a previous request already sent -- the pseudo-headers and headers
    a load test repeats on every request -- goes out as an index of a byte or
    two, found with dict lookups instead of a table scan and a Huffman pass.

    A dynamic index only means something to the peer that received the entry,
    so each connection needs its own encoder, discarded with the connection.
    """

    __slots__ = (
        "_huffman_encoder",
        "_maximum_size",
        "_current_size",
        "_entries",
        "_newest_insertion",
        "_insertion_by_header",
        "_insertion_by_name",
        "_smallest_pending_size",
    )

    def __init__(self) -> None:
        self._huffman_encoder = HuffmanEncoder()
        self._maximum_size = HeaderTable.DEFAULT_SIZE
        self._current_size = 0

        # Oldest first: (name, value, size, insertion number).
        self._entries: Deque[Tuple[bytes, bytes, int, int]] = deque()
        self._newest_insertion = 0
        self._insertion_by_header: Dict[Tuple[bytes, bytes], int] = {}
        self._insertion_by_name: Dict[bytes, int] = {}

        # The smallest table size set since the last header block, while a
        # size change waits to be signaled (RFC 7541 4.2).
        self._smallest_pending_size: Optional[int] = None

    @property
    def header_table_size(self) -> int:
        return self._maximum_size

    @header_table_size.setter
    def header_table_size(self, new_size: int) -> None:
        if new_size == self._maximum_size and self._smallest_pending_size is None:
            return

        self._maximum_size = new_size
        if self._smallest_pending_size is None or new_size < self._smallest_pending_size:
            self._smallest_pending_size = new_size

        self._evict_to_fit()

    def encode(self, headers: Sequence[Tuple[bytes, ...]]) -> bytes:
        """
        Encodes ``headers`` -- ``(name, value)`` byte pairs, or
        ``(name, value, sensitive)`` -- into a header block, updating the
        dynamic table as the peer's decoder will.
        """
        header_block = bytearray()

        if self._smallest_pending_size is not None:
            self._encode_size_updates(header_block)

        for header in headers:
            name = header[0]
            value = header[1]

            if len(header) > 2 and header[2]:
                self._encode_never_indexed(header_block, name, value)
                continue

            static_entry = HeaderTable.STATIC_TABLE_MAPPING.get(name)
            if static_entry is not None and (static_index := static_entry[1].get(value)):
                header_block.append(INDEXED_FIELD | static_index)
                continue

            if (insertion := self._insertion_by_header.get((name, value))) is not None:
                header_block += encode_integer(self._dynamic_index(insertion), INDEX_PREFIX, INDEXED_FIELD)
                continue

            self._encode_literal_with_indexing(header_block, name, value, static_entry)

        return bytes(header_block)

    def _encode_literal_with_indexing(
        self,
        header_block: bytearray,
        name: bytes,
        value: bytes,
        static_entry: Optional[Tuple[int, Dict[bytes, int]]],
    ) -> None:
        name_index = self._name_index(name, static_entry)

        if name_index:
            header_block += encode_integer(name_index, NAME_INDEX_WITH_INDEXING_PREFIX, LITERAL_WITH_INDEXING)
        else:
            header_block.append(LITERAL_WITH_INDEXING)
            self._encode_string(header_block, name)

        self._encode_string(header_block, value)
        self._add(name, value)

    def _encode_never_indexed(self, header_block: bytearray, name: bytes, value: bytes) -> None:
        name_index = self._name_index(name, HeaderTable.STATIC_TABLE_MAPPING.get(name))

        if name_index:
            header_block += encode_integer(name_index, NAME_INDEX_NEVER_INDEXED_PREFIX, LITERAL_NEVER_INDEXED)
        else:
            header_block.append(LITERAL_NEVER_INDEXED)
            self._encode_string(header_block, name)

        self._encode_string(header_block, value)

    def _name_index(self, name: bytes, static_entry: Optional[Tuple[int, Dict[bytes, int]]]) -> int:
        if static_entry is not None:
            return static_entry[0]

        if (insertion := self._insertion_by_name.get(name)) is not None:
            return self._dynamic_index(insertion)

        return 0

    def _encode_string(self, header_block: bytearray, octets: bytes) -> None:
        encoded = self._huffman_encoder.encode(octets)
        header_block += encode_integer(len(encoded), STRING_LENGTH_PREFIX, HUFFMAN_ENCODED)
        header_block += encoded

    def _encode_size_updates(self, header_block: bytearray) -> None:
        # RFC 7541 4.2: signal the smallest size since the last block, then
        # the final size -- at most two updates, at the start of the block.
        if self._smallest_pending_size < self._maximum_size:
            header_block += encode_integer(self._smallest_pending_size, TABLE_SIZE_PREFIX, TABLE_SIZE_UPDATE)

        header_block += encode_integer(self._maximum_size, TABLE_SIZE_PREFIX, TABLE_SIZE_UPDATE)
        self._smallest_pending_size = None

    def _dynamic_index(self, insertion: int) -> int:
        # The newest entry is index STATIC_LENGTH + 1 (RFC 7541 2.3.3).
        return STATIC_LENGTH + 1 + self._newest_insertion - insertion

    def _add(self, name: bytes, value: bytes) -> None:
        size = ENTRY_OVERHEAD + len(name) + len(value)

        # RFC 7541 4.4: an entry larger than the table empties it.
        if size > self._maximum_size:
            self._entries.clear()
            self._insertion_by_header.clear()
            self._insertion_by_name.clear()
            self._current_size = 0
            return

        self._newest_insertion += 1
        self._entries.append((name, value, size, self._newest_insertion))
        self._insertion_by_header[(name, value)] = self._newest_insertion
        self._insertion_by_name[name] = self._newest_insertion
        self._current_size += size

        self._evict_to_fit()

    def _evict_to_fit(self) -> None:
        while self._current_size > self._maximum_size and self._entries:
            name, value, size, insertion = self._entries.popleft()
            self._current_size -= size

            if self._insertion_by_header.get((name, value)) == insertion:
                del self._insertion_by_header[(name, value)]

            if self._insertion_by_name.get(name) == insertion:
                del self._insertion_by_name[name]
