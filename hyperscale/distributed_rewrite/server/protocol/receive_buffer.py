from __future__ import annotations


class ReceiveBuffer:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self._next_line_search = 0
        self._multiple_lines_search = 0

    def __iadd__(self, byteslike: bytes | bytearray) -> "ReceiveBuffer":
        self.buffer += byteslike
        return self

    def __bool__(self) -> bool:
        return bool(len(self))

    def __len__(self) -> int:
        return len(self.buffer)

    # for @property unprocessed_data
    def __bytes__(self) -> bytes:
        return bytes(self.buffer)

    def _extract(self, count: int) -> bytearray:
        # extracting an initial slice of the data buffer and return it
        out = self.buffer[:count]
        del self.buffer[:count]

        self._next_line_search = 0
        self._multiple_lines_search = 0

        return out

    def maybe_extract_next(self) -> bytearray | None:
        """
        Extract the first line, if it is completed in the buffer.
        """
        # Only search in buffer space that we've not already looked at.
        search_start_index = max(0, self._next_line_search - 1)
        partial_idx = self.buffer.find(b"<END", search_start_index)

        if partial_idx == -1:
            self._next_line_search = len(self.buffer)
            return None

        # + 2 is to compensate len(b"\r\n")
        idx = partial_idx + 2

        out = self.buffer[:idx]
        del self.buffer[:idx]

        self._next_line_search = 0
        self._multiple_lines_search = 0

        return out
    
    def clear(self):
        self.buffer.clear()
        self._next_line_search = 0
        self._multiple_lines_search = 0