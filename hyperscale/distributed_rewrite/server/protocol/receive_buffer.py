from __future__ import annotations
import re

blank_line_regex = re.compile(b"\n\r?\n", re.MULTILINE)


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

    def read_all(self) -> bytearray:
        data = bytes(self.buffer)
        self.buffer.clear()

        self._next_line_search = 0
        self._multiple_lines_search = 0

        return data

    def maybe_extract_at_most(self, count: int) -> bytearray | None:
        """
        Extract a fixed number of bytes from the buffer.
        """
        out = self.buffer[:count]
        if not out:
            return None

        out = self.buffer[:count]
        del self.buffer[:count]

        self._next_line_search = 0
        self._multiple_lines_search = 0

        return out

    def maybe_extract_next_line(self) -> bytearray | None:
        """
        Extract the first line, if it is completed in the buffer.
        """
        # Only search in buffer space that we've not already looked at.
        search_start_index = max(0, self._next_line_search - 1)
        partial_idx = self.buffer.find(b"\r\n", search_start_index)

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

    def maybe_extract_lines(self) -> list[bytearray] | None:
        """
        Extract everything up to the first blank line, and return a list of lines.
        """
        # Handle the case where we have an immediate empty line.
        if self.buffer[:1] == b"\n":
            out = self.buffer[:1]
            del self.buffer[:1]

            self._next_line_search = 0
            self._multiple_lines_search = 0

            return []

        if self.buffer[:2] == b"\r\n":
            out = self.buffer[:2]
            del self.buffer[:2]

            self._next_line_search = 0
            self._multiple_lines_search = 0

            return []

        # Only search in buffer space that we've not already looked at.
        match = blank_line_regex.search(self.buffer, self._multiple_lines_search)
        if match is None:
            self._multiple_lines_search = max(0, len(self.buffer) - 2)
            return None

        # Truncate the buffer and return it.
        idx = match.span(0)[-1]
        out = self.buffer[:idx]
        del self.buffer[:idx]

        self._next_line_search = 0
        self._multiple_lines_search = 0

        lines = out.split(b"\n")

        for line in lines:
            if line.endswith(b"\r"):
                del line[-1]

        assert lines[-2] == lines[-1] == b""

        del lines[-2:]

        return lines

    # In theory we should wait until `\r\n` before starting to validate
    # incoming data. However it's interesting to detect (very) invalid data
    # early given they might not even contain `\r\n` at all (hence only
    # timeout will get rid of them).
    # This is not a 100% effective detection but more of a cheap sanity check
    # allowing for early abort in some useful cases.
    # This is especially interesting when peer is messing up with HTTPS and
    # sent us a TLS stream where we were expecting plain HTTP given all
    # versions of TLS so far start handshake with a 0x16 message type code.
    def is_next_line_obviously_invalid_request_line(self) -> bool:
        try:
            # HTTP header line must not contain non-printable characters
            # and should not start with a space
            return self.buffer[0] < 0x21
        except IndexError:
            return False

    def clear(self):
        self.buffer.clear()
        self._next_line_search = 0
        self._multiple_lines_search = 0