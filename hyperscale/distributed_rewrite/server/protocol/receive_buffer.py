from __future__ import annotations

# Length prefix size (4 bytes = 32-bit unsigned integer, supports up to ~4GB messages)
LENGTH_PREFIX_SIZE = 4


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

    def maybe_extract_framed(self) -> bytes | None:
        """
        Extract a length-prefixed message from the buffer.
        
        Message format: [4-byte length prefix (big-endian)] + [payload]
        
        Returns the payload (without length prefix) if complete message is available,
        otherwise returns None.
        """
        # Need at least the length prefix to know message size
        if len(self.buffer) < LENGTH_PREFIX_SIZE:
            return None
        
        # Read the length prefix (4 bytes, big-endian unsigned int)
        message_length = int.from_bytes(self.buffer[:LENGTH_PREFIX_SIZE], 'big')
        
        # Check if we have the complete message
        total_length = LENGTH_PREFIX_SIZE + message_length
        if len(self.buffer) < total_length:
            return None
        
        # Extract the complete message (skip the length prefix)
        self._extract(LENGTH_PREFIX_SIZE)  # Remove length prefix
        payload = bytes(self._extract(message_length))  # Extract payload
        
        return payload
    
    def clear(self):
        self.buffer.clear()
        self._next_line_search = 0
        self._multiple_lines_search = 0


def frame_message(data: bytes) -> bytes:
    """
    Frame a message with a length prefix for TCP transmission.
    
    Returns: [4-byte length prefix (big-endian)] + [data]
    """
    length_prefix = len(data).to_bytes(LENGTH_PREFIX_SIZE, 'big')
    return length_prefix + data