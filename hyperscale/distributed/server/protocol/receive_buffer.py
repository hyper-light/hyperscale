from __future__ import annotations

# Length prefix size (4 bytes = 32-bit unsigned integer, supports up to ~4GB messages)
LENGTH_PREFIX_SIZE = 4

# Security limits - prevent memory exhaustion attacks
# Max frame length: 1MB compressed (aligns with MAX_MESSAGE_SIZE in security.py)
MAX_FRAME_LENGTH = 1 * 1024 * 1024
# Max buffer size: 2MB (allows for some buffering of partial frames)
MAX_BUFFER_SIZE = 2 * 1024 * 1024


class BufferOverflowError(Exception):
    """Raised when buffer size limits are exceeded."""
    pass


class FrameTooLargeError(Exception):
    """Raised when a frame's length prefix exceeds the maximum allowed."""

    def __init__(
        self,
        message: str,
        actual_size: int = 0,
        max_size: int = 0,
    ) -> None:
        super().__init__(message)
        self.actual_size = actual_size
        self.max_size = max_size

    def to_error_response(self) -> bytes:
        """
        Generate structured error response for protocol size violation (Task 63).

        Returns a length-prefixed JSON error response with:
        - error_type: "FRAME_TOO_LARGE"
        - actual_size: The actual frame size
        - max_size: The maximum allowed size
        - suggestion: Remediation suggestion
        """
        import json
        error = {
            "error_type": "FRAME_TOO_LARGE",
            "actual_size": self.actual_size,
            "max_size": self.max_size,
            "suggestion": "Split payload into smaller chunks or compress data",
        }
        json_bytes = json.dumps(error).encode("utf-8")
        length_prefix = len(json_bytes).to_bytes(LENGTH_PREFIX_SIZE, "big")
        return length_prefix + json_bytes


class ReceiveBuffer:
    def __init__(
        self,
        max_frame_length: int = MAX_FRAME_LENGTH,
        max_buffer_size: int = MAX_BUFFER_SIZE,
    ) -> None:
        self.buffer = bytearray()
        self._next_line_search = 0
        self._multiple_lines_search = 0
        self._max_frame_length = max_frame_length
        self._max_buffer_size = max_buffer_size

    def __iadd__(self, byteslike: bytes | bytearray) -> "ReceiveBuffer":
        new_size = len(self.buffer) + len(byteslike)
        if new_size > self._max_buffer_size:
            raise BufferOverflowError(
                f"Buffer would exceed max size: {new_size} > {self._max_buffer_size} bytes"
            )
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

        Raises:
            FrameTooLargeError: If the length prefix indicates a frame larger than max_frame_length
        """
        # Need at least the length prefix to know message size
        if len(self.buffer) < LENGTH_PREFIX_SIZE:
            return None

        # Read the length prefix (4 bytes, big-endian unsigned int)
        message_length = int.from_bytes(self.buffer[:LENGTH_PREFIX_SIZE], 'big')

        # Security check: reject frames that are too large
        if message_length > self._max_frame_length:
            raise FrameTooLargeError(
                f"Frame length exceeds maximum: {message_length} > {self._max_frame_length} bytes",
                actual_size=message_length,
                max_size=self._max_frame_length,
            )

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