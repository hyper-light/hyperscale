"""``FrameTooLargeError`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.receive_buffer`` (see that module)."""

from __future__ import annotations

import json

from .receive_buffer_shared import LENGTH_PREFIX_SIZE


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
        error = {
            "error_type": "FRAME_TOO_LARGE",
            "actual_size": self.actual_size,
            "max_size": self.max_size,
            "suggestion": "Split payload into smaller chunks or compress data",
        }
        json_bytes = json.dumps(error).encode("utf-8")
        length_prefix = len(json_bytes).to_bytes(LENGTH_PREFIX_SIZE, "big")
        return length_prefix + json_bytes
