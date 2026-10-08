"""Definitions shared by the classes of
``hyperscale.distributed.server.protocol.receive_buffer`` (see that module)."""

from __future__ import annotations


# Length prefix size (4 bytes = 32-bit unsigned integer, supports up to ~4GB messages)
LENGTH_PREFIX_SIZE = 4
