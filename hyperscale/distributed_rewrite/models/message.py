import io
import os
import secrets
import time
import cloudpickle
from typing import Self

from hyperscale.distributed_rewrite.models.restricted_unpickler import RestrictedUnpickler
from hyperscale.distributed_rewrite.taskex.snowflake import SnowflakeGenerator


def _generate_instance_id() -> int:
    """
    Generate a unique instance ID for the Snowflake generator.

    Combines:
    - PID (provides process uniqueness on same machine)
    - Random incarnation nonce (provides restart uniqueness)

    The Snowflake instance field is 10 bits (0-1023), so we combine
    5 bits from PID and 5 bits from random to maximize uniqueness.
    """
    pid_component = (os.getpid() & 0x1F) << 5  # 5 bits from PID, shifted left
    random_component = secrets.randbits(5)  # 5 random bits for incarnation
    return pid_component | random_component


# Module-level Snowflake generator for message IDs
# Uses combined PID + random incarnation for collision resistance
_message_id_generator = SnowflakeGenerator(instance=_generate_instance_id())

# Incarnation nonce - random value generated at module load time
# Used to detect messages from previous incarnations of this process
MESSAGE_INCARNATION = secrets.token_bytes(8)


def _generate_message_id() -> int:
    """Generate a unique message ID using Snowflake algorithm."""
    message_id = _message_id_generator.generate()
    # If generator returns None (sequence exhausted), wait and retry
    while message_id is None:
        time.sleep(0.001)  # Wait 1ms for next timestamp
        message_id = _message_id_generator.generate()
    return message_id


class Message:
    """
    Base class for all distributed messages.

    Uses restricted unpickling for secure deserialization - only allows
    safe standard library modules and hyperscale.* modules.

    Each message includes:
    - message_id: Unique Snowflake ID with embedded timestamp for replay detection
    - sender_incarnation: Random nonce identifying the sender's process incarnation

    The combination of message_id + sender_incarnation provides robust replay
    protection even across process restarts.
    """

    # Snowflake message ID for replay protection
    # Automatically generated on first access if not set
    _message_id: int | None = None

    # Sender incarnation - set from module-level constant on first access
    _sender_incarnation: bytes | None = None

    @property
    def message_id(self) -> int:
        """
        Get the message's unique ID.

        Generates a new Snowflake ID on first access. This ID embeds
        a timestamp and is used for replay attack detection.
        """
        if self._message_id is None:
            self._message_id = _generate_message_id()
        return self._message_id

    @message_id.setter
    def message_id(self, value: int) -> None:
        """Set the message ID (used during deserialization)."""
        self._message_id = value

    @property
    def sender_incarnation(self) -> bytes:
        """
        Get the sender's incarnation nonce.

        This 8-byte value is randomly generated when the sender process starts.
        It allows receivers to detect when a sender has restarted and clear
        stale replay protection state for that sender.
        """
        if self._sender_incarnation is None:
            self._sender_incarnation = MESSAGE_INCARNATION
        return self._sender_incarnation

    @sender_incarnation.setter
    def sender_incarnation(self, value: bytes) -> None:
        """Set the sender incarnation (used during deserialization)."""
        self._sender_incarnation = value

    @classmethod
    def load(cls, data: bytes) -> Self:
        """
        Securely deserialize a message using restricted unpickling.
        
        This prevents arbitrary code execution by blocking dangerous
        modules like os, subprocess, sys, etc.
        
        Args:
            data: Pickled message bytes
            
        Returns:
            The deserialized message
            
        Raises:
            SecurityError: If the data tries to load blocked modules/classes
        """
        return RestrictedUnpickler(io.BytesIO(data)).load()
    
    def dump(self) -> bytes:
        """Serialize the message using cloudpickle."""
        return cloudpickle.dumps(self)
