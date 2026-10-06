import dataclasses
import io
import os
import secrets
import cloudpickle
from typing import Self

from hyperscale.distributed.models.restricted_unpickler import RestrictedUnpickler
from hyperscale.distributed.taskex.snowflake import SnowflakeGenerator


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


# Module-level Snowflake generator for message IDs — LAZY.
#
# Construction is deferred to first use for two determinism reasons
# (both measured as chaos-VOPR twin forks):
# * the taskex generator captures its clock AT CONSTRUCTION — a
#   module-import-time instance is born BEFORE the SIM seam swap
#   (spawn bootstrap imports the entry's whole module graph first)
#   and keeps the REAL clock forever, so every message id embeds a
#   wall-time cursor that differs run to run;
# * the instance bits combine PID + secrets — random per run. Under
#   SIM the swapped ``_DEFAULT_RANDOM`` supplies them
#   deterministically; REAL mode keeps the PID+secrets nonce.
_DEFAULT_RANDOM = None
_message_id_generator: SnowflakeGenerator | None = None


def _get_message_id_generator() -> SnowflakeGenerator:
    global _message_id_generator
    if _message_id_generator is None:
        if _DEFAULT_RANDOM is not None:
            instance = int(_DEFAULT_RANDOM.uniform(0.0, 1023.0))
        else:
            instance = _generate_instance_id()
        _message_id_generator = SnowflakeGenerator(instance=instance)
    return _message_id_generator

# Incarnation nonce - random value generated at module load time
# Used to detect messages from previous incarnations of this process
MESSAGE_INCARNATION = secrets.token_bytes(8)


def generate_message_id() -> int:
    """Generate a unique message ID using Snowflake algorithm.

    ``generate_sync`` is total and monotone (backwards realtime steps
    and same-millisecond sequence exhaustion are absorbed by the
    generator), so no retry is needed — the previous ``None``-retry
    here spun a blocking ``time.sleep`` on the event-loop thread, which
    under a frozen virtual clock could never terminate.
    """
    return _get_message_id_generator().generate_sync()


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
            self._message_id = generate_message_id()
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

    def __getattr__(self, name: str) -> object:
        """A field this message's sender did not have reads as the field's
        default (AD-25 rolling upgrades).

        A sender running an older version pickles only the fields it knows,
        so a field added since is unset on this side, and reading it raised
        AttributeError. Python calls this only after normal lookup fails, so
        reading a set field costs nothing extra. The default is stored, so
        later reads are ordinary. Every other missing attribute raises as
        usual. A sender running a newer version is already read safely: its
        unknown fields land in the instance ``__dict__`` and are never read.
        """
        if (message_field := getattr(type(self), "__dataclass_fields__", {}).get(name)) is None:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        default = _field_default(message_field, type(self).__name__)
        object.__setattr__(self, name, default)
        return default

    def dump(self) -> bytes:
        """Serialize the message using cloudpickle, stamped with a fresh
        message id and this process's incarnation.

        Both are stamped on every serialization, so each frame carries the
        id the receiver's replay guard checks. Generated lazily on first
        read instead, they were never pickled unless something happened to
        read them first; the receiver then minted a fresh id of its own for
        every copy of a frame, so a captured frame replayed as new every
        time. A resend serializes again under a new id: replay protection
        refuses captured frames, while idempotency keys deduplicate retries.
        """
        self._message_id = generate_message_id()
        self._sender_incarnation = MESSAGE_INCARNATION
        return cloudpickle.dumps(self)


def _field_default(message_field: dataclasses.Field, message_type_name: str) -> object:
    """The value a field takes when its sender did not send it: the
    field's default, or a fresh value from its default factory.

    Raises:
        AttributeError: the field has no default, so a message without it
            cannot be read (adding a field with no default is a breaking
            wire change).
    """
    if message_field.default_factory is not dataclasses.MISSING:
        return message_field.default_factory()
    if message_field.default is dataclasses.MISSING:
        raise AttributeError(
            f"{message_type_name!r} message lacks field {message_field.name!r}, which has no default"
        )
    return message_field.default
