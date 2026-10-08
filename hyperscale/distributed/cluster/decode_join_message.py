from typing import TypeVar

from hyperscale.distributed.models.message import Message

from .cluster_join_error import ClusterJoinError


JoinMessage = TypeVar("JoinMessage", bound=Message)


def decode_join_message(
    data: bytes,
    message_type: type[JoinMessage],
    description: str,
) -> JoinMessage:
    """Decode a message on the join path or raise ``ClusterJoinError``.

    ``Message.load`` returns whatever object was pickled (it is not typed
    to the class it is called on), and non-pickle replies such as a
    handler's ``b"error"`` raise pickling errors. Both become a refusal
    the operator can read instead of an opaque traceback.
    """
    try:
        decoded = message_type.load(data)

    except Exception as decode_error:
        raise ClusterJoinError(
            f"malformed {description}: {type(decode_error).__name__}: {decode_error}"
        ) from decode_error

    if not isinstance(decoded, message_type):
        raise ClusterJoinError(
            f"malformed {description}: expected {message_type.__name__}, "
            f"got {type(decoded).__name__}"
        )

    return decoded
