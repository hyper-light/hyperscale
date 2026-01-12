from enum import Enum, IntEnum


class WALEntryState(IntEnum):
    """
    State machine for WAL entries tracking durability progress.

    Transitions: PENDING -> REGIONAL -> GLOBAL -> APPLIED -> COMPACTED
    """

    PENDING = 0
    REGIONAL = 1
    GLOBAL = 2
    APPLIED = 3
    COMPACTED = 4


class TransitionResult(Enum):
    SUCCESS = "success"
    ALREADY_AT_STATE = "already_at_state"
    ALREADY_PAST_STATE = "already_past_state"
    ENTRY_NOT_FOUND = "entry_not_found"
    INVALID_TRANSITION = "invalid_transition"

    @property
    def is_ok(self) -> bool:
        return self in (
            TransitionResult.SUCCESS,
            TransitionResult.ALREADY_AT_STATE,
            TransitionResult.ALREADY_PAST_STATE,
        )
