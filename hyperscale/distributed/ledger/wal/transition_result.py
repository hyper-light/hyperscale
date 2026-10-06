"""``TransitionResult`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.entry_state`` (see that module)."""

from enum import Enum


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
