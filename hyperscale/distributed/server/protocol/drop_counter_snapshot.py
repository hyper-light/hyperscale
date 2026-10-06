"""``DropCounterSnapshot`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.drop_counter`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DropCounterSnapshot:
    """Immutable snapshot of drop counter values."""

    rate_limited: int
    message_too_large: int
    decompression_too_large: int
    decryption_failed: int
    malformed_message: int
    replay_detected: int
    load_shed: int  # AD-32: Messages dropped due to backpressure
    log_write_failed: int
    interval_seconds: float

    @property
    def total(self) -> int:
        return (
            self.rate_limited
            + self.message_too_large
            + self.decompression_too_large
            + self.decryption_failed
            + self.malformed_message
            + self.replay_detected
            + self.load_shed
            + self.log_write_failed
        )

    @property
    def has_drops(self) -> bool:
        return self.total > 0
