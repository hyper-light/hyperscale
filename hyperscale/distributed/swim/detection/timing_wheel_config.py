"""``TimingWheelConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.timing_wheel`` (see that module)."""

from dataclasses import dataclass


@dataclass
class TimingWheelConfig:
    """Configuration shim for backward compatibility.

    The wheel-resolution fields (``coarse_tick_ms``, ``fine_tick_ms``,
    wheel sizes, ``fine_wheel_threshold_ms``) parameterised the
    previous polling-tick wheel. The event-driven registry does not
    consume them — asyncio's timer queue resolves expirations to its
    own precision (microseconds in practice). The fields and the
    historical ``coarse_tick_ms == fine_tick_ms * fine_wheel_size``
    invariant remain so existing callers that pass them through
    don't error out.
    """

    coarse_tick_ms: int = 1000
    coarse_wheel_size: int = 64
    fine_tick_ms: int = 100
    fine_wheel_size: int = 10
    fine_wheel_threshold_ms: int = 1000

    def __post_init__(self) -> None:
        expected_coarse = self.fine_tick_ms * self.fine_wheel_size
        if self.coarse_tick_ms != expected_coarse:
            raise ValueError(
                f"TimingWheelConfig: coarse_tick_ms must equal "
                f"fine_tick_ms * fine_wheel_size "
                f"({self.fine_tick_ms} * {self.fine_wheel_size} = "
                f"{expected_coarse}); got coarse_tick_ms={self.coarse_tick_ms}."
            )
        if self.fine_wheel_threshold_ms > expected_coarse:
            raise ValueError(
                f"TimingWheelConfig: fine_wheel_threshold_ms "
                f"({self.fine_wheel_threshold_ms}) cannot exceed the fine "
                f"wheel span ({expected_coarse})."
            )
