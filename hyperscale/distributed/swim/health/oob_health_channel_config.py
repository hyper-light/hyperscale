"""``OOBHealthChannelConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.health.out_of_band_health_channel`` (see that module)."""

from dataclasses import dataclass

# Maximum OOB message size (minimal for fast processing)
MAX_OOB_MESSAGE_SIZE = 64

# Rate limiting for OOB channel
OOB_MAX_PROBES_PER_SECOND = 100

OOB_PROBE_COOLDOWN = 0.01  # 10ms between probes to same target


@dataclass(slots=True)
class OOBHealthChannelConfig:
    """Configuration for out-of-band health channel."""

    # Port offset from main UDP port (e.g., if main is 8000, OOB is 8000 + offset)
    port_offset: int = 100

    # Timeout for OOB probes (shorter than regular probes)
    probe_timeout_seconds: float = 0.5

    # Maximum probes per second (global rate limit)
    max_probes_per_second: int = OOB_MAX_PROBES_PER_SECOND

    # Cooldown between probes to same target
    per_target_cooldown_seconds: float = OOB_PROBE_COOLDOWN

    # Buffer size for receiving
    receive_buffer_size: int = MAX_OOB_MESSAGE_SIZE

    # Enable NACK responses when overloaded
    send_nack_when_overloaded: bool = True
