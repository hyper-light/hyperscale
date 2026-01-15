from dataclasses import dataclass

from hyperscale.distributed.env import Env


@dataclass(slots=True)
class IdempotencyConfig:
    """Configuration settings for idempotency handling."""

    pending_ttl_seconds: float = 60.0
    committed_ttl_seconds: float = 300.0
    rejected_ttl_seconds: float = 60.0
    max_entries: int = 100_000
    cleanup_interval_seconds: float = 10.0
    wait_for_pending: bool = True
    pending_wait_timeout: float = 30.0

    @classmethod
    def from_env(cls, env: Env) -> "IdempotencyConfig":
        """Create a config instance from environment settings."""
        return cls(
            pending_ttl_seconds=env.IDEMPOTENCY_PENDING_TTL_SECONDS,
            committed_ttl_seconds=env.IDEMPOTENCY_COMMITTED_TTL_SECONDS,
            rejected_ttl_seconds=env.IDEMPOTENCY_REJECTED_TTL_SECONDS,
            max_entries=env.IDEMPOTENCY_MAX_ENTRIES,
            cleanup_interval_seconds=env.IDEMPOTENCY_CLEANUP_INTERVAL_SECONDS,
            wait_for_pending=env.IDEMPOTENCY_WAIT_FOR_PENDING,
            pending_wait_timeout=env.IDEMPOTENCY_PENDING_WAIT_TIMEOUT,
        )


def create_idempotency_config_from_env(env: Env) -> IdempotencyConfig:
    """Create idempotency config using Env values."""
    return IdempotencyConfig.from_env(env)
