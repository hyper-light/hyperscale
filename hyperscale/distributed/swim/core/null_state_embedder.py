"""``NullStateEmbedder`` -- pickled under the namespace
``hyperscale.distributed.swim.core.state_embedder`` (see that module)."""

from hyperscale.distributed.health.tracker import HealthPiggyback


class NullStateEmbedder:
    """
    Default no-op state embedder.

    Used when no state embedding is needed (base HealthAwareServer behavior).
    """

    def get_state(self) -> bytes | None:
        """No state to embed."""
        return None

    async def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Ignore received state."""
        pass

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """No health piggyback available."""
        return None

    def record_probe_rtt(self, source_addr: tuple[str, int], rtt_ms: float) -> None:
        return None
