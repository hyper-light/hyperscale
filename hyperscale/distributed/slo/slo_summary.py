from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env import Env

from .latency_observation import LatencyObservation
from .latency_slo import LatencySLO
from .slo_compliance_score import SLOComplianceScore


# Field count for ``SLOSummary.to_bytes`` / ``from_bytes``. The
# wire format keeps every field as a primitive scalar so the
# message embeds cleanly into ``WorkerHeartbeat`` /
# ``ManagerHeartbeat`` without bringing the whole T-Digest along.
_SLO_SUMMARY_FIELD_COUNT: int = 7


@dataclass(slots=True)
class SLOSummary:
    """Compact SLO summary for SWIM gossip (AD-42 Part 4).

    ~32 bytes on the wire vs ~1.6 KiB for a full T-Digest. The
    AD-42 hierarchy uses these summaries at every tier:

    * Worker -> Manager: per-worker recent latency
    * Manager -> Gate: per-DC aggregated latency
    * Gate <-> Gate: cluster-wide cross-DC latency

    Attributes:
        p50_ms / p95_ms / p99_ms: Streaming percentiles aggregated
            from the most-recent windowed T-Digest.
        sample_count: Number of observations the percentiles were
            computed over. Compliance scoring derates the composite
            score below ``min_sample_count`` so a sparse summary
            doesn't dominate routing decisions.
        compliance_score: Pre-computed SLOComplianceScore.composite_
            score so receivers don't need the full SLO config to
            interpret. Range typically [0.0, 1.5+]; lower is
            healthier.
        routing_factor: Pre-computed ``SLOComplianceScore.routing_
            factor`` so AD-36 routing can use the summary directly
            without re-running the SLO config every receive.
        updated_at: Sender's monotonic clock at summary build time.
            Receivers use this for staleness pruning.
    """

    p50_ms: float
    p95_ms: float
    p99_ms: float
    sample_count: int
    compliance_score: float
    routing_factor: float
    updated_at: float

    @classmethod
    def from_observation(
        cls,
        *,
        observation: LatencyObservation,
        slo: LatencySLO | None = None,
        env: Env | None = None,
    ) -> "SLOSummary":
        """Build a summary from a ``LatencyObservation``.

        Composes the SLOComplianceScore inline so the receiver
        doesn't need access to the sender's SLO config — the
        summary self-describes its compliance.
        """
        latency_slo = slo if slo is not None else LatencySLO.from_env(env)
        score = SLOComplianceScore.calculate(
            target_id=observation.target_id,
            observation=observation,
            slo=latency_slo,
            env=env,
        )
        return cls(
            p50_ms=observation.p50_ms,
            p95_ms=observation.p95_ms,
            p99_ms=observation.p99_ms,
            sample_count=observation.sample_count,
            compliance_score=score.composite_score,
            routing_factor=score.routing_factor,
            updated_at=observation.window_end,
        )

    @classmethod
    def empty(cls, *, updated_at: float = 0.0) -> "SLOSummary":
        """Return a zero-sample baseline summary.

        Used when there are no observations yet (e.g. a freshly-
        started worker hasn't completed any workflows). Compliance
        score 1.0 and routing factor 1.0 are the neutral
        baselines: receivers treat the sender as compliant by
        default until real data arrives.
        """
        return cls(
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            sample_count=0,
            compliance_score=1.0,
            routing_factor=1.0,
            updated_at=updated_at,
        )

    def to_bytes(self) -> bytes:
        """Serialize for AD-42 piggyback dissemination.

        Format: 7 ``:``-delimited float/int fields. All primitives,
        no nested types, so the round-trip is lossless.
        """
        parts = [
            f"{self.p50_ms:.6f}".encode(),
            f"{self.p95_ms:.6f}".encode(),
            f"{self.p99_ms:.6f}".encode(),
            str(self.sample_count).encode(),
            f"{self.compliance_score:.6f}".encode(),
            f"{self.routing_factor:.6f}".encode(),
            f"{self.updated_at:.6f}".encode(),
        ]
        return b":".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "SLOSummary | None":
        """Deserialize from piggyback bytes. Returns ``None`` on
        any parse error so a single corrupt entry can't sink a
        whole frame."""
        try:
            decoded = data.decode()
            parts = decoded.split(":")
            if len(parts) < _SLO_SUMMARY_FIELD_COUNT:
                return None
            return cls(
                p50_ms=float(parts[0]),
                p95_ms=float(parts[1]),
                p99_ms=float(parts[2]),
                sample_count=int(parts[3]),
                compliance_score=float(parts[4]),
                routing_factor=float(parts[5]),
                updated_at=float(parts[6]),
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None

    def is_empty(self) -> bool:
        """True if no samples have been recorded yet."""
        return self.sample_count <= 0
