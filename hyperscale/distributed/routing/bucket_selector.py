"""
Health bucket selection for datacenter routing (AD-36 Part 3).

Preserves AD-17 health bucket ordering: HEALTHY > BUSY > DEGRADED.
UNHEALTHY datacenters are excluded (handled by CandidateFilter).
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.candidate_filter import (
    DatacenterCandidate,
)


@dataclass(slots=True)
class BucketSelectionResult:
    """Result of bucket selection."""

    primary_bucket: str | None  # HEALTHY, BUSY, or DEGRADED
    primary_candidates: list[DatacenterCandidate]
    fallback_candidates: list[DatacenterCandidate]
    bucket_counts: dict[str, int]


class BucketSelector:
    """
    Selects the primary health bucket for routing (AD-36 Part 3).

    Bucket priority: HEALTHY > BUSY > DEGRADED
    UNHEALTHY is never selected (excluded by CandidateFilter).

    Only candidates in the primary_bucket are eligible for primary selection.
    Lower buckets are fallback only.
    """

    # Bucket priority order (higher index = better)
    BUCKET_PRIORITY = ["DEGRADED", "BUSY", "HEALTHY"]

    def select_bucket(
        self,
        candidates: list[DatacenterCandidate],
    ) -> BucketSelectionResult:
        """
        Select primary bucket and partition candidates.

        Args:
            candidates: Filtered (non-excluded) datacenter candidates

        Returns:
            BucketSelectionResult with primary and fallback candidates
        """
        # Group by health bucket
        by_bucket: dict[str, list[DatacenterCandidate]] = {
            "HEALTHY": [],
            "BUSY": [],
            "DEGRADED": [],
        }

        for candidate in candidates:
            bucket = candidate.health_bucket
            if bucket in by_bucket:
                by_bucket[bucket].append(candidate)

        # Find primary bucket (first non-empty in priority order)
        primary_bucket: str | None = None
        for bucket in reversed(self.BUCKET_PRIORITY):  # HEALTHY first
            if by_bucket[bucket]:
                primary_bucket = bucket
                break

        if primary_bucket is None:
            return BucketSelectionResult(
                primary_bucket=None,
                primary_candidates=[],
                fallback_candidates=[],
                bucket_counts={b: len(c) for b, c in by_bucket.items()},
            )

        # Primary candidates are from primary bucket
        primary_candidates = by_bucket[primary_bucket]

        # Fallback candidates are from lower buckets
        fallback_candidates: list[DatacenterCandidate] = []
        primary_idx = self.BUCKET_PRIORITY.index(primary_bucket)

        for idx, bucket in enumerate(self.BUCKET_PRIORITY):
            if idx < primary_idx:  # Lower priority buckets
                fallback_candidates.extend(by_bucket[bucket])

        return BucketSelectionResult(
            primary_bucket=primary_bucket,
            primary_candidates=primary_candidates,
            fallback_candidates=fallback_candidates,
            bucket_counts={b: len(c) for b, c in by_bucket.items()},
        )

    @staticmethod
    def is_bucket_drop(
        current_bucket: str | None,
        new_bucket: str | None,
    ) -> bool:
        """
        Check if switching buckets represents a "drop" (degradation).

        Used to force switch when current DC drops to a lower bucket.
        """
        if current_bucket is None or new_bucket is None:
            return False

        try:
            current_idx = BucketSelector.BUCKET_PRIORITY.index(current_bucket)
            new_idx = BucketSelector.BUCKET_PRIORITY.index(new_bucket)
            return new_idx < current_idx
        except ValueError:
            return False
