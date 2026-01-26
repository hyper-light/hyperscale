"""
Fallback chain construction for datacenter routing (AD-36 Part 7).

Builds deterministic fallback chain that preserves AD-17 health bucket semantics.
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.bucket_selector import BucketSelector
from hyperscale.distributed.routing.candidate_filter import DatacenterCandidate
from hyperscale.distributed.routing.routing_state import DatacenterRoutingScore


@dataclass(slots=True)
class FallbackChain:
    """A fallback chain of datacenters for job dispatch."""

    primary_datacenters: list[str]  # Primary DCs from best bucket
    fallback_datacenters: list[str]  # Fallback DCs from lower buckets
    primary_bucket: str | None
    scores: dict[str, float]  # DC -> score mapping

    def get_ordered_chain(self) -> list[str]:
        """Get full chain in priority order."""
        return self.primary_datacenters + self.fallback_datacenters

    def get_primary(self) -> str | None:
        """Get the primary (best) datacenter."""
        return self.primary_datacenters[0] if self.primary_datacenters else None


class FallbackChainBuilder:
    """
    Builds fallback chains for job dispatch (AD-36 Part 7).

    Chain construction:
    1. Select primary_dcs from primary_bucket sorted by score
    2. Add remaining DCs from primary_bucket as fallback
    3. Append lower buckets (BUSY, then DEGRADED) sorted by score

    Preserves AD-17 semantics: HEALTHY > BUSY > DEGRADED ordering.
    """

    def __init__(self, bucket_selector: BucketSelector | None = None) -> None:
        self._bucket_selector = bucket_selector or BucketSelector()

    def build_chain(
        self,
        primary_scores: list[DatacenterRoutingScore],
        fallback_candidates: list[DatacenterCandidate],
        fallback_scores: dict[str, DatacenterRoutingScore],
        max_primary: int = 2,
    ) -> FallbackChain:
        """
        Build a fallback chain from scored candidates.

        Args:
            primary_scores: Scored and sorted primary bucket candidates
            fallback_candidates: Candidates from lower buckets
            fallback_scores: Scores for fallback candidates
            max_primary: Maximum number of primary DCs to select

        Returns:
            FallbackChain with ordered datacenters
        """
        # Primary DCs are top N from primary bucket
        primary_dcs = [s.datacenter_id for s in primary_scores[:max_primary]]

        # Remaining primary bucket DCs are fallback
        remaining_primary = [s.datacenter_id for s in primary_scores[max_primary:]]

        # Group fallback by bucket and sort each bucket by score
        fallback_by_bucket: dict[str, list[DatacenterRoutingScore]] = {}
        for candidate in fallback_candidates:
            score = fallback_scores.get(candidate.datacenter_id)
            if score:
                bucket = candidate.health_bucket
                if bucket not in fallback_by_bucket:
                    fallback_by_bucket[bucket] = []
                fallback_by_bucket[bucket].append(score)

        # Sort each bucket
        for bucket in fallback_by_bucket:
            fallback_by_bucket[bucket].sort(key=lambda s: s.final_score)

        # Build fallback chain: remaining primary, then BUSY, then DEGRADED
        fallback_chain: list[str] = remaining_primary.copy()

        for bucket in ["BUSY", "DEGRADED"]:
            if bucket in fallback_by_bucket:
                fallback_chain.extend(
                    s.datacenter_id for s in fallback_by_bucket[bucket]
                )

        # Build scores dict
        all_scores: dict[str, float] = {}
        for score in primary_scores:
            all_scores[score.datacenter_id] = score.final_score
        for scores_list in fallback_by_bucket.values():
            for score in scores_list:
                all_scores[score.datacenter_id] = score.final_score

        # Determine primary bucket
        primary_bucket = primary_scores[0].health_bucket if primary_scores else None

        return FallbackChain(
            primary_datacenters=primary_dcs,
            fallback_datacenters=fallback_chain,
            primary_bucket=primary_bucket,
            scores=all_scores,
        )

    def build_simple_chain(
        self,
        datacenters: list[str],
        health_buckets: dict[str, str],
    ) -> FallbackChain:
        """
        Build a simple chain without scoring (for bootstrap mode).

        Args:
            datacenters: List of datacenter IDs
            health_buckets: Mapping of DC ID to health bucket

        Returns:
            FallbackChain ordered by health bucket priority
        """
        # Group by bucket
        by_bucket: dict[str, list[str]] = {
            "HEALTHY": [],
            "BUSY": [],
            "DEGRADED": [],
        }

        for dc_id in datacenters:
            bucket = health_buckets.get(dc_id, "DEGRADED")
            if bucket in by_bucket:
                by_bucket[bucket].append(dc_id)

        # Build chain in bucket order
        primary_bucket: str | None = None
        primary_dcs: list[str] = []
        fallback_dcs: list[str] = []

        for bucket in ["HEALTHY", "BUSY", "DEGRADED"]:
            dcs = by_bucket[bucket]
            if not dcs:
                continue

            if primary_bucket is None:
                primary_bucket = bucket
                primary_dcs = dcs
            else:
                fallback_dcs.extend(dcs)

        return FallbackChain(
            primary_datacenters=primary_dcs,
            fallback_datacenters=fallback_dcs,
            primary_bucket=primary_bucket,
            scores={},
        )
