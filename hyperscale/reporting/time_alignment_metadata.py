from dataclasses import dataclass


@dataclass
class TimeAlignmentMetadata:
    """Metadata about the time alignment performed during aggregation."""
    reference_time: float       # The target alignment timestamp
    min_collected_at: float     # Earliest collection time
    max_collected_at: float     # Latest collection time
    time_spread_seconds: float  # Spread between earliest and latest
    sources_count: int          # Number of sources aggregated
    sources: list[str]          # Source identifiers
