"""``AllocationResult`` -- pickled under the namespace
``hyperscale.distributed.jobs.core_allocator`` (see that module)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class AllocationResult:
    """Result of a core allocation attempt."""

    success: bool
    allocated_cores: list[int] = field(default_factory=list)
    error: str | None = None
    # The allocator's availability version once this allocation landed.
    availability_version: int = 0
