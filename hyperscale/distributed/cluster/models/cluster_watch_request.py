from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterWatchRequest(Message):
    """Watch a cluster's membership (AD-52 section 9): a long poll the
    member answers with the membership changes it applied after
    ``after_index`` of cluster ``cluster_uuid`` -- at once if there are
    any, else once one is applied or ``wait_seconds`` pass. A watcher of
    another cluster (the cluster was founded anew), or behind the member's
    log compaction, is answered with a snapshot to resume from."""

    cluster_uuid: str | None
    after_index: int
    wait_seconds: float
