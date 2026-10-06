import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True, eq=False)
class WorkflowRunControl:
    """
    One submission of a workflow's run on a node, as cancellation reaches
    it: whether its VUs may start another iteration, and whether the run
    has ended (finished, failed, rejected or cancelled). Compared by
    identity, as a run can be submitted to a node more than once.
    """

    running: bool = True
    ended: asyncio.Event = field(default_factory=asyncio.Event)
