"""``TaskOverloadError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .resource_error import ResourceError


class TaskOverloadError(ResourceError):
    """Too many concurrent tasks running."""
    
    def __init__(self, task_count: int, max_tasks: int):
        super().__init__(
            message=f"Task overload: {task_count}/{max_tasks} tasks",
            task_count=task_count,
            max_tasks=max_tasks,
        )
