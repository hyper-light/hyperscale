from hyperscale.commands.cli import CLI

from .cancel import cancel
from .status import status


@CLI.group(
    cancel,
    status,
)
async def job():
    """
    Inspect or cancel a job running on a Hyperscale cluster
    """
