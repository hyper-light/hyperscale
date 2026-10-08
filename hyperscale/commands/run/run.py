from hyperscale.commands.cli import CLI
from .gate import gate
from .manager import manager
from .worker import worker
from .workflow import workflow



@CLI.group(
    gate,
    manager,
    worker,
    workflow,
)
async def run():
    '''
    Run a test workflow or a Hyperscale server (worker, manager, gate)
    '''