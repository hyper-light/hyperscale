import asyncio
import functools
import psutil
import uvloop

from typing import Literal

from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow
from .cli import (
    CLI,
    ImportFile,
    JsonFile,
    AssertSet,
)
from .hyperscale_config import HyperscaleConfig

uvloop.install()


async def new(
    path: str,
    overwrite: bool,
):
    pass
