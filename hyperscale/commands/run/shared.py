import asyncio
import functools
import json
import os
import psutil


from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.distributed.env import Env, load_env


async def get_default_workers():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            psutil.cpu_count,
            logical=False,
        ),
    )

def _get_default_config():
    config = HyperscaleConfig()
    config_path = ".hyperscale.config.json"
    if not os.path.exists(config_path):
        with open(config_path, "w") as config_file:
            json.dump(
                config.model_dump(),
                config_file,
                indent=4,
            )

    else:
        with open(config_path, "r") as config_file:
            config_data = json.load(config_file)
            config_data["logs_directory"] = os.path.join(
                os.getcwd(),
                "logs",
            )

            config = HyperscaleConfig(**config_data)

    return config


async def get_default_config():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        _get_default_config,
    )

AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"


def resolve_auth_secret(acm_secret: str | None) -> str:
    """Resolve the cluster auth secret every node must share.

    Precedence: the ``--acm-secret`` flag, then ``MERCURY_SYNC_AUTH_SECRET``,
    then the distributed ``Env`` default — the same env-then-default
    resolution ``LocalRunner`` and ``ServerRunner`` use. The secret keys
    message encryption, so it must be identical across the cluster; a
    per-process random default made every multi-node cluster unable to
    communicate.
    """
    if acm_secret:
        return acm_secret

    return os.getenv(AUTH_SECRET_ENVAR) or Env().MERCURY_SYNC_AUTH_SECRET
    

def node_env(**explicit_values) -> Env:
    """The ``Env`` a ``hyperscale run worker|manager|gate`` node runs with.

    Every setting is read from the process environment (and a ``.env``
    file), with the command's explicit flags taking precedence -- building
    ``Env`` from the flags alone silently ignored every other setting an
    operator exported (resource guards, timeouts, intervals).
    """
    return load_env(Env, override=Env(**explicit_values))
