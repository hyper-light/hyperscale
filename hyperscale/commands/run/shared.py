import asyncio
import contextlib
import functools
import json
import os
import pathlib
import re
import sys
import psutil


from hyperscale.core.jobs.models import HyperscaleConfig, TerminalMode
from hyperscale.distributed.env import Env, load_env
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.raft.store.raft_store import RaftStore
from hyperscale.distributed.runtime import RealClock, RealFilesystem, RealRandom
from hyperscale.distributed.swim.core.node_id import NodeId
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from hyperscale.ui.ci_safe import TerminalSelection
from hyperscale.ui.node_dashboard.models import NodeDashboardLayout
from hyperscale.ui.node_dashboard.terminal_capability import select_node_terminal_mode

from .cluster_cookie import ClusterCookie
from .cluster_cookie_unavailable_error import ClusterCookieUnavailableError


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


async def resolve_auth_secret(acm_secret: str | None) -> str:
    """Resolve the cluster auth secret every node must share.

    Precedence: the ``--acm-secret`` flag, then ``MERCURY_SYNC_AUTH_SECRET``,
    then the per-user cluster cookie (``ClusterCookie``), created on first
    use. The secret keys message authentication and encryption, so it must
    be identical across the cluster and must never be a published default:
    a cookie that cannot be created, read or trusted fails the command
    (exit status 1, the reason and both ways to configure a secret on
    stderr) rather than running without one.
    """
    if acm_secret:
        return acm_secret

    if environment_secret := os.getenv(AUTH_SECRET_ENVAR):
        return environment_secret

    return await cluster_cookie_secret()


async def cluster_cookie_secret() -> str:
    """The current user's cluster cookie secret; a cookie that cannot be
    used ends the command with exit status 1 and the reason on stderr."""
    try:
        return await ClusterCookie.secret_for_current_user()
    except ClusterCookieUnavailableError as cookie_error:
        print(str(cookie_error), file=sys.stderr)
        raise SystemExit(1) from cookie_error


def node_env(**explicit_values) -> Env:
    """The ``Env`` a ``hyperscale run worker|manager|gate`` node runs with.

    Every setting is read from the process environment (and a ``.env``
    file), with the command's explicit flags taking precedence -- building
    ``Env`` from the flags alone silently ignored every other setting an
    operator exported (resource guards, timeouts, intervals).
    """
    return load_env(Env, override=Env(**explicit_values))


_UNSAFE_PATH_CHARACTERS = re.compile(r"[^A-Za-z0-9._-]")


async def node_terminal_mode(
    configured_mode: TerminalMode,
    quiet: bool,
    layout: NodeDashboardLayout,
) -> TerminalSelection:
    """The terminal mode a ``hyperscale run worker|manager|gate`` node's
    dashboard runs in, detected at runtime (``select_node_terminal_mode``):
    "full" only where stdout can show ``layout``'s dashboard, otherwise
    append-only summary lines; --quiet and an explicit mode win."""
    return await select_node_terminal_mode(configured_mode, quiet, layout)


def node_log_path(
    logs_directory: str,
    role: str,
    datacenter: str,
    host: str,
    tcp_port: int,
) -> pathlib.Path:
    """The file a node's stderr logs go to while its dashboard renders: one
    per node identity in the configured logs directory, named as its data
    directory is."""
    node_name = _UNSAFE_PATH_CHARACTERS.sub("_", f"{role}-{datacenter}-{host}-{tcp_port}")
    return pathlib.Path(logs_directory).absolute() / f"{node_name}.log"


def _create_node_data_directory(
    data_directory: str | None,
    logs_directory: str,
    role: str,
    datacenter: str,
    host: str,
    tcp_port: int,
) -> pathlib.Path:
    if data_directory:
        directory = pathlib.Path(data_directory)
    else:
        # One directory per node identity: a node restarted at the same
        # address finds -- and recovers from -- its own durable state.
        # Host characters a filesystem may reject (IPv6 colons) are
        # replaced, so the name is valid on any OS.
        node_name = _UNSAFE_PATH_CHARACTERS.sub(
            "_",
            f"{role}-{datacenter}-{host}-{tcp_port}",
        )
        directory = pathlib.Path(logs_directory).parent / "data" / node_name

    directory = directory.absolute()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


async def node_data_directory(
    data_directory: str | None,
    logs_directory: str,
    role: str,
    datacenter: str,
    host: str,
    tcp_port: int,
) -> pathlib.Path:
    """The directory a manager or gate keeps its durable state in (job
    ledger WAL, checkpoints, archive, idempotency WAL, incarnation).

    ``--data-directory`` when given; otherwise a directory per node
    identity under ``data/`` beside the configured logs directory. It is
    created if missing; a location that cannot be created fails the
    command instead of running the node without durability.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            _create_node_data_directory,
            data_directory,
            logs_directory,
            role,
            datacenter,
            host,
            tcp_port,
        ),
    )


async def drain_cluster_membership(node, timeout_seconds: float) -> None:
    """Before an operator stop aborts a manager or gate, have its cluster
    release the node's membership (AD-52 section 13) so the cluster does
    not wait out the tombstone retention for it. A drain that fails or
    overruns ``timeout_seconds`` is reported on stderr and the stop goes on:
    the leader's silence detection still releases the node later."""
    try:
        await asyncio.wait_for(node.leave_cluster(), timeout=timeout_seconds)
    except Exception as drain_error:
        print(
            f"cluster membership drain did not finish: {type(drain_error).__name__}: {drain_error}",
            file=sys.stderr,
        )


@contextlib.asynccontextmanager
async def opened_raft_store(
    node_directory: pathlib.Path,
    env: Env,
    datacenter: str,
    host: str,
    udp_port: int,
):
    """The node's Raft store (D1), open for the node's whole run: it
    resumes the identity and every Raft group its disk holds when that disk
    is this node's and intact, sets an untrustworthy one aside, and starts
    a new identity otherwise. The node is handed the store and runs as the
    identity it holds. A store that cannot be opened at all fails the
    command, as an unusable data directory does."""
    filesystem = RealFilesystem()
    task_runner = TaskRunner(0, env)
    store = RaftStore(
        directory=node_directory / "raft",
        filesystem=filesystem,
        random_source=RealRandom(),
        clock=RealClock(),
        logger=Logger(),
        task_runner=task_runner,
        set_aside_retained=env.RAFT_SET_ASIDE_RETAINED,
        storage_health=StorageHealth(),
    )
    fresh_node_id_full = NodeId.generate(datacenter, host=host, port=udp_port).full
    try:
        await store.open(
            fresh_node_id_full,
            is_this_node=lambda node_id_full: NodeId.placement_of(node_id_full)
            == NodeId.placement_of(fresh_node_id_full),
        )
        yield store
    finally:
        await store.close()
        await task_runner.shutdown()
        filesystem.shutdown(wait=True)
