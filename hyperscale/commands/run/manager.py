import asyncio
from hyperscale.commands.cli import JsonFile, command, AssertSet
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.core.engines.client.time_parser import TimeParser

from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.logging import LoggingConfig, LogLevelName

from .node_address import parse_node_host, parse_peer_addresses
from .seed_locators import is_dynamic_locator, resolve_cohort_addresses
from .shared import (
    opened_raft_store,
    get_default_config,
    node_data_directory,
    node_env,
    drain_cluster_membership,
    resolve_auth_secret,
)
from hyperscale.core.jobs.runner.shutdown_signals import ShutdownSignals


@command(
    display_help_on_error=False,
    shortnames={
        "host": "H",
        "managers": "m",
        "manager_udp": "M",
        "gates": "g",
        "gate_udp": "G",
        "data_directory": "D",
        "leader_lease_enabled": "L",
    },
)
async def manager(
    host: str = "127.0.0.1",
    tcp_port: int = 8231,
    udp_port: int = 8241,
    datacenter: str = 'default',
    boot_timeout: str = '5m',
    shutdown_timeout: str = "1m",
    acm_secret: str | None = None,
    data_directory: str | None = None,
    managers: list[str] = [],
    manager_udp: list[str] = [],
    gates: list[str] = [],
    gate_udp: list[str] = [],
    cohort_size: int | None = None,
    leader_lease_enabled: bool = False,
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Run a Hyperscale manager. Its datacenter's managers form one election
    cohort, listed at boot with --managers/--manager-udp; without them the
    manager runs standalone and accepts worker registrations, and
    `hyperscale join` connects it to a gate.

    @param host The address to bind and advertise to peers (an IP, or a stable DNS name such as a StatefulSet pod's)
    @param tcp_port The TCP port for data operations
    @param udp_port The UDP port for SWIM health checks
    @param datacenter The datacenter this manager belongs to
    @param boot_timeout How long to wait for the manager to boot
    @param shutdown_timeout How long to wait for the manager to shut down
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param data_directory Where the manager keeps its durable state (defaults to a directory per manager under data/ beside the logs directory)
    @param managers The TCP host:port of every manager in this datacenter's cohort (this manager's own entry is skipped)
    @param manager_udp The UDP host:port of the same managers, in the same order
    @param cohort_size How many members the datacenter's manager cohort has -- required when --managers gives seed locators (dns://, dns-srv://, file://, exec://), which resolve at launch until they yield exactly this many
    @param leader_lease_enabled Serve linearizable cluster reads from the leader's lease instead of a quorum round each (AD-52 section 11) -- only where every node's clock is NTP-synchronized, and on every node of the cluster alike
    @param gates The TCP host:port of the gates this manager reports to
    @param gate_udp The UDP host:port of the same gates, in the same order
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use
    """
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    host = parse_node_host(host, tcp_port)
    gate_tcp_addresses, gate_udp_addresses = parse_peer_addresses(
        gates, gate_udp, "--gates", "--gate-udp", (host, tcp_port)
    )

    env = node_env(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
        # Given only when set, so an operator's exported setting stands.
        **({"RAFT_LEADER_LEASES_ENABLED": True} if leader_lease_enabled else {}),
    )

    # AD-52 section 2: seed locators resolve at launch, to exactly the
    # cohort size every founder agrees on.
    if any(is_dynamic_locator(entry) for entry in [*managers, *manager_udp]):
        peer_tcp_addresses, peer_udp_addresses = await resolve_cohort_addresses(
            managers,
            manager_udp,
            "--managers",
            "--manager-udp",
            (host, tcp_port),
            cohort_size,
            TimeParser(boot_timeout).time,
            env.CLUSTER_FORMATION_INTERVAL_SECONDS,
            env.MANAGER_TCP_TIMEOUT_STANDARD,
        )
    else:
        peer_tcp_addresses, peer_udp_addresses = parse_peer_addresses(
            managers, manager_udp, "--managers", "--manager-udp", (host, tcp_port)
        )

    node_directory = await node_data_directory(
        data_directory,
        config.data.logs_directory,
        "manager",
        datacenter,
        host,
        tcp_port,
    )

    start_timeout_sec = TimeParser(boot_timeout).time
    shutdown_timeout_sec = TimeParser(shutdown_timeout).time

    # D1: the node runs as the identity its Raft store holds, its Raft
    # groups resumed from it when its disk is its own and intact.
    async with opened_raft_store(node_directory, env, datacenter, host, udp_port) as raft_store:
        manager = ManagerServer(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=datacenter,
            gate_addrs=gate_tcp_addresses,
            gate_udp_addrs=gate_udp_addresses,
            seed_managers=peer_tcp_addresses,
            manager_udp_peers=peer_udp_addresses,
            wal_data_dir=node_directory,
            # A restarted manager returns at the same address; its persisted
            # incarnation keeps peers from refusing it as a zombie.
            incarnation_storage_dir=str(node_directory / "incarnation"),
            raft_store=raft_store,
        )

        try:
            await manager.start(timeout=start_timeout_sec)

        except BaseException:
            await manager.abort_and_wait(timeout=shutdown_timeout_sec)
            raise

        try:
            with ShutdownSignals(asyncio.current_task()):
                await manager.wait()

        except (
                KeyboardInterrupt,
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
        ):
                await drain_cluster_membership(manager, shutdown_timeout_sec)
                await manager.abort_and_wait(
                    timeout=shutdown_timeout_sec,
                )
