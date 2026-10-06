import asyncio
from hyperscale.commands.cli import JsonFile, command, AssertSet
from hyperscale.distributed.nodes import GateServer
from hyperscale.core.engines.client.time_parser import TimeParser

from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.logging import Logger, LoggingConfig, LogLevelName
from hyperscale.ui.node_dashboard import (
    GateDashboardReader,
    NodeDashboard,
    NodeDashboardConfig,
    StderrLogRedirect,
)

from .node_address import parse_node_host, parse_peer_addresses
from .node_lifecycle import run_node_until_stopped
from .seed_locators import is_dynamic_locator, resolve_cohort_addresses
from .shared import (
    opened_raft_store,
    get_default_config,
    node_data_directory,
    node_env,
    node_log_path,
    node_terminal_mode,
    resolve_auth_secret,
)


@command(
    display_help_on_error=False,
    shortnames={
        "host": "H",
        "gates": "g",
        "gate_udp": "G",
        "data_directory": "D",
        "leader_lease_enabled": "L",
    },
)
async def gate(
    host: str = "127.0.0.1",
    tcp_port: int = 8431,
    udp_port: int = 8441,
    datacenter: str = 'global',
    boot_timeout: str = '5m',
    shutdown_timeout: str = "1m",
    acm_secret: str | None = None,
    data_directory: str | None = None,
    gates: list[str] = [],
    gate_udp: list[str] = [],
    cohort_size: int | None = None,
    leader_lease_enabled: bool = False,
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
    quiet: bool = False,
):
    """
    Run a Hyperscale gate. The gate tier's peers are listed at boot with
    --gates/--gate-udp; datacenter managers started with --gates report to
    it, and `hyperscale join` connects managers at runtime.

    @param host The address to bind and advertise to peers (an IP, or a stable DNS name such as a StatefulSet pod's)
    @param tcp_port The TCP port for data operations
    @param udp_port The UDP port for SWIM health checks
    @param datacenter The gate's datacenter identifier
    @param boot_timeout How long to wait for the gate to boot
    @param shutdown_timeout How long to wait for the gate to shut down
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET, else the per-user cluster cookie)
    @param data_directory Where the gate keeps its durable state (defaults to a directory per gate under data/ beside the logs directory)
    @param gates The TCP host:port of every gate in the tier (this gate's own entry is skipped)
    @param gate_udp The UDP host:port of the same gates, in the same order
    @param cohort_size How many members the gate cohort has -- required when --gates gives seed locators (dns://, dns-srv://, file://, exec://), which resolve at launch until they yield exactly this many
    @param leader_lease_enabled Serve linearizable cluster reads from the leader's lease instead of a quorum round each (AD-52 section 11) -- only where every node's clock is NTP-synchronized, and on every node of the cluster alike
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use
    @param quiet If specified, the live node dashboard is disabled
    """
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    host = parse_node_host(host, tcp_port)

    env = node_env(
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
        # Given only when set, so an operator's exported setting stands.
        **({"RAFT_LEADER_LEASES_ENABLED": True} if leader_lease_enabled else {}),
    )

    # AD-52 section 2: seed locators resolve at launch, to exactly the
    # cohort size every founder agrees on.
    if any(is_dynamic_locator(entry) for entry in [*gates, *gate_udp]):
        peer_tcp_addresses, peer_udp_addresses = await resolve_cohort_addresses(
            gates,
            gate_udp,
            "--gates",
            "--gate-udp",
            (host, tcp_port),
            cohort_size,
            TimeParser(boot_timeout).time,
            env.CLUSTER_FORMATION_INTERVAL_SECONDS,
            env.GATE_TCP_TIMEOUT_STANDARD,
        )
    else:
        peer_tcp_addresses, peer_udp_addresses = parse_peer_addresses(
            gates, gate_udp, "--gates", "--gate-udp", (host, tcp_port)
        )

    node_directory = await node_data_directory(
        data_directory,
        config.data.logs_directory,
        "gate",
        datacenter,
        host,
        tcp_port,
    )

    start_timeout_sec = TimeParser(boot_timeout).time
    shutdown_timeout_sec = TimeParser(shutdown_timeout).time

    terminal_mode = await node_terminal_mode(config.data.terminal_mode, quiet)
    log_path = node_log_path(config.data.logs_directory, "gate", datacenter, host, tcp_port)

    # While the dashboard renders, stderr (the node's logs) goes to the log
    # file -- redirected before the Raft store opens its first log stream.
    async with StderrLogRedirect(log_path, enabled=terminal_mode != "disabled"):
        # D1: the node runs as the identity its Raft store holds, its Raft
        # groups resumed from it when its disk is its own and intact.
        async with opened_raft_store(node_directory, env, datacenter, host, udp_port) as raft_store:
            gate = GateServer(
                host=host,
                tcp_port=tcp_port,
                udp_port=udp_port,
                env=env,
                dc_id=datacenter,
                gate_peers=peer_tcp_addresses,
                gate_udp_peers=peer_udp_addresses,
                wal_data_dir=node_directory,
                incarnation_storage_dir=str(node_directory / "incarnation"),
                raft_store=raft_store,
            )

            await run_node_until_stopped(
                gate,
                # GateServer.start takes no timeout of its own; bound the boot
                # here so --boot-timeout means the same thing for every role.
                lambda: asyncio.wait_for(gate.start(), timeout=start_timeout_sec),
                NodeDashboard(
                    GateDashboardReader(gate),
                    gate,
                    terminal_mode,
                    env,
                    log_path,
                    NodeDashboardConfig(),
                    Logger(),
                ),
                drain_before_stop=True,
                shutdown_timeout_seconds=shutdown_timeout_sec,
            )
