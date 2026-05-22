from __future__ import annotations

import asyncio
import os
import signal
import sys
from typing import Literal

try:
    import uvloop

    uvloop.install()

except Exception:
    pass

from hyperscale.distributed.env import Env
from hyperscale.distributed.env.load_env import load_env
from hyperscale.distributed.env.time_parser import TimeParser

from .cli import (
    CLI,
    AssertPath,
    AssertSet,
)


NodeRoleName = Literal["gate", "manager", "worker"]


# ---------------------------------------------------------------------------
# Locator parsing (AD-52 §2).
#
# The full seed-locator package lives at hyperscale/distributed/cluster/
# seed_locators/ and supports all five AD-52 §2 schemes (tcp, dns,
# dns-srv, file, exec). The serve command parses URIs into the AD-52
# locator package for the cluster-module path; for the legacy adapter
# path (until the cluster module is fully wired through the transport
# layer) it still resolves tcp:// + dns:// to (host, port) tuples
# in-process.
# ---------------------------------------------------------------------------

_WIRED_SCHEMES = ("tcp://", "dns://")
_UNWIRED_SCHEMES = ("dns-srv://", "file://", "exec://")


def _parse_locator(locator_uri: str) -> tuple[str, int]:
    locator_uri = locator_uri.strip()
    if not locator_uri:
        raise ValueError("empty locator")

    matched_scheme: str | None = None
    for scheme in _WIRED_SCHEMES:
        if locator_uri.startswith(scheme):
            matched_scheme = scheme
            break

    if matched_scheme is None:
        for scheme in _UNWIRED_SCHEMES:
            if locator_uri.startswith(scheme):
                raise ValueError(
                    f"locator scheme {scheme!r} is declared in AD-52 §2 but its "
                    f"resolver has not been wired yet; hyperscale/distributed/cluster/"
                    f"seed_locators/ ships with AD-52 Phase 1. Use tcp:// or dns:// "
                    f"for now, or wait for the locator package."
                )
        raise ValueError(
            f"unsupported locator scheme in {locator_uri!r}; "
            f"AD-52 §2 defines tcp://, dns://, dns-srv://, file://, exec://"
        )

    address_part = locator_uri[len(matched_scheme):]
    host_part, separator, port_part = address_part.rpartition(":")
    if not separator or not host_part or not port_part:
        raise ValueError(
            f"locator {locator_uri!r} must include an explicit port per AD-52 §2"
        )

    try:
        port_number = int(port_part)
    except ValueError as conversion_error:
        raise ValueError(
            f"locator {locator_uri!r} has a non-integer port"
        ) from conversion_error

    if port_number < 1 or port_number > 65535:
        raise ValueError(f"locator {locator_uri!r} port out of range")

    return host_part, port_number


def _parse_locator_list(comma_separated: str) -> list[tuple[str, int]]:
    if not comma_separated:
        return []

    return [
        _parse_locator(entry)
        for entry in comma_separated.split(",")
        if entry.strip()
    ]


def _parse_initial_members(
    comma_separated: str,
) -> list[tuple[str, tuple[str, int]]]:
    """
    Parse AD-52 §4 --initial-members string of form
        node_id_1@locator_1,node_id_2@locator_2,...
    Returns a list of (founding_node_id, resolved_address) pairs preserving
    the operator-supplied ordering.
    """
    if not comma_separated:
        return []

    members: list[tuple[str, tuple[str, int]]] = []
    for entry in comma_separated.split(","):
        entry = entry.strip()
        if not entry:
            continue
        founding_node_id, separator, locator_uri = entry.partition("@")
        if not separator or not founding_node_id or not locator_uri:
            raise ValueError(
                f"--initial-members entry {entry!r} must be in the "
                f"node_id@locator form per AD-52 §4"
            )
        members.append((founding_node_id, _parse_locator(locator_uri)))

    return members


# ---------------------------------------------------------------------------
# Listen-address parsing.
# AD-52 §21 declares one TCP listen address per node. The legacy server
# implementations also expose a UDP SWIM port; until AD-52 §8's
# SWIM+phi-accrual rewrite lands, --swim-port keeps the legacy SWIM
# socket configurable. Default UDP port = TCP port + 1 by convention.
# ---------------------------------------------------------------------------


def _parse_listen_address(listen_address: str) -> tuple[str, int]:
    host_part, separator, port_part = listen_address.rpartition(":")
    if not separator or not host_part or not port_part:
        raise ValueError(
            f"--listen-address {listen_address!r} must be host:port"
        )
    try:
        port_number = int(port_part)
    except ValueError as conversion_error:
        raise ValueError(
            f"--listen-address {listen_address!r} has a non-integer port"
        ) from conversion_error
    if port_number < 1 or port_number > 65535:
        raise ValueError(f"--listen-address {listen_address!r} port out of range")
    return host_part, port_number


# ---------------------------------------------------------------------------
# AD-52 §21 config → process env vars.
# Stashes the AD-52-shaped flag values under HYPERSCALE_CLUSTER_* env vars
# so the cluster module reads them once AD-52 Phase 1 lands. This is the
# stable forward-compatible bridge: today's launch wires the legacy
# server; tomorrow's launch (same flags, same Helm chart) wires the
# AD-52 cluster module without any operator-visible change.
# ---------------------------------------------------------------------------


_AD52_FLAGS_TO_ENV: dict[str, str] = {
    "cluster_id": "HYPERSCALE_CLUSTER_ID",
    "role": "HYPERSCALE_CLUSTER_ROLE",
    "mtls_cert": "HYPERSCALE_CLUSTER_MTLS_CERT",
    "mtls_key": "HYPERSCALE_CLUSTER_MTLS_KEY",
    "mtls_ca": "HYPERSCALE_CLUSTER_MTLS_CA",
    "listen_address": "HYPERSCALE_CLUSTER_LISTEN_ADDRESS",
    "advertised_address": "HYPERSCALE_CLUSTER_ADVERTISED_ADDRESS",
    "initial_members": "HYPERSCALE_CLUSTER_INITIAL_MEMBERS",
    "cluster_size": "HYPERSCALE_CLUSTER_SIZE",
    "seeds": "HYPERSCALE_CLUSTER_SEEDS",
    "manager_seeds": "HYPERSCALE_CLUSTER_MANAGER_SEEDS",
    "verify_advertised_address": "HYPERSCALE_CLUSTER_VERIFY_ADVERTISED_ADDRESS",
    "max_seed_candidates": "HYPERSCALE_CLUSTER_MAX_SEED_CANDIDATES",
    "seed_refresh_interval": "HYPERSCALE_CLUSTER_SEED_REFRESH_INTERVAL",
    "bootstrap_window": "HYPERSCALE_CLUSTER_BOOTSTRAP_WINDOW",
    "learner_promote_threshold": "HYPERSCALE_CLUSTER_LEARNER_PROMOTE_THRESHOLD",
    "learner_max_lifetime": "HYPERSCALE_CLUSTER_LEARNER_MAX_LIFETIME",
    "tombstone_retention": "HYPERSCALE_CLUSTER_TOMBSTONE_RETENTION",
    "disconnected_mode_threshold": "HYPERSCALE_CLUSTER_DISCONNECTED_MODE_THRESHOLD",
    "leader_lease_enabled": "HYPERSCALE_CLUSTER_LEADER_LEASE_ENABLED",
    "snapshot_interval_entries": "HYPERSCALE_CLUSTER_SNAPSHOT_INTERVAL_ENTRIES",
    "max_inflight_appendentries": "HYPERSCALE_CLUSTER_MAX_INFLIGHT_APPENDENTRIES",
    "federation_quorum": "HYPERSCALE_CLUSTER_FEDERATION_QUORUM",
    "datacenter_id": "HYPERSCALE_CLUSTER_DATACENTER_ID",
}


def _export_ad52_flags(flag_values: dict[str, str]) -> None:
    for flag_name, env_name in _AD52_FLAGS_TO_ENV.items():
        flag_value = flag_values.get(flag_name)
        if flag_value is None or flag_value == "":
            continue
        os.environ[env_name] = str(flag_value)


def _validate_durations(*duration_strings: str) -> None:
    for duration_string in duration_strings:
        if not duration_string:
            continue
        TimeParser(duration_string)


def _install_signal_handlers(
    shutdown_event: asyncio.Event,
    running_loop: asyncio.AbstractEventLoop,
) -> None:
    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            running_loop.add_signal_handler(
                handled_signal,
                shutdown_event.set,
            )
        except NotImplementedError:
            # Windows / restricted envs: fall back to default Python handling.
            # asyncio.run() converts KeyboardInterrupt into CancelledError on
            # the main task, which our serve() body catches anyway.
            pass


# ---------------------------------------------------------------------------
# Role adapters.
#
# Each adapter constructs the legacy server with arguments derived from the
# AD-52 §21 flag surface. The adapters are intentionally thin — they
# translate the operator-facing config into today's constructor shape and
# nothing more. They will collapse to a single
# `from hyperscale.distributed.cluster import ClusterNode` line once AD-52
# Phase 1 lands.
# ---------------------------------------------------------------------------


async def _run_gate(
    listen_host: str,
    listen_port: int,
    swim_port: int,
    env: Env,
    datacenter_id: str,
    seed_addresses: list[tuple[str, int]],
    federation_quorum: int,
) -> None:
    from hyperscale.distributed.nodes.gate.server import GateServer

    # AD-52 §17 federation: managers register themselves with the gate
    # cluster via RegisterDatacenter at their own bootstrap completion.
    # Until that path lands, the legacy datacenter_managers map starts
    # empty and is populated dynamically as managers register.
    gate_server = GateServer(
        host=listen_host,
        tcp_port=listen_port,
        udp_port=swim_port,
        env=env,
        dc_id=datacenter_id or "global",
        datacenter_managers={},
        datacenter_manager_udp={},
        gate_peers=seed_addresses,
        gate_udp_peers=[],
    )

    if federation_quorum > 0:
        # Stash for AD-52 §17 / AD-33 federated health monitor consumers.
        os.environ["HYPERSCALE_CLUSTER_FEDERATION_QUORUM"] = str(federation_quorum)

    await _run_server_lifecycle(gate_server)


async def _run_manager(
    listen_host: str,
    listen_port: int,
    swim_port: int,
    env: Env,
    datacenter_id: str,
    seed_addresses: list[tuple[str, int]],
    gate_seed_addresses: list[tuple[str, int]],
) -> None:
    from hyperscale.distributed.nodes.manager.server import ManagerServer

    manager_server = ManagerServer(
        host=listen_host,
        tcp_port=listen_port,
        udp_port=swim_port,
        env=env,
        dc_id=datacenter_id or "default",
        gate_addrs=gate_seed_addresses,
        gate_udp_addrs=[],
        seed_managers=seed_addresses,
        manager_udp_peers=[],
    )

    await _run_server_lifecycle(manager_server)


async def _run_worker(
    listen_host: str,
    listen_port: int,
    swim_port: int,
    env: Env,
    datacenter_id: str,
    manager_seed_addresses: list[tuple[str, int]],
) -> None:
    from hyperscale.distributed.nodes.worker.server import WorkerServer

    worker_server = WorkerServer(
        host=listen_host,
        tcp_port=listen_port,
        udp_port=swim_port,
        env=env,
        dc_id=datacenter_id or "default",
        seed_managers=manager_seed_addresses,
    )

    await _run_server_lifecycle(worker_server)


async def _run_server_lifecycle(server: object) -> None:
    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event, asyncio.get_running_loop())

    start_method = getattr(server, "start")
    stop_method = getattr(server, "stop")

    server_task = asyncio.create_task(start_method())

    try:
        shutdown_wait_task = asyncio.create_task(shutdown_event.wait())
        done_set, pending_set = await asyncio.wait(
            {server_task, shutdown_wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if server_task in done_set:
            # Server returned (or crashed) on its own.
            for pending_task in pending_set:
                pending_task.cancel()
            await server_task
            return

        # Shutdown requested.
        shutdown_wait_task.cancel()
        await stop_method()
        await server_task

    except asyncio.CancelledError:
        await stop_method()
        raise


# ---------------------------------------------------------------------------
# CLI entrypoint.
# ---------------------------------------------------------------------------


@CLI.command()
async def serve(
    role: AssertSet[NodeRoleName],
    cluster_id: str,
    mtls_cert: AssertPath,
    mtls_key: AssertPath,
    mtls_ca: AssertPath,
    listen_address: str,
    initial_members: str = "",
    cluster_size: int = 0,
    seeds: str = "",
    manager_seeds: str = "",
    gate_seeds: str = "",
    datacenter_id: str = "",
    advertised_address: str = "",
    verify_advertised_address: bool = False,
    max_seed_candidates: int = 64,
    seed_refresh_interval: str = "60s",
    bootstrap_window: str = "5s",
    learner_promote_threshold: int = 256,
    learner_max_lifetime: str = "30m",
    tombstone_retention: str = "10m",
    disconnected_mode_threshold: str = "30s",
    leader_lease_enabled: bool = False,
    snapshot_interval_entries: int = 10000,
    max_inflight_appendentries: int = 256,
    swim_port: int = 0,
    federation_quorum: int = 0,
    env_file: str = "",
):
    """
    Run a hyperscale node — gate, manager, or worker.

    Implements the AD-52 §21 operator-facing configuration surface. Locator
    parsing currently supports tcp:// and dns:// schemes; dns-srv://,
    file://, and exec:// are reserved for AD-52 Phase 1 and rejected
    explicitly so operators receive clear errors rather than silent
    fallbacks.

    @param role One of "gate", "manager", "worker".
    @param cluster_id Human-readable cluster identifier (e.g. "prod-uswest-managers").
    @param mtls_cert Path to PEM cert chain.
    @param mtls_key Path to PEM private key.
    @param mtls_ca Path to PEM CA bundle for cluster admission.
    @param listen_address Listen address as host:port for cluster TCP.
    @param initial_members Comma-separated node_id@locator list for cold bootstrap (founding nodes only).
    @param cluster_size Number of founding members; must match initial_members length when set.
    @param seeds Comma-separated locator list for joining an existing cluster (gate, manager).
    @param manager_seeds Comma-separated locator list for finding managers (worker-only).
    @param gate_seeds Comma-separated locator list for finding gates (manager-only; AD-52 §17 federation).
    @param datacenter_id Datacenter identifier. Defaults to "global" for gates and "default" otherwise.
    @param advertised_address Address peers should use to reach this node, if different from listen_address.
    @param verify_advertised_address Opt-in reverse-probe verification of advertised_address.
    @param max_seed_candidates Bound on locator resolution fan-out.
    @param seed_refresh_interval Re-resolution cadence as a duration like "60s".
    @param bootstrap_window Bootstrap stabilization window as a duration like "5s".
    @param learner_promote_threshold Max log lag (entries) at which a learner can be promoted.
    @param learner_max_lifetime Learner timeout before eviction as a duration like "30m".
    @param tombstone_retention DEAD-to-REMOVE delay as a duration like "10m".
    @param disconnected_mode_threshold Watch disconnect to disconnected-mode threshold as a duration like "30s".
    @param leader_lease_enabled Opt-in leader leases for fast reads (requires bounded clock skew).
    @param snapshot_interval_entries Raft snapshot cadence in log entries.
    @param max_inflight_appendentries Per-follower replication window.
    @param swim_port Legacy SWIM UDP port. Defaults to listen-port + 1.
    @param federation_quorum Minimum DCs that must be reachable (gate-only, informational).
    @param env_file Path to a .env file for additional environment overrides.
    """

    role_name = role.data
    cert_path = mtls_cert.data
    key_path = mtls_key.data
    ca_path = mtls_ca.data

    try:
        listen_host, listen_port = _parse_listen_address(listen_address)
    except ValueError as parse_error:
        sys.stderr.write(f"hyperscale serve: {parse_error}\n")
        sys.exit(2)

    resolved_swim_port = swim_port if swim_port > 0 else listen_port + 1

    try:
        _validate_durations(
            seed_refresh_interval,
            bootstrap_window,
            learner_max_lifetime,
            tombstone_retention,
            disconnected_mode_threshold,
        )
    except Exception as duration_error:
        sys.stderr.write(
            f"hyperscale serve: invalid duration in flags — {duration_error}\n"
        )
        sys.exit(2)

    try:
        seed_addresses = _parse_locator_list(seeds)
        manager_seed_addresses = _parse_locator_list(manager_seeds)
        gate_seed_addresses = _parse_locator_list(gate_seeds)
        initial_member_pairs = _parse_initial_members(initial_members)
    except ValueError as locator_error:
        sys.stderr.write(f"hyperscale serve: {locator_error}\n")
        sys.exit(2)

    if cluster_size > 0 and initial_member_pairs and len(initial_member_pairs) != cluster_size:
        sys.stderr.write(
            f"hyperscale serve: --cluster-size={cluster_size} does not match "
            f"--initial-members length={len(initial_member_pairs)} (AD-52 §4)\n"
        )
        sys.exit(2)

    if role_name == "worker" and not manager_seed_addresses:
        sys.stderr.write(
            "hyperscale serve: workers require --manager-seeds per AD-52 §12\n"
        )
        sys.exit(2)

    if role_name in ("gate", "manager") and not (initial_member_pairs or seed_addresses):
        sys.stderr.write(
            f"hyperscale serve: {role_name} nodes require either --initial-members "
            f"(cold bootstrap) or --seeds (join existing cluster) per AD-52 §§4-5\n"
        )
        sys.exit(2)

    _export_ad52_flags(
        {
            "cluster_id": cluster_id,
            "role": role_name,
            "mtls_cert": cert_path or "",
            "mtls_key": key_path or "",
            "mtls_ca": ca_path or "",
            "listen_address": listen_address,
            "advertised_address": advertised_address,
            "initial_members": initial_members,
            "cluster_size": cluster_size,
            "seeds": seeds,
            "manager_seeds": manager_seeds,
            "datacenter_id": datacenter_id,
            "verify_advertised_address": "1" if verify_advertised_address else "0",
            "max_seed_candidates": max_seed_candidates,
            "seed_refresh_interval": seed_refresh_interval,
            "bootstrap_window": bootstrap_window,
            "learner_promote_threshold": learner_promote_threshold,
            "learner_max_lifetime": learner_max_lifetime,
            "tombstone_retention": tombstone_retention,
            "disconnected_mode_threshold": disconnected_mode_threshold,
            "leader_lease_enabled": "1" if leader_lease_enabled else "0",
            "snapshot_interval_entries": snapshot_interval_entries,
            "max_inflight_appendentries": max_inflight_appendentries,
            "federation_quorum": federation_quorum,
        }
    )

    env = load_env(Env, env_file=env_file or None)

    if role_name == "gate":
        await _run_gate(
            listen_host=listen_host,
            listen_port=listen_port,
            swim_port=resolved_swim_port,
            env=env,
            datacenter_id=datacenter_id,
            seed_addresses=seed_addresses,
            federation_quorum=federation_quorum,
        )
    elif role_name == "manager":
        await _run_manager(
            listen_host=listen_host,
            listen_port=listen_port,
            swim_port=resolved_swim_port,
            env=env,
            datacenter_id=datacenter_id,
            seed_addresses=seed_addresses,
            gate_seed_addresses=gate_seed_addresses,
        )
    elif role_name == "worker":
        await _run_worker(
            listen_host=listen_host,
            listen_port=listen_port,
            swim_port=resolved_swim_port,
            env=env,
            datacenter_id=datacenter_id,
            manager_seed_addresses=manager_seed_addresses,
        )
    else:
        sys.stderr.write(
            f"hyperscale serve: unreachable role {role_name!r} — "
            "this should have been rejected at parse time\n"
        )
        sys.exit(2)
