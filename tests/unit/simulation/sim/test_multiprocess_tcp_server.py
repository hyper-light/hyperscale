"""
Real production TCP stack over the multi-process coordinator.

Two REAL OS processes each run a ``MercurySyncBaseServer`` (the cluster
base every gate/manager/worker node builds on). The client dials the
server through the ``CrossProcessTransport`` stream seam and the whole
production TCP path — encode, compress, encrypt, frame,
``MercurySyncTCPProtocol`` deframe/decrypt/dispatch, response — runs
unchanged over the deterministic boundary: connect costs one round trip
(SYN/ACK analog), the request/response another two legs.

Also asserts refused connects (hosted address, no listener — RST
analog) resolve deterministically, that a server starting *mid-run*
becomes routable at the window it registers in, and byte-identical
replay across two full runs. This unblocks the worker<->manager tier
(registration is TCP) for multi-process SIM.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.tcp_echo_demo import (
    echo_client_entry,
    echo_server_entry,
    refused_client_entry,
)


def _run_echo() -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=2.0)
    coordinator.add_process("server", echo_server_entry, "sim-tcp", 9000, 9001, 0.0)
    coordinator.add_process(
        "client", echo_client_entry, "sim-tcp", 9002, 9003, ("sim-tcp", 9000), 0.0
    )
    return coordinator.run()


def test_real_tcp_roundtrip_over_coordinator():
    results = _run_echo()

    assert results["server"] == [("started", 0.0)]
    # connect = one round trip (2 * 0.01); request + response = another.
    assert results["client"] == [("response", b"tcp-echo:hello", 0.04)]


def test_real_tcp_over_coordinator_is_replay_deterministic():
    assert _run_echo() == _run_echo()


def test_connect_to_hosted_address_without_listener_is_refused():
    """The server hosts its UDP sockname but no stream listener there:
    a dial must come back ``ConnectionRefusedError`` after one round
    trip — the RST analog (a silent drop would hang the caller)."""
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=2.0)
    coordinator.add_process("server", echo_server_entry, "sim-tcp", 9000, 9001, 0.0)
    coordinator.add_process(
        "client", refused_client_entry, ("sim-tcp", 9100), ("sim-tcp", 9001)
    )
    results = coordinator.run()

    assert results["client"] == [("refused", 0.02)]


def test_late_starting_server_becomes_routable_mid_run():
    """The server starts at virtual 5.0 — its listener address reaches
    the coordinator's route map at that window's barrier, and a client
    dialing at 6.0 completes the full production exchange."""
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=10.0)
    coordinator.add_process("server", echo_server_entry, "sim-tcp", 9000, 9001, 5.0)
    coordinator.add_process(
        "client", echo_client_entry, "sim-tcp", 9002, 9003, ("sim-tcp", 9000), 6.0
    )
    results = coordinator.run()

    assert results["server"] == [("started", 5.0)]
    assert results["client"] == [("response", b"tcp-echo:hello", 6.04)]
