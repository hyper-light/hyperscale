"""
Real production UDP stack over the multi-process coordinator.

Two REAL OS processes each run a ``_GreetServer`` (a ``UDPProtocol``
subclass — the same base the worker-pool ``RemoteGraphController`` uses).
The client connects to the server over the ``CrossProcessTransport`` /
``SimulationCoordinator`` boundary and issues one ``greet`` request; the
whole production path — AES-GCM encryption, zstd compression, pickling,
the node-id connect handshake, the request/reply waiter — runs unchanged.

Asserts virtual-time coherence (connect completes after one round-trip,
the reply after another) and byte-identical replay across two full runs.
This is the proof the worker-pool stack is SIM-compatible with the
multi-process topology preserved.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.udp_demo_server import (
    greet_client_entry,
    greet_server_entry,
)


def _run_greet() -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=30.0)
    coordinator.add_process("server", greet_server_entry, "sim", 1)
    coordinator.add_process("client", greet_client_entry, "sim", 2, ("sim", 1))
    return coordinator.run()


def test_real_udp_server_handshake_over_coordinator():
    results = _run_greet()

    assert results["server"] == [("started", 0.0)]
    # connect handshake = one round-trip (2 * 0.01); reply = another.
    assert results["client"] == [
        ("connected", 0.02),
        ("response", "hello-world", 0.04),
    ]


def test_real_udp_over_coordinator_is_replay_deterministic():
    assert _run_greet() == _run_greet()
