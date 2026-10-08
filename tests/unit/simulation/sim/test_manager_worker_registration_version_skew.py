"""
AD-25 at worker registration, on real managers on virtual time.

A manager negotiated a protocol version with gates and clients but not
with workers: it never read a registering worker's MAJOR version, so a
worker of another MAJOR version was admitted ("Different MAJOR: reject
connection"), and both registration-response builders sent
``capabilities=""``, so a worker negotiated nothing.

A three-manager datacenter (``manager_datacenter``); workers register
through the leader's own ``worker_register`` handler:

* a worker of another MAJOR version is refused, naming the version, and
  is not admitted to the worker pool;
* a worker of this MAJOR version, newer MINOR, is admitted, and the
  response carries the features both sides name -- the worker's newer
  features are ignored.
"""

import sys

import cloudpickle

from hyperscale.distributed.models import NodeInfo, NodeRole, RegistrationResponse, WorkerRegistration
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    DATACENTER,
    HOST,
    MANAGER_TCP_ADDRESSES,
    ManagerBuilder,
    form_datacenter,
    run_scenario,
)

cloudpickle.register_pickle_by_value(sys.modules[__name__])

NEWER_MINOR_ONLY_FEATURE = "feature_from_a_newer_minor"


def registration_of(
    leader: ManagerServer,
    node_id: str,
    tcp_port: int,
    major: int,
    minor: int,
    capabilities: str,
) -> bytes:
    return WorkerRegistration(
        node=NodeInfo(
            node_id=node_id,
            role=NodeRole.WORKER.value,
            host=HOST,
            port=tcp_port,
            datacenter=DATACENTER,
            udp_port=tcp_port + 1,
        ),
        total_cores=4,
        available_cores=4,
        memory_mb=1024,
        cluster_id=leader._config.cluster_id,
        environment_id=leader._config.environment_id,
        protocol_version_major=major,
        protocol_version_minor=minor,
        capabilities=capabilities,
    ).dump()


def test_worker_registration_negotiates_the_protocol_version() -> None:
    async def scenario(
        managers: list[ManagerServer], _build_manager: ManagerBuilder
    ) -> tuple[RegistrationResponse, RegistrationResponse, set[str]]:
        leader = await form_datacenter(managers, link)
        other_major_response = RegistrationResponse.load(
            await leader.worker_register(
                (HOST, 9200),
                registration_of(
                    leader,
                    "worker-other-major",
                    9200,
                    CURRENT_PROTOCOL_VERSION.major + 1,
                    0,
                    "cancellation",
                ),
                0,
            )
        )
        newer_minor_response = RegistrationResponse.load(
            await leader.worker_register(
                (HOST, 9300),
                registration_of(
                    leader,
                    "worker-newer-minor",
                    9300,
                    CURRENT_PROTOCOL_VERSION.major,
                    CURRENT_PROTOCOL_VERSION.minor + 1,
                    f"cancellation,heartbeat,{NEWER_MINOR_ONLY_FEATURE}",
                ),
                0,
            )
        )
        admitted_worker_ids = {worker.node.node_id for worker in leader._manager_state.get_all_workers().values()}
        return other_major_response, newer_minor_response, admitted_worker_ids

    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)
    other_major_response, newer_minor_response, admitted_worker_ids = run_scenario(scenario, link)

    assert other_major_response.accepted is False
    assert other_major_response.error == f"Incompatible protocol version: {CURRENT_PROTOCOL_VERSION.major + 1}.0"
    assert "worker-other-major" not in admitted_worker_ids

    assert newer_minor_response.accepted is True, newer_minor_response.error
    assert set(newer_minor_response.capabilities.split(",")) == {"cancellation", "heartbeat"}
    assert (newer_minor_response.protocol_version_major, newer_minor_response.protocol_version_minor) == (
        CURRENT_PROTOCOL_VERSION.major,
        CURRENT_PROTOCOL_VERSION.minor,
    )
    assert "worker-newer-minor" in admitted_worker_ids
