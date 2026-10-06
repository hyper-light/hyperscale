"""
Transport interface — typing-only seam describing the contract
consumers depend on for inter-node messaging.

``MercurySyncBaseServer.send_tcp`` and ``send_udp`` already match this
Protocol structurally; the Phase 4 test harness at
``tests/simulation/harness/fault_transport.py`` wraps the same bound
methods to inject network faults. Phase 5 introduces no runtime
change for transport — this Protocol exists so consumer classes
(``WorkflowDispatcher``, retry helpers, SWIM gossip) can type-hint
against the seam rather than the concrete server class, and so Phase
6's ``InProcessTransport`` can satisfy the same contract.

Out of scope: ``loop.create_connection``, ``loop.create_server``,
``loop.create_datagram_endpoint``, raw sockets. These are internal
to ``MercurySyncBaseServer`` and the Phase 4 harness already proved
``send_tcp`` / ``send_udp`` is the right intercept point.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    # ``models`` imports the runtime seams; a runtime import back would cycle.
    from hyperscale.distributed.models.message import Message


class Transport(Protocol):
    """Send a request and await a framed response over TCP or UDP.

    Return type matches the existing
    ``MercurySyncBaseServer.send_tcp`` / ``send_udp`` signature: a
    ``(response, clock_time)`` pair where ``response`` is the framed
    reply bytes or the ``Exception`` that ended the request (returned,
    never raised). A ``Message`` is serialized before it is framed. The
    clock_time is the receiver's logical clock at the moment the reply
    was produced.
    """

    async def send_tcp(
        self,
        address: tuple[str, int],
        action: str,
        data: "bytes | Message",
        timeout: int | float | None = None,
    ) -> tuple[bytes | Exception, int]:
        """Send a TCP request and await the framed response."""
        ...

    async def send_udp(
        self,
        address: tuple[str, int],
        action: str,
        data: "bytes | Message",
        timeout: int | float | None = None,
    ) -> tuple[bytes | Exception, int]:
        """Send a UDP request and await the framed response."""
        ...
