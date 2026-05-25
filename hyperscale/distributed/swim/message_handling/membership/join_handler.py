"""
Handler for JOIN messages.
"""

import time
from typing import ClassVar

from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed.swim.core.audit import AuditEventType
from hyperscale.distributed.swim.message_handling.models import (
    MessageContext,
    HandlerResult,
    ServerInterface,
)
from hyperscale.distributed.swim.message_handling.core import BaseHandler


# SWIM protocol version prefix (included in join messages)
SWIM_VERSION_PREFIX = (
    f"v{CURRENT_PROTOCOL_VERSION.major}.{CURRENT_PROTOCOL_VERSION.minor}".encode()
)


class JoinHandler(BaseHandler):
    """
    Handles JOIN messages.

    Processes new nodes joining the cluster:
    - Validates protocol version (AD-25)
    - Clears stale state
    - Propagates join to other nodes
    - Adds to probe scheduler
    """

    message_types: ClassVar[tuple[bytes, ...]] = (b"join",)

    def __init__(self, server: ServerInterface) -> None:
        super().__init__(server)

    async def handle(self, context: MessageContext) -> HandlerResult:
        """Handle a join message."""
        self._server.increment_metric("joins_received")

        source_addr = context.source_addr
        target_addr_bytes = context.target_addr_bytes
        # Capture the unparsed payload BEFORE _parse_join_message
        # rewrites ``target_addr_bytes`` to just the host:port portion.
        # The trailing ``|i:{inc}`` field needed for the rejoin
        # zombie-check fix lives in this original blob.
        original_target_addr_bytes = target_addr_bytes

        # Parse version, role and target from join message
        version, role, target, target_addr_bytes = self._parse_join_message(
            context.target, target_addr_bytes
        )

        # Validate protocol version (AD-25)
        if version is None:
            self._server.increment_metric("joins_rejected_no_version")
            return self._nack(b"version_required")

        if version[0] != CURRENT_PROTOCOL_VERSION.major:
            self._server.increment_metric("joins_rejected_version_mismatch")
            return self._nack(b"version_mismatch")

        # Validate target
        if not await self._server.validate_target(target, b"join", source_addr):
            return self._nack()

        # Handle self-join
        if self._server.udp_target_is_self(target):
            return self._ack(embed_state=False)

        # Record the joining node's role immediately so leader-election
        # cohort and role-aware probe scheduling don't have to wait for
        # gossip to propagate role info. The join is authoritative for
        # role: the joining node tells us what it is.
        if role and target is not None:
            self._server.record_peer_role(target, role)

        async with await self._server.context_with_value(target):
            nodes = self._server.read_nodes()
            is_rejoin = target in nodes

            incarnation_tracker = self._server.incarnation_tracker
            # Prefer the joiner's live incarnation (carried in the
            # ``|i:{inc}`` trailer) over the receiver's stale tracker
            # view. Without this, a node that peers have marked DEAD
            # but is now alive cannot rejoin: the receiver compares
            # its own DEAD-marked incarnation against itself, fails
            # the zombie check, and rejects the rejoin — the recovery
            # path then never fires.
            tracker_incarnation = incarnation_tracker.get_node_incarnation(target)
            sent_incarnation = self._parse_claimed_incarnation(
                original_target_addr_bytes
            )
            claimed_incarnation = (
                sent_incarnation
                if sent_incarnation is not None
                else tracker_incarnation
            )

            if is_rejoin and incarnation_tracker.is_potential_zombie(
                target, claimed_incarnation
            ):
                authorized_rejoin_incarnation = (
                    await self._server.authorize_rejoin_reset(
                        target,
                        role,
                        source_addr,
                    )
                )
                if authorized_rejoin_incarnation is not None:
                    sent_incarnation = max(
                        sent_incarnation or 0,
                        authorized_rejoin_incarnation,
                    )
                    claimed_incarnation = sent_incarnation
                else:
                    required_incarnation = (
                        incarnation_tracker.get_required_rejoin_incarnation(target)
                    )
                    self._server.increment_metric("joins_rejected_zombie")
                    self._server.audit_log.record(
                        AuditEventType.NODE_REJOIN,
                        node=target,
                        source=source_addr,
                        extra={
                            "rejected": True,
                            "reason": "potential_zombie",
                            "required_incarnation": required_incarnation,
                        },
                    )
                    return self._nack(b"zombie_rejected")

            # SWIM canonical refutation contract: incarnations are owned
            # by the node they identify. The receiver MUST NOT synthesize
            # an incarnation increase on behalf of the joiner — doing so
            # turns the incarnation into a shared mutable counter and
            # lets stale UDP replays (e.g. a worker's pre-kill startup
            # JOIN delivered after the worker dies) refute live SUSPECT
            # state in the tracker.
            #
            # The joiner is therefore required to carry its current
            # ``self_incarnation`` in the ``|i:{inc}`` trailer
            # (``join_cluster`` already does this and bumps
            # ``self_incarnation`` by ``minimum_rejoin_incarnation_bump
            # + 1`` per call so the claim strictly exceeds any prior
            # tracker view). Without the trailer we have no live claim
            # to compare against — the only sound action is to reject.
            #
            # Once the trailer is present we trust the joiner's claim
            # as authoritative. ``NodeState.update``'s freshness check
            # then naturally handles every case:
            #
            #   * claim > current  → OK transition wins (genuine join /
            #     rejoin / refutation after a real self_incarnation bump)
            #   * claim == current → status priority decides; SUSPECT
            #     and DEAD both outrank OK at equal incarnation, so
            #     stale replays during SUSPECT and zombie replays
            #     post-DEAD are silently dropped (the matching
            #     piggyback-ALIVE gate in
            #     ``health_aware_server._should_apply_alive_piggyback``
            #     closes the gossip side of the same invariant)
            #   * claim < current → freshness check drops it as stale
            if sent_incarnation is None:
                self._server.increment_metric("joins_rejected_no_incarnation")
                return self._nack(b"incarnation_required")

            await self._server.clear_stale_state(target)

            event_type = (
                AuditEventType.NODE_REJOIN if is_rejoin else AuditEventType.NODE_JOINED
            )
            self._server.audit_log.record(
                event_type,
                node=target,
                source=source_addr,
            )

            await self._server.write_context(target, b"OK")

            self._queue_join_propagation(
                target, role, target_addr_bytes, sent_incarnation
            )

            self._server.probe_scheduler.add_member(target)

            await self._server.confirm_peer(source_addr)
            await self._server.confirm_peer(target)
            # JOIN is the SWIM-level registration handshake. Mark the
            # joining peer (and the source if different — for forwarded
            # joins the source is the propagator, but it too is a
            # registered cluster member by definition) as registered
            # so the ``start_suspicion`` registration gate clears.
            self._server.register_peer(target)
            self._server.register_peer(source_addr)

            # Apply the joiner's authoritative incarnation directly.
            # No receiver-side ``current + 1`` synthesis: see the
            # SWIM-canonical-refutation block above. The zombie check
            # (lines 103-120) has already verified the claim is high
            # enough above any prior DEAD record, and
            # ``NodeState.update``'s freshness check handles the
            # SUSPECT/OK priority ordering at equal incarnation. Route
            # through the server's ``update_node_state`` rather than
            # the incarnation tracker directly so the DEAD→OK
            # transition fires ``_on_node_join_callbacks``.
            await self._server.update_node_state(
                target, b"OK", sent_incarnation, time.monotonic()
            )

            incarnation_tracker.clear_death_record(target)

            # Always fire the join callbacks: the join message
            # semantically means "add me back as a peer". The
            # DEAD→OK gate inside ``update_node_state`` only fires
            # callbacks when the receiver's tracker still records the
            # joiner as DEAD; gossip-propagated joins and probe-ACK
            # updates can flip the tracker back to OK before the join
            # arrives, leaving downstream observers (manager peer-
            # recovery handler, etc.) un-notified. Firing here makes
            # rejoin handling deterministic.
            self._server.notify_node_join(target)

            return self._ack()

    def _parse_join_message(
        self,
        target: tuple[str, int] | None,
        target_addr_bytes: bytes | None,
    ) -> tuple[
        tuple[int, int] | None,
        str | None,
        tuple[str, int] | None,
        bytes | None,
    ]:
        """
        Parse version, role and target from join message.

        Format: v{major}.{minor}|{role}|host:port (current)
        or:     v{major}.{minor}|host:port (legacy, role omitted)

        Returns:
            Tuple of (version, role, target, addr_part_bytes). ``role``
            is None when the sender used the legacy 2-field format —
            receivers must tolerate it for rolling upgrades.
        """
        if not target_addr_bytes or b"|" not in target_addr_bytes:
            return (None, None, target, target_addr_bytes)

        # Capture up to 4 fields: version | role | host:port | i:{incarnation}
        # (older 3-field and legacy 2-field encodings still parse cleanly).
        parts = target_addr_bytes.split(b"|", maxsplit=3)

        # Always parse version from the first segment.
        version_part = parts[0]
        version: tuple[int, int] | None = None
        if version_part.startswith(b"v"):
            try:
                version_str = version_part[1:].decode()
                version_components = version_str.split(".")
                if len(version_components) == 2:
                    version = (
                        int(version_components[0]),
                        int(version_components[1]),
                    )
            except (ValueError, UnicodeDecodeError):
                pass

        role: str | None = None
        addr_part: bytes
        if len(parts) >= 3:
            # New format: version | role | host:port [| i:{inc}]
            try:
                role = parts[1].decode().lower() or None
            except UnicodeDecodeError:
                role = None
            addr_part = parts[2]
        else:
            # Legacy 2-field format
            addr_part = parts[1]

        # Parse target address
        parsed_target: tuple[str, int] | None = None
        try:
            host, port_str = addr_part.decode().split(":", maxsplit=1)
            parsed_target = (host, int(port_str))
        except (ValueError, UnicodeDecodeError):
            pass

        return (version, role, parsed_target, addr_part)

    def _parse_claimed_incarnation(
        self, target_addr_bytes: bytes | None
    ) -> int | None:
        """Extract the joiner's claimed incarnation from a join message.

        The trailing ``|i:{incarnation}`` field is the live self_incarnation
        of the joining node. Returns ``None`` when the field is absent
        (older senders, legacy format) so callers fall back to the
        receiver-side tracker view (preserving the original behavior).
        """
        if not target_addr_bytes or b"|i:" not in target_addr_bytes:
            return None
        try:
            tail = target_addr_bytes.rsplit(b"|i:", maxsplit=1)[1]
            return int(tail.decode())
        except (ValueError, UnicodeDecodeError):
            return None

    def _queue_join_propagation(
        self,
        target: tuple[str, int],
        role: str | None,
        target_addr_bytes: bytes | None,
        claimed_incarnation: int,
    ) -> None:
        """Queue JOIN dissemination without blocking the handler.

        Builds the propagate payload in the current 4-field
        ``v{ver}|{role}|host:port|i:{inc}`` shape (or 3-field
        ``v{ver}|host:port|i:{inc}`` when role is unknown) and hands
        it to the server-level dissemination queue. The ``|i:{inc}``
        trailer is mandatory: receivers reject JOINs without it
        (see ``handle``) because there is no sound way to refute a
        SUSPECT bracket without a fresh joiner-owned incarnation.

        Previously this method (then ``_propagate_join``) awaited
        ``gather_with_errors`` over every peer in-line, holding the
        SWIM in-flight admission slot for ~2s of LHM-adjusted
        timeout per join. With N=50 nodes and re-registration storms
        the SWIM cap saturated and load-shed unrelated SWIM traffic
        (LEAVE/probe/ack). Mirrors the LEAVE-side fix in
        ``LeaveHandler._queue_leave_propagation``.
        """
        if target_addr_bytes is None:
            return

        incarnation_trailer = b"|i:" + str(claimed_incarnation).encode()
        if role:
            propagate_msg = (
                b"join>"
                + SWIM_VERSION_PREFIX
                + b"|"
                + role.encode()
                + b"|"
                + target_addr_bytes
                + incarnation_trailer
            )
        else:
            propagate_msg = (
                b"join>"
                + SWIM_VERSION_PREFIX
                + b"|"
                + target_addr_bytes
                + incarnation_trailer
            )

        self._server.queue_join_dissemination(
            target,
            claimed_incarnation,
            target_addr_bytes,
            propagate_msg,
        )
