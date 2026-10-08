"""
VOPR harness for a cluster's membership group (AD-52 slice C): real
``ClusterMembership`` instances -- at most one process per cohort address
at a time, a restarted one under a new node id -- on a ``SimulationLoop``.

The network is TCP-faithful: a request is answered after a seeded latency
per leg, sometimes stretched by a retransmission; it is never silently
lost while both ends are up and connected. A connection resets now and
then -- before the request is handled, or after, losing the reply. An
address with no process refuses at once; a partition answers nothing, so
the request times out; a process that dies mid-request answers nothing.

Faults: staggered starts, crashes (the address restarts later under a new
node id, sometimes inside one election, sometimes after the tombstone
retention), partitions -- both ways, and one way only (one side's
requests and replies are lost, the other side's arrive) -- some healing
before the tombstone retention and some after, and connection resets.

Checked as the run goes, recorded rather than raised so a run reports all
of them:

* vote safety -- per cluster, every member id grants its vote to at most
  one candidate per term (read off every granted vote on the wire);
* election safety -- per cluster, at most one leader per term;
* state machine safety -- per cluster, every member holds the same entry
  committed at each index, and the same state (who holds each address)
  at each applied index -- snapshots install state, not entries;
* acknowledgment durability -- per cluster, no member id ever holds fewer
  committed entries than it acknowledged to a leader (a member that left
  its group and came back under the same id would hold none: a voter
  that lost what it vouched for can elect a leader missing it);
* one operable cluster -- no two clusters could commit at once: no two
  have a quorum of their configuration, by their own quorum rule, among
  their live members;
* no handler raises, and no membership loop dies.
"""

import asyncio
import math
import random

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.cluster.cluster_view_cache import ClusterViewCache
from hyperscale.distributed.cluster.cluster_watch_follower import ClusterWatchFollower
from hyperscale.distributed.cluster.models.cluster_view import ClusterView
from hyperscale.distributed.cluster.cluster_membership import (
    CLUSTER_APPEND_ENTRIES_ACTION,
    CLUSTER_HELLO_ACTION,
    CLUSTER_INSTALL_SNAPSHOT_ACTION,
    CLUSTER_JOIN_ACTION,
    CLUSTER_LEAVE_ACTION,
    CLUSTER_MODE_ACTION,
    CLUSTER_MODE_FROZEN,
    CLUSTER_MODE_OPEN,
    CLUSTER_RESIZE_ACTION,
    CLUSTER_STATUS_ACTION,
    CLUSTER_WATCH_ACTION,
    CLUSTER_REQUEST_VOTE_ACTION,
    FOUND_CLUSTER_ACTION,
    ClusterMembership,
)
from hyperscale.distributed.cluster.models import (
    ClusterLeaveReply,
    ClusterLeaveRequest,
    ClusterMemberId,
    ClusterModeReply,
    ClusterModeRequest,
    ClusterResizeReply,
    ClusterResizeRequest,
    ClusterStatusReply,
    ClusterStatusRequest,
    ClusterWatchReply,
    ClusterWatchRequest,
)
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.raft.models import (
    RaftConfiguration,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.taskex.models.run_status import RunStatus
from hyperscale.logging import Logger
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

from .cluster_membership_simulation_report import ClusterMembershipSimulationReport
from .virtual_clock import VirtualClock

MemberAddress = tuple[str, int]

# The membership group's snapshot policy under simulation: production's
# (10,000 / 5,000) would never compact within a run.
SIMULATION_SNAPSHOT_ENTRIES = 8
SIMULATION_SNAPSHOT_CATCH_UP_ENTRIES = 4

# Where the operator's CLI asks from: no cohort address, never partitioned.
OPERATOR_ADDRESS: MemberAddress = ("127.0.0.1", 1)


class ClusterMembershipSimulation:
    """A cohort's membership group under seeded faults."""

    def __init__(
        self,
        seed: int,
        cohort_size: int,
        clock: VirtualClock,
        latency_bounds_seconds: tuple[float, float],
        retransmission_probability: float,
        retransmission_delay_bounds_seconds: tuple[float, float],
        connection_reset_probability: float,
        request_timeout_seconds: float,
        start_delay_bounds_seconds: tuple[float, float],
        mean_seconds_between_faults: float,
        fault_weights: dict[str, float],
        crash_downtime_bounds_seconds: tuple[float, float],
        partition_duration_bounds_seconds: tuple[float, float],
        formation_interval_seconds: float,
        tombstone_retention_seconds: float,
        observation_interval_seconds: float,
        leader_lease_drift_bound: float | None,
    ) -> None:
        self._random = random.Random(seed)
        self._leader_lease_drift_bound = leader_lease_drift_bound
        self._clock = clock
        self._latency_bounds_seconds = latency_bounds_seconds
        self._retransmission_probability = retransmission_probability
        self._retransmission_delay_bounds_seconds = retransmission_delay_bounds_seconds
        self._connection_reset_probability = connection_reset_probability
        self._request_timeout_seconds = request_timeout_seconds
        self._start_delay_bounds_seconds = start_delay_bounds_seconds
        self._mean_seconds_between_faults = mean_seconds_between_faults
        self._fault_weights = fault_weights
        self._crash_downtime_bounds_seconds = crash_downtime_bounds_seconds
        self._partition_duration_bounds_seconds = partition_duration_bounds_seconds
        self._formation_interval_seconds = formation_interval_seconds
        self._tombstone_retention_seconds = tombstone_retention_seconds
        self._observation_interval_seconds = observation_interval_seconds

        self._addresses: list[MemberAddress] = [
            ("127.0.0.1", 20_000 + slot) for slot in range(cohort_size)
        ]
        self._cohort = frozenset(self._addresses)
        # The cohort each slot's next process is launched with (its flags).
        self._slot_cohorts: list[frozenset[MemberAddress]] = [self._cohort] * cohort_size
        self._incarnation_counts = [0] * cohort_size
        self._live: dict[MemberAddress, tuple[str, ClusterMembership]] = {}
        # Addresses whose machine is gone for good once faults heal.
        self._lost_addresses: set[MemberAddress] = set()
        # The side each address is on while a partition is up; the
        # (from, to) pairs a one-way partition loses.
        self._partition_groups: dict[MemberAddress, int] = {}
        self._one_way_cuts: set[tuple[MemberAddress, MemberAddress]] = set()
        # Member id -> the commit index its committed entries were checked to.
        self._checked_commit_indexes: dict[str, int] = {}
        # (cluster group, term, voter) -> the candidate it granted.
        self._granted_votes: dict[tuple[str, int, str], str] = {}
        # (cluster, member id) -> the highest committed index a leader saw
        # it acknowledge.
        self._acknowledged_indexes: dict[tuple[str, str], int] = {}
        # (cluster, applied index) -> who held each address once applied.
        self._applied_states: dict[tuple[str, int], tuple[str, ...]] = {}
        self._faults_enabled = True
        self._running = True
        # The watcher's view: the cluster and index it resumes from, and the
        # state it rebuilt (who holds each address, the mode).
        # The operator's watch folds what it hears into the production
        # soft-state cache (AD-52 section 10): its view is checked against
        # what the members applied.
        self._watch_cache = ClusterViewCache(
            poll_wait_seconds=request_timeout_seconds / 2,
            request_timeout_seconds=request_timeout_seconds / 2,
        )

        self._logger = Logger()
        self._task_runner = TaskRunner()
        self.report = ClusterMembershipSimulationReport()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(
        self,
        fault_seconds: float,
        quiet_seconds: float,
        lost_address_count: int = 0,
        *,
        settle_seconds: float = 0.0,
        drained_address_count: int = 0,
        force_removed_address_count: int = 0,
        live_removal_attempts: int = 0,
        frozen_seconds: float = 0.0,
        resize: str | None = None,
    ) -> ClusterMembershipSimulationReport:
        """Start the cohort's first processes at staggered instants and
        inject faults for ``fault_seconds``; heal every fault, take
        ``lost_address_count`` addresses down for good, restart every other
        crashed address. After ``settle_seconds`` more, depart (AD-52
        section 13): ``drained_address_count`` processes drain and stop,
        ``force_removed_address_count`` crash and an operator removes them,
        and ``live_removal_attempts`` force-removes target live members.
        None of those addresses restarts. Run ``quiet_seconds`` more;
        report."""
        for slot, address in enumerate(self._addresses):
            self._task_runner.run(
                self._start_after,
                slot,
                address,
                self._random.uniform(*self._start_delay_bounds_seconds),
                alias="cluster-sim-start",
            )
        self._task_runner.run(self._observe_loop, alias="cluster-sim-observe")
        self._task_runner.run(self._fault_loop, alias="cluster-sim-faults")
        self._task_runner.run(self._status_read_loop, alias="cluster-sim-status-reads")
        self._watch_follower = self._start_watch()

        await self._clock.sleep(fault_seconds)
        self._faults_enabled = False
        self._partition_groups.clear()
        self._one_way_cuts.clear()
        self._lost_addresses = set(self._random.sample(self._addresses, lost_address_count))
        for lost_address in sorted(self._lost_addresses):
            if lost_address in self._live:
                await self._crash(lost_address)
        for slot, address in enumerate(self._addresses):
            if (
                address not in self._live
                and address not in self._lost_addresses
                and self._incarnation_counts[slot] > 0
            ):
                await self._start_incarnation(slot, address)

        if frozen_seconds:
            await self._clock.sleep(settle_seconds)
            await self._hold_frozen(frozen_seconds)

        if resize is not None:
            await self._clock.sleep(settle_seconds)
            await {
                "grow": self._grow,
                "shrink": self._shrink,
                "grow_then_restart_stale": self._grow_then_restart_stale,
            }[resize](settle_seconds)

        if drained_address_count or force_removed_address_count or live_removal_attempts:
            await self._clock.sleep(settle_seconds)
            await self._depart(
                drained_address_count, force_removed_address_count, live_removal_attempts, settle_seconds
            )

        await self._clock.sleep(quiet_seconds)
        self._record_final_state()
        await self._shutdown()
        return self.report

    async def _shutdown(self) -> None:
        self._running = False
        self._watch_follower.stop()
        stopped = [membership for _node_id, membership in self._live.values()]
        for membership in stopped:
            self.report.groups_left += membership._participation
            await membership.stop()
        # A watch waiting on a stopped member answers at once: none stays
        # counted open (a leaked count would grow without bound).
        await self._clock.sleep(self._request_timeout_seconds)
        self.report.watches_open_after_stop = sum(membership._open_watches for membership in stopped)
        self._live.clear()
        await self._task_runner.shutdown()

    async def _start_after(self, slot: int, address: MemberAddress, delay: float) -> None:
        await self._clock.sleep(delay)
        if address not in self._live and address not in self._lost_addresses and self._running:
            await self._start_incarnation(slot, address)

    async def _start_incarnation(self, slot: int, address: MemberAddress) -> None:
        incarnation = self._incarnation_counts[slot]
        self._incarnation_counts[slot] += 1
        node_id = f"node-{slot}-incarnation-{incarnation}"
        membership = ClusterMembership(
            node_id,
            address,
            self._slot_cohorts[slot],
            send_request=lambda destination, action, payload, sender=address: self._request(
                sender, destination, action, payload
            ),
            logger=self._logger,
            task_runner=self._task_runner,
            hlc=new_hybrid_logical_clock(node_id=slot * 1_000 + incarnation + 1, clock=self._clock),
            clock=self._clock,
            may_lead=lambda: True,
            cluster_uuids=LogicalIdGenerator(scope=node_id, clock=self._clock),
            formation_interval_seconds=self._formation_interval_seconds,
            tombstone_retention_seconds=self._tombstone_retention_seconds,
            request_timeout_seconds=self._request_timeout_seconds,
            # The wait this simulation's watchers ask for (``_start_watch``).
            watch_wait_ceiling_seconds=self._request_timeout_seconds / 2,
            on_cohort_change=None,
            # Small enough that runs snapshot, install snapshots and catch
            # members and watchers up from the retained tail, many times over.
            snapshot_entries=SIMULATION_SNAPSHOT_ENTRIES,
            snapshot_catch_up_entries=SIMULATION_SNAPSHOT_CATCH_UP_ENTRIES,
            leader_lease_drift_bound=self._leader_lease_drift_bound, storage=VolatileRaftStorage()
        )
        self._live[address] = (node_id, membership)
        await membership.start()

    async def _crash(self, address: MemberAddress) -> None:
        _node_id, membership = self._live.pop(address)
        self.report.crashes += 1
        self.report.groups_left += membership._participation
        await membership.stop()

    # ------------------------------------------------------------------
    # Departures (AD-52 section 13)
    # ------------------------------------------------------------------

    async def _depart(
        self,
        drained_address_count: int,
        force_removed_address_count: int,
        live_removal_attempts: int,
        operator_delay_seconds: float,
    ) -> None:
        departing = self._random.sample(
            sorted(self._live), drained_address_count + force_removed_address_count
        )
        for address in departing[:drained_address_count]:
            _node_id, membership = self._live[address]
            reply = await membership.leave()
            self.report.drains.append((membership.member_id, reply.released, reply.refusal))
            self._lost_addresses.add(address)
            await self._crash(address)
        removed_addresses = departing[drained_address_count:]
        for address in removed_addresses:
            self._lost_addresses.add(address)
            await self._crash(address)
        # The operator notices a machine is gone some while after it died
        # -- after the survivors elect past a leader that died with it.
        if removed_addresses:
            await self._clock.sleep(operator_delay_seconds)
        for address in removed_addresses:
            reply = await self._operator_remove(address)
            self.report.force_removals.append(
                (f"{address[0]}:{address[1]}", reply.released, reply.refusal)
            )
        for _ in range(live_removal_attempts):
            address = self._random.choice(sorted(self._live))
            reply = await self._operator_remove(address)
            self.report.live_removal_refusals.append(None if reply.released else reply.refusal)

    async def _hold_frozen(self, frozen_seconds: float) -> None:
        """Freeze the cluster, let a member die and another try to drain,
        hold for ``frozen_seconds``, then open it again."""
        await self._operator_set_mode(CLUSTER_MODE_FROZEN)
        dead_address = self._random.choice(sorted(self._live))
        self.report.dead_member_while_frozen = self._live[dead_address][1].member_id
        self._lost_addresses.add(dead_address)
        await self._crash(dead_address)
        # Midway -- the survivors have elected past a leader that died.
        await self._clock.sleep(frozen_seconds / 2)
        drain_reply = await self._live[self._random.choice(sorted(self._live))][1].leave()
        self.report.frozen_drain_refusal = None if drain_reply.released else drain_reply.refusal
        await self._clock.sleep(frozen_seconds / 2)
        if leaders := [membership for _node_id, membership in self._live.values() if membership.is_leader()]:
            self.report.voters_while_frozen = leaders[0]._group.configuration.voters
        await self._operator_set_mode(CLUSTER_MODE_OPEN)

    # ------------------------------------------------------------------
    # Resizes (AD-52 ``ResizeCluster``)
    # ------------------------------------------------------------------

    def _new_slot(self) -> tuple[int, MemberAddress]:
        slot = len(self._addresses)
        address = ("127.0.0.1", 20_000 + slot)
        self._addresses.append(address)
        self._slot_cohorts.append(frozenset())
        self._incarnation_counts.append(0)
        return slot, address

    async def _grow(self, settle_seconds: float) -> None:
        """Add an address, launch a process there with the new cohort, see
        a second resize refused while the others run with the old one,
        then relaunch each of them with the new cohort."""
        old_cohort = frozenset(self._slot_cohorts[0])
        slot, address = self._new_slot()
        reply = await self._operator_resize(address, add=True)
        new_cohort = old_cohort | {address}
        self._slot_cohorts[slot] = new_cohort
        await self._start_incarnation(slot, address)
        await self._clock.sleep(settle_seconds)
        early = await self._operator_resize(("127.0.0.1", 20_000 + len(self._addresses)), add=True)
        self.report.early_resize_refusal = None if early.applied else early.refusal
        await self._relaunch_with(new_cohort, settle_seconds, skip=address)

    async def _shrink(self, settle_seconds: float) -> None:
        """Remove a live non-leader's address, stop the process there, then
        relaunch the rest with the new cohort."""
        removed = self._random.choice(
            sorted(address for address, (_node_id, membership) in self._live.items() if not membership.is_leader())
        )
        await self._operator_resize(removed, add=False)
        await self._clock.sleep(settle_seconds)
        self._lost_addresses.add(removed)
        await self._crash(removed)
        await self._relaunch_with(frozenset(self._slot_cohorts[0]) - {removed}, settle_seconds, skip=removed)

    async def _grow_then_restart_stale(self, settle_seconds: float) -> None:
        """Grow; launch a process at every address the committed cohort
        holds that has none; relaunch a random part of the old members
        with the new cohort -- a rollout under way -- and then every
        process dies and comes back at once, each with the cohort its slot
        was last launched with. Nothing may split."""
        old_cohort = frozenset(self._slot_cohorts[0])
        slot, address = self._new_slot()
        reply = await self._operator_resize(address, add=True)
        new_cohort = frozenset(
            (cohort_host, int(cohort_port))
            for cohort_host, cohort_port in (entry.rsplit(":", 1) for entry in reply.cohort)
        )
        self._slot_cohorts[slot] = new_cohort
        await self._start_incarnation(slot, address)
        for extra_address in sorted(new_cohort - set(self._addresses)):
            extra_slot, _ = self._new_slot()
            self._addresses[extra_slot] = extra_address
            self._slot_cohorts[extra_slot] = new_cohort
            await self._start_incarnation(extra_slot, extra_address)
        await self._clock.sleep(settle_seconds)
        relaunched = self._random.sample(sorted(old_cohort), self._random.randint(0, len(old_cohort)))
        for old_slot, old_address in enumerate(self._addresses):
            if old_address in relaunched:
                self._slot_cohorts[old_slot] = new_cohort
        for live_address in sorted(self._live):
            await self._crash(live_address)
        for restart_slot, restart_address in enumerate(self._addresses):
            await self._start_incarnation(restart_slot, restart_address)

    async def _relaunch_with(
        self, cohort: frozenset[MemberAddress], settle_seconds: float, skip: MemberAddress
    ) -> None:
        """Roll every other live process: drain, stop, launch again with
        ``cohort``, one at a time, a settle apart."""
        for slot, address in enumerate(self._addresses):
            self._slot_cohorts[slot] = cohort if address in cohort else self._slot_cohorts[slot]
            if address == skip or address not in self._live:
                continue
            await self._live[address][1].leave()
            await self._crash(address)
            await self._start_incarnation(slot, address)
            await self._clock.sleep(settle_seconds)

    async def _operator_resize(self, address: MemberAddress, add: bool) -> ClusterResizeReply:
        """``hyperscale resize``: ask a live member, then the leader it
        names, once."""
        request = ClusterResizeRequest(host=address[0], port=address[1], add=add).dump()
        asked = self._random.choice(sorted(self._live))
        for _ in range(2):
            response = await self._request(OPERATOR_ADDRESS, asked, CLUSTER_RESIZE_ACTION, request)
            if isinstance(response, Exception) or not response:
                reply = ClusterResizeReply(applied=False, refusal=repr(response))
                break
            reply = ClusterResizeReply.load(response)
            if reply.applied or reply.leader_member_id is None:
                break
            asked = ClusterMemberId.parse(reply.leader_member_id).address
        self.report.resizes.append(
            (f"{'add' if add else 'remove'} {address[0]}:{address[1]}", reply.applied, reply.refusal)
        )
        return reply

    async def _operator_set_mode(self, mode: str) -> None:
        """``hyperscale membership --mode``: ask a live member, then the
        leader it names, once (``HyperscaleClient.set_cluster_mode``)."""
        request = ClusterModeRequest(mode=mode).dump()
        asked = self._random.choice(sorted(self._live))
        for _ in range(2):
            response = await self._request(OPERATOR_ADDRESS, asked, CLUSTER_MODE_ACTION, request)
            if isinstance(response, Exception) or not response:
                reply = ClusterModeReply(applied=False, refusal=repr(response))
                break
            reply = ClusterModeReply.load(response)
            if reply.applied or reply.leader_member_id is None:
                break
            asked = ClusterMemberId.parse(reply.leader_member_id).address
        self.report.mode_changes.append((mode, reply.applied, reply.refusal))

    async def _operator_remove(self, address: MemberAddress) -> ClusterLeaveReply:
        """``hyperscale remove``: ask a live member, then the leader it
        names, once (``HyperscaleClient.remove_cluster_member``)."""
        request = ClusterLeaveRequest(host=address[0], port=address[1]).dump()
        asked = self._random.choice(sorted(self._live))
        for _ in range(2):
            response = await self._request(OPERATOR_ADDRESS, asked, CLUSTER_LEAVE_ACTION, request)
            if isinstance(response, Exception) or not response:
                return ClusterLeaveReply(released=False, refusal=repr(response))
            reply = ClusterLeaveReply.load(response)
            if reply.released or reply.leader_member_id is None:
                return reply
            asked = ClusterMemberId.parse(reply.leader_member_id).address
        return reply

    # ------------------------------------------------------------------
    # Faults
    # ------------------------------------------------------------------

    async def _fault_loop(self) -> None:
        fault_kinds = sorted(self._fault_weights)
        while self._faults_enabled and self._running:
            await self._clock.sleep(self._random.expovariate(1.0 / self._mean_seconds_between_faults))
            if not self._faults_enabled:
                return
            match self._random.choices(
                fault_kinds, weights=[self._fault_weights[kind] for kind in fault_kinds]
            )[0]:
                case "crash" if self._live:
                    address = self._random.choice(sorted(self._live))
                    await self._crash(address)
                    # Log-uniform: a restart inside one election as often as
                    # one after the tombstone retention.
                    shortest_downtime, longest_downtime = self._crash_downtime_bounds_seconds
                    self._task_runner.run(
                        self._start_after,
                        self._addresses.index(address),
                        address,
                        math.exp(
                            self._random.uniform(
                                math.log(shortest_downtime), math.log(longest_downtime)
                            )
                        ),
                        alias="cluster-sim-start",
                    )
                case "partition" | "one_way_partition" as partition_kind:
                    shuffled = list(self._addresses)
                    self._random.shuffle(shuffled)
                    boundary = self._random.randint(1, len(shuffled) - 1)
                    if partition_kind == "partition":
                        self.report.partitions += 1
                        self._partition_groups = {
                            address: 0 if position < boundary else 1
                            for position, address in enumerate(shuffled)
                        }
                    else:
                        self.report.one_way_partitions += 1
                        self._one_way_cuts = {
                            (sender, destination)
                            for sender in shuffled[:boundary]
                            for destination in shuffled[boundary:]
                        }
                    self._task_runner.run(
                        self._heal_after,
                        self._random.uniform(*self._partition_duration_bounds_seconds),
                        alias="cluster-sim-heal",
                    )

    async def _status_read_loop(self) -> None:
        """An operator reading the cluster's membership as of now, through a
        random live member, as faults go on (AD-52 section 11). A served
        read must be linearizable: its read index at least every commit
        its cluster had made before the read was sent, and its state the
        state every member applied at that index."""
        while self._faults_enabled and self._running:
            await self._clock.sleep(self._random.expovariate(1.0 / self._mean_seconds_between_faults))
            if not self._live:
                continue
            committed_before: dict[str, int] = {}
            for _node_id, membership in self._live.values():
                if (group := membership._group) is not None and membership._cluster_uuid is not None:
                    committed_before[membership._cluster_uuid] = max(
                        committed_before.get(membership._cluster_uuid, 0), group.commit_index
                    )
            asked = self._random.choice(sorted(self._live))
            lease_reads_before = sum(
                membership._group.metrics()["lease_reads"]
                for _node_id, membership in self._live.values()
                if membership._group is not None
            )
            response = await self._request(
                OPERATOR_ADDRESS, asked, CLUSTER_STATUS_ACTION, ClusterStatusRequest().dump()
            )
            lease_reads_after = sum(
                membership._group.metrics()["lease_reads"]
                for _node_id, membership in self._live.values()
                if membership._group is not None
            )
            if isinstance(response, Exception) or not response:
                self.report.status_reads_refused += 1
                continue
            reply = ClusterStatusReply.load(response)
            if not reply.served:
                self.report.status_reads_refused += 1
                continue
            self.report.status_reads_served += 1
            if lease_reads_after > lease_reads_before:
                self.report.status_reads_served_by_lease += 1
            if reply.read_index < committed_before.get(reply.cluster_uuid, 0):
                self.report.violations.append(
                    f"stale read: cluster {reply.cluster_uuid} served read index {reply.read_index} "
                    f"after it had committed {committed_before[reply.cluster_uuid]}"
                )
            if (
                applied_state := self._applied_states.get((reply.cluster_uuid, reply.read_index))
            ) is not None and applied_state != (tuple(reply.holders), reply.mode):
                self.report.violations.append(
                    f"read state: cluster {reply.cluster_uuid} served {reply.holders} at index "
                    f"{reply.read_index}; its members applied {applied_state[0]}"
                )

    def _start_watch(self) -> ClusterWatchFollower:
        """An operator watching the membership (AD-52 sections 9-10)
        through the production follower: it polls one live member until it
        fails, then the next, and folds the changes -- in commit order,
        never replayed -- into the soft-state cache, whose every view must
        be the state the members applied at its index."""
        wait_seconds = self._request_timeout_seconds / 2

        def check_view(view: ClusterView) -> None:
            if (
                applied_state := self._applied_states.get((view.cluster_uuid, view.applied_index))
            ) is not None and applied_state != (tuple(sorted(view.holders.values())), view.mode):
                self.report.violations.append(
                    f"watch: rebuilt {sorted(view.holders.values())} mode {view.mode} at "
                    f"index {view.applied_index} of {view.cluster_uuid}; members applied {applied_state}"
                )

        follower = ClusterWatchFollower(
            self._watch_cache,
            seeds=lambda: sorted(self._live),
            send_watch=self._send_operator_watch,
            clock=self._clock,
            # The poll answers within the request: half its timeout
            # waiting, the rest for the round trip -- as a production
            # watch's wait sits under its transport budget.
            poll_wait_seconds=wait_seconds,
            request_timeout_seconds=self._request_timeout_seconds - wait_seconds,
            on_view_changed=check_view,
            on_disconnected_changed=self._record_watch_connectivity,
        )
        self._task_runner.run(follower.run, alias="cluster-sim-watch")
        return follower

    async def _record_watch_connectivity(self, disconnected: bool) -> None:
        """The watch entered or left disconnected mode: it must say so once
        per change, and only what its cache shows at that instant."""
        now = self._clock.monotonic()
        transitions = self.report.watch_connectivity_transitions
        if transitions and transitions[-1][1] == disconnected:
            self.report.violations.append(f"watch: reported disconnected={disconnected} twice in a row at {now}")
        if self._watch_cache.is_disconnected(now) != disconnected:
            self.report.violations.append(
                f"watch: reported disconnected={disconnected} at {now} while its cache read the opposite"
            )
        transitions.append((now, disconnected))

    async def _send_operator_watch(
        self, destination: MemberAddress, payload: bytes, timeout: float
    ) -> bytes | Exception | None:
        """One watch poll from the operator; what it brings is counted, and
        an event the cache already holds is a replay."""
        try:
            response = await self._clock.wait_for(
                self._round_trip(OPERATOR_ADDRESS, destination, CLUSTER_WATCH_ACTION, payload), timeout=timeout
            )
        except asyncio.TimeoutError as timeout_error:
            return timeout_error
        if isinstance(response, Exception) or not response:
            return response
        reply = ClusterWatchReply.load(response)
        if reply.served:
            self.report.watch_replies += 1
            watched = self._watch_cache.view
            if reply.snapshot:
                self.report.watch_snapshots += 1
            elif reply.cluster_uuid == watched.cluster_uuid:
                for index, kind, _detail in reply.events:
                    if index <= watched.applied_index:
                        self.report.violations.append(
                            f"watch: event {index} ({kind}) replayed after index {watched.applied_index}"
                        )
                self.report.watch_events += len(reply.events)
        return response

    async def _heal_after(self, duration: float) -> None:
        await self._clock.sleep(duration)
        self._partition_groups.clear()
        self._one_way_cuts.clear()

    def _cut(self, sender: MemberAddress, destination: MemberAddress) -> bool:
        """Whether what ``sender`` sends ``destination`` is lost now."""
        return (sender, destination) in self._one_way_cuts or (
            bool(self._partition_groups)
            and self._partition_groups.get(sender) != self._partition_groups.get(destination)
        )

    # ------------------------------------------------------------------
    # Network
    # ------------------------------------------------------------------

    def _leg_delay(self) -> float:
        delay = self._random.uniform(*self._latency_bounds_seconds)
        if self._faults_enabled and self._random.random() < self._retransmission_probability:
            delay += self._random.uniform(*self._retransmission_delay_bounds_seconds)
        return delay

    async def _request(
        self,
        sender: MemberAddress,
        destination: MemberAddress,
        action: str,
        payload: bytes,
    ) -> bytes | Exception | None:
        try:
            return await self._clock.wait_for(
                self._round_trip(sender, destination, action, payload),
                timeout=self._request_timeout_seconds,
            )
        except asyncio.TimeoutError:
            return TimeoutError(
                f"{destination[0]}:{destination[1]} did not answer {action} "
                f"within {self._request_timeout_seconds}s"
            )

    async def _round_trip(
        self,
        sender: MemberAddress,
        destination: MemberAddress,
        action: str,
        payload: bytes,
    ) -> bytes | Exception | None:
        await self._clock.sleep(self._leg_delay())
        if self._cut(sender, destination):
            # Nothing answers across a partition: the request times out.
            await self._clock.sleep(self._request_timeout_seconds)
        if (receiver := self._live.get(destination)) is None:
            return ConnectionRefusedError(f"nothing listens at {destination[0]}:{destination[1]}")
        receiver_node_id, membership = receiver

        reset = self._faults_enabled and self._random.random() < self._connection_reset_probability
        if reset and self._random.random() < 0.5:
            self.report.connection_resets += 1
            return ConnectionResetError("reset before the request was handled")
        handler = {
            CLUSTER_HELLO_ACTION: membership.handle_hello,
            FOUND_CLUSTER_ACTION: membership.handle_found,
            CLUSTER_JOIN_ACTION: membership.handle_join,
            CLUSTER_LEAVE_ACTION: membership.handle_leave,
            CLUSTER_MODE_ACTION: membership.handle_mode,
            CLUSTER_RESIZE_ACTION: membership.handle_resize,
            CLUSTER_STATUS_ACTION: membership.handle_status,
            CLUSTER_WATCH_ACTION: membership.handle_watch,
            CLUSTER_REQUEST_VOTE_ACTION: membership.handle_request_vote,
            CLUSTER_APPEND_ENTRIES_ACTION: membership.handle_append_entries,
            CLUSTER_INSTALL_SNAPSHOT_ACTION: membership.handle_install_snapshot,
        }[action]
        try:
            reply = await handler(payload)
        except Exception as handler_error:
            self.report.violations.append(
                f"{receiver_node_id}'s {action} handler raised {handler_error!r}"
            )
            return handler_error
        if action == CLUSTER_REQUEST_VOTE_ACTION and reply:
            request = RequestVote.load(payload)
            vote = RequestVoteResponse.load(reply)
            if vote.vote_granted and not vote.pre_vote:
                granted = self._granted_votes.setdefault(
                    (request.job_id, vote.term, vote.voter_id), request.candidate_id
                )
                if granted != request.candidate_id:
                    self.report.violations.append(
                        f"vote safety: {vote.voter_id} granted term {vote.term} of "
                        f"{request.job_id} to {granted} and to {request.candidate_id}"
                    )
        if reset:
            self.report.connection_resets += 1
            return ConnectionResetError("reset before the reply arrived")

        await self._clock.sleep(self._leg_delay())
        if self._live.get(destination) is not receiver:
            return ConnectionResetError(f"{receiver_node_id} went down before answering")
        if self._cut(destination, sender):
            # The reply is lost: the request times out.
            await self._clock.sleep(self._request_timeout_seconds)
        return reply

    # ------------------------------------------------------------------
    # Observation and invariants
    # ------------------------------------------------------------------

    async def _observe_loop(self) -> None:
        while self._running:
            self._observe()
            await self._clock.sleep(self._observation_interval_seconds)

    def _observe(self) -> None:
        live_members_by_cluster: dict[str, set[str]] = {}
        # Each member's view of its group's quorum: its latest configuration
        # and its group's quorum floor.
        quorum_views_by_cluster: dict[str, list[tuple[RaftConfiguration, int]]] = {}
        for _address, (node_id, membership) in sorted(self._live.items()):
            if (group := membership._group) is None:
                continue
            cluster_uuid = membership._cluster_uuid
            member_id = membership.member_id
            live_members_by_cluster.setdefault(cluster_uuid, set()).add(member_id)
            quorum_views_by_cluster.setdefault(cluster_uuid, []).append(
                (group.configuration, group._quorum_floor)
            )

            if group.is_leader():
                leader = self.report.leaders_by_term.setdefault(
                    (cluster_uuid, group.current_term), member_id
                )
                if leader != member_id:
                    self.report.violations.append(
                        f"election safety: cluster {cluster_uuid} term {group.current_term} "
                        f"has two leaders, {leader} and {member_id}"
                    )
                for follower, match_index in group._match_index.items():
                    acknowledged = min(match_index, group.commit_index)
                    if acknowledged > self._acknowledged_indexes.get((cluster_uuid, follower), 0):
                        self._acknowledged_indexes[(cluster_uuid, follower)] = acknowledged

            if (
                acknowledged := self._acknowledged_indexes.get((cluster_uuid, member_id), 0)
            ) > group.last_log_index:
                self.report.violations.append(
                    f"acknowledgment durability: {member_id} acknowledged committed index "
                    f"{acknowledged} of cluster {cluster_uuid} but holds only "
                    f"{group.last_log_index}"
                )

            checked_index = self._checked_commit_indexes.get(member_id, 0)
            for index in range(checked_index + 1, group.commit_index + 1):
                if (entry := group._log.get(index)) is None:
                    continue
                held = (entry.term, entry.command_type, entry.command)
                committed = self.report.committed_entries.setdefault((cluster_uuid, index), held)
                if committed != held:
                    self.report.violations.append(
                        f"state machine safety: {member_id} holds (term {entry.term}, "
                        f"{entry.command_type}) committed at index {index} of cluster "
                        f"{cluster_uuid}; another member holds (term {committed[0]}, {committed[1]})"
                    )
            self._checked_commit_indexes[member_id] = max(checked_index, group.commit_index)
            if group.last_applied_index >= 1:
                # The mode is the log's alone; the cohort starts from each
                # process's flags, so only the log's resizes agree.
                applied_state = (tuple(sorted(membership._address_holders.values())), membership._mode)
                recorded_state = self._applied_states.setdefault(
                    (cluster_uuid, group.last_applied_index), applied_state
                )
                if recorded_state != applied_state:
                    self.report.violations.append(
                        f"state machine safety: {member_id} holds addresses {applied_state} "
                        f"at applied index {group.last_applied_index} of cluster {cluster_uuid}; "
                        f"another member held {recorded_state}"
                    )
            if group.commit_index >= 1 and cluster_uuid not in self.report.clusters_formed:
                self.report.clusters_formed.append(cluster_uuid)
                if self.report.first_formed_at is None:
                    self.report.first_formed_at = self._clock.monotonic()

        operable = sorted(
            cluster_uuid
            for cluster_uuid, quorum_views in quorum_views_by_cluster.items()
            if any(
                configuration.has_quorum(live_members_by_cluster[cluster_uuid], quorum_floor)
                for configuration, quorum_floor in quorum_views
            )
        )
        if len(operable) > 1:
            self.report.violations.append(
                f"split brain: clusters {operable} each hold a quorum of live members"
            )

    def _record_final_state(self) -> None:
        for _address, (node_id, membership) in sorted(self._live.items()):
            for loop_token in membership._loop_tokens:
                if (status := self._task_runner.get_run_status(loop_token)) != RunStatus.RUNNING:
                    task_name, run_id = loop_token.rsplit(":", maxsplit=1)
                    failure = self._task_runner.tasks[task_name]._runs[int(run_id)].error
                    self.report.violations.append(
                        f"{node_id}'s {task_name} loop is {status}: {failure}"
                    )
        self.report.final_cohorts = {membership._cohort for _node_id, membership in self._live.values()}
        self.report.final_changes_applied = {
            membership.member_id: dict(membership._changes_applied) for _node_id, membership in self._live.values()
        }
        self.report.final_snapshots_installed = {
            membership.member_id: membership._group.metrics()["snapshots_installed"]
            for _node_id, membership in self._live.values()
            if membership._group is not None
        }
        if leaders := [membership for _node_id, membership in self._live.values() if membership.is_leader()]:
            self.report.watch_matches_final_state = (
                self._watch_cache.view.cluster_uuid == leaders[0]._cluster_uuid
                and self._watch_cache.view.holders == leaders[0]._address_holders
                and self._watch_cache.view.mode == leaders[0]._mode
            )
        self.report.live_member_ids = frozenset(
            membership.member_id for _node_id, membership in self._live.values()
        )
        self.report.final_cluster_uuids = {
            membership._cluster_uuid
            for _node_id, membership in self._live.values()
            if membership._cluster_uuid is not None
        }
        self.report.final_formations = {
            membership.member_id: membership.formation for _node_id, membership in self._live.values()
        }
        self.report.final_leaders = sorted(
            membership.member_id for _node_id, membership in self._live.values() if membership.is_leader()
        )
        self.report.final_commit_indexes = {
            membership.member_id: membership._group.commit_index
            for _node_id, membership in self._live.values()
            if membership._group is not None
        }
        if leaders := [
            membership for _node_id, membership in self._live.values() if membership.is_leader()
        ]:
            configuration = leaders[0]._group.configuration
            self.report.final_voters = configuration.voters
            self.report.final_learners = configuration.learners
            self.report.final_is_joint = configuration.is_joint
