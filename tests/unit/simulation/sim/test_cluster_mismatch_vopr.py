"""
VOPR: a cluster's membership group (AD-52) refuses every node that is not
of it -- another cluster, another founding cohort, a cluster founded anew
under the same cohort -- and every message too damaged to be one of its
own, at each door: greeting (``cluster_hello``), founding
(``found_cluster``), join (``cluster_join``), the group's Raft RPCs and the
membership watch.

Real ``ClusterMembership`` instances form real clusters on a
``SimulationLoop``; each seed then fires a seeded stream of foreign and
corrupted messages (truncated, bit-flipped, garbage, another message type,
fields of the wrong type or value) straight at a seeded member's
handlers. Every one must be refused -- a refusal reply, no Raft answer,
or a clean exception the server turns into an error response -- and leave
the member exactly as it was: its formation, cluster, Raft term, vote,
log, commit, configuration, address holders, mode and cohort, and the
size of every map, set and queue it or its group keeps. Refusing takes no
virtual time and sends nothing: a foreign join is never greeted back, a
foreign watch never parks. Once the stream ends the attacked cluster is
the cluster it was -- one leader, same uuid, same voters -- and no
legitimate handler raised throughout.

A member that has not formed is held to the same: a founding from another
cohort, or one naming it among damaged co-founders, is never adopted.
A node whose cohort addresses are held by a cluster configured with a
different cohort never joins it, and that cluster never takes it in.

The watch is the one door that answers a stranger (AD-52 section 9): a
watcher of another cluster is served a snapshot to resume from, at once.
"""

import contextvars
import math
import random
from collections import deque
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.distributed.cluster.cluster_membership import (
    CLUSTER_APPEND_ENTRIES_ACTION,
    CLUSTER_HELLO_ACTION,
    CLUSTER_INSTALL_SNAPSHOT_ACTION,
    CLUSTER_JOIN_ACTION,
    CLUSTER_REQUEST_VOTE_ACTION,
    CLUSTER_WATCH_ACTION,
    FORMATION_DISCOVERING,
    FORMATION_FORMED,
    FOUND_CLUSTER_ACTION,
    ClusterMembership,
)
from hyperscale.distributed.cluster.models import (
    ClusterHello,
    ClusterHelloReply,
    ClusterJoinReply,
    ClusterJoinRequest,
    ClusterMemberId,
    ClusterWatchReply,
    ClusterWatchRequest,
    FoundCluster,
    FoundClusterReply,
)
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.models.message import Message
from hyperscale.distributed.raft.models import AppendEntries, RaftConfiguration, RaftLogEntry, RequestVote
from hyperscale.distributed.raft.snapshot import InstallSnapshot
from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger, LoggingConfig
from tests.simulation.harness.sim import SeededRandom, SimulationLoop, VirtualClock
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

ScenarioResult = TypeVar("ScenarioResult")
MemberAddress = tuple[str, int]

HOST = "127.0.0.1"
COHORT_PORTS = (20_000, 20_001, 20_002)
FOREIGN_COHORT_PORTS = (21_000, 21_001, 21_002)
# A cohort sharing two of the attacked cohort's addresses: a misconfigured
# neighbour, the likeliest stranger in production.
OVERLAPPING_COHORT_PORTS = (20_001, 20_002, 20_003)
LATENCY_BOUNDS_SECONDS = (0.001, 0.02)
REQUEST_TIMEOUT_SECONDS = 1.0
FORMATION_INTERVAL_SECONDS = 1.0
TOMBSTONE_RETENTION_SECONDS = 30.0
FORMATION_DEADLINE_SECONDS = 20.0
SETTLE_POLL_SECONDS = 0.1
ATTACK_GAP_BOUNDS_SECONDS = (0.0, 0.3)
ATTACKS_PER_SEED = 160
SEEDS = range(12)
SNAPSHOT_ENTRIES = 8
SNAPSHOT_CATCH_UP_ENTRIES = 4
WATCH_WAIT_SECONDS = 5.0
CORRUPTION_KINDS = ("truncate", "bit_flip", "garbage", "append_garbage", "zero_length")
WRONG_TYPED_VALUES: tuple[object, ...] = (
    None,
    0,
    -1,
    2**80,
    math.nan,
    math.inf,
    b"\xff\x00",
    "",
    "not-a-member-id",
    "#@:",
    [],
    ["x"],
    {"k": "v"},
    (1, 2),
)


class MismatchNetwork:
    """Delivers requests between live members after a seeded latency, and
    records every request sent and every legitimate handler that raised."""

    def __init__(self, seed: int, clock: VirtualClock) -> None:
        self._random = random.Random(seed)
        self._clock = clock
        self.live: dict[MemberAddress, ClusterMembership] = {}
        self.sent: list[tuple[MemberAddress, MemberAddress, str]] = []
        self.handler_errors: list[str] = []

    async def request(
        self,
        sender: MemberAddress,
        destination: MemberAddress,
        action: str,
        payload: bytes,
    ) -> bytes | Exception | None:
        self.sent.append((sender, destination, action))
        await self._clock.sleep(self._random.uniform(*LATENCY_BOUNDS_SECONDS))
        if (receiver := self.live.get(destination)) is None:
            return ConnectionRefusedError(f"nothing listens at {destination[0]}:{destination[1]}")
        try:
            reply = await handler_for(receiver, action)(payload)
        except Exception as handler_error:
            self.handler_errors.append(f"{destination}'s {action} handler raised {handler_error!r}")
            return handler_error
        await self._clock.sleep(self._random.uniform(*LATENCY_BOUNDS_SECONDS))
        return reply


def handler_for(membership: ClusterMembership, action: str) -> Callable[[bytes], Coroutine[Any, Any, bytes | None]]:
    return {
        CLUSTER_HELLO_ACTION: membership.handle_hello,
        FOUND_CLUSTER_ACTION: membership.handle_found,
        CLUSTER_JOIN_ACTION: membership.handle_join,
        CLUSTER_WATCH_ACTION: membership.handle_watch,
        CLUSTER_REQUEST_VOTE_ACTION: membership.handle_request_vote,
        CLUSTER_APPEND_ENTRIES_ACTION: membership.handle_append_entries,
        CLUSTER_INSTALL_SNAPSHOT_ACTION: membership.handle_install_snapshot,
    }[action]


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    seed: int,
    run_until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop``: virtual time, seeded
    randomness, logging off, a context of its own."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock, random_source=SeededRandom(seed=seed))

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(run_until)
        assert scenario_task.done(), f"seed {seed}: still running at virtual {run_until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


async def start_member(
    network: MismatchNetwork,
    clock: VirtualClock,
    task_runner: TaskRunner,
    logger: Logger,
    node_id: str,
    address: MemberAddress,
    cohort_ports: tuple[int, ...],
) -> ClusterMembership:
    membership = ClusterMembership(
        node_id,
        address,
        frozenset((HOST, port) for port in cohort_ports),
        send_request=lambda destination, action, payload: network.request(address, destination, action, payload),
        logger=logger,
        task_runner=task_runner,
        hlc=new_hybrid_logical_clock(node_id=address[1], clock=clock),
        clock=clock,
        may_lead=lambda: True,
        cluster_uuids=LogicalIdGenerator(scope=node_id, clock=clock),
        formation_interval_seconds=FORMATION_INTERVAL_SECONDS,
        tombstone_retention_seconds=TOMBSTONE_RETENTION_SECONDS,
        request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        watch_wait_ceiling_seconds=WATCH_WAIT_SECONDS,
        on_cohort_change=None,
        snapshot_entries=SNAPSHOT_ENTRIES,
        snapshot_catch_up_entries=SNAPSHOT_CATCH_UP_ENTRIES,
        leader_lease_drift_bound=None,
        storage=VolatileRaftStorage(),
    )
    network.live[address] = membership
    await membership.start()
    return membership


async def form_cluster(
    network: MismatchNetwork,
    clock: VirtualClock,
    task_runner: TaskRunner,
    logger: Logger,
    name: str,
    cohort_ports: tuple[int, ...],
) -> list[ClusterMembership]:
    """Start a cohort and wait until every member has formed in one cluster
    with one leader."""
    members = [
        await start_member(network, clock, task_runner, logger, f"{name}-{port}", (HOST, port), cohort_ports)
        for port in cohort_ports
    ]
    waited = 0.0
    while not cluster_settled(members):
        assert waited < FORMATION_DEADLINE_SECONDS, f"cluster {name} did not form: {[m.formation for m in members]}"
        await clock.sleep(SETTLE_POLL_SECONDS)
        waited += SETTLE_POLL_SECONDS
    return members


def cluster_settled(members: list[ClusterMembership]) -> bool:
    return (
        all(member.formation == FORMATION_FORMED for member in members)
        and len({member.cluster_uuid for member in members}) == 1
        and sum(member.is_leader() for member in members) == 1
        and all(len(member.voters) == len(members) for member in members)
    )


def container_sizes(owner: object) -> tuple[tuple[str, int], ...]:
    """The size of every map, set, list and queue ``owner`` holds."""
    slot_names = [
        slot_name
        for owner_class in type(owner).__mro__
        for slot_name in getattr(owner_class, "__slots__", ())
    ]
    return tuple(
        (slot_name, len(value))
        for slot_name in sorted(slot_names)
        if isinstance(value := getattr(owner, slot_name, None), (dict, set, frozenset, list, deque))
    )


def fingerprint(membership: ClusterMembership) -> tuple[object, ...]:
    """Everything a refused message must leave as it was."""
    member_state = (
        membership.formation,
        membership._cluster_uuid,
        membership._joined,
        membership._formed,
        membership._formed_event.is_set(),
        membership._adopted_at,
        membership._discovering_seen,
        tuple(sorted(membership._address_holders.items())),
        membership._mode,
        membership._cohort,
        membership._cohort_digest,
        membership._previous_cohort_digest,
        membership._participation,
        membership._quorum,
        tuple(sorted(membership._changes_applied.items())),
        membership._foundings_proposed,
        membership._open_watches,
        membership._signalled_applied_index,
        container_sizes(membership),
        container_sizes(membership._outbox),
        tuple((address, len(pending)) for address, pending in sorted(membership._outbox._pending.items())),
    )
    if (group := membership._group) is None:
        return member_state + (None,)
    return member_state + (
        id(group),
        group._role,
        group._current_term,
        group._voted_for,
        group._current_leader,
        group._commit_index,
        group._last_applied,
        group._log.last_index(),
        group._log.last_term(),
        group.configuration.voters,
        group.configuration.learners,
        group._configuration_index,
        group._last_leader_contact,
        group._votes_withheld_until,
        group._election_deadline,
        container_sizes(group),
    )


def corrupt(attack_random: random.Random, payload: bytes) -> bytes:
    """A seeded wire corruption of ``payload``."""
    match attack_random.choice(CORRUPTION_KINDS):
        case "truncate":
            return payload[: attack_random.randrange(len(payload))]
        case "bit_flip":
            damaged = bytearray(payload)
            for _flip in range(attack_random.randint(1, 4)):
                position = attack_random.randrange(len(damaged))
                damaged[position] ^= 1 << attack_random.randrange(8)
            return bytes(damaged)
        case "garbage":
            return attack_random.randbytes(attack_random.randint(1, 2 * len(payload)))
        case "append_garbage":
            return payload + attack_random.randbytes(attack_random.randint(1, 64))
        case _:
            return b""


def wrong_typed(attack_random: random.Random) -> object:
    return attack_random.choice(WRONG_TYPED_VALUES)


def foreign_member_id(attack_random: random.Random) -> str:
    return str(
        ClusterMemberId(
            node_id=f"stranger-{attack_random.randrange(1_000)}",
            host=HOST,
            port=attack_random.choice((*FOREIGN_COHORT_PORTS, *COHORT_PORTS, 22_000)),
            participation=attack_random.randrange(4),
        )
    )


def foreign_digest(attack_random: random.Random, own_digest: str, foreign_cohort_digest: str) -> object:
    """A founders digest that is not the attacked cohort's: the foreign
    cohort's, the attacked one damaged, none, or not a digest at all."""
    match attack_random.randrange(5):
        case 0:
            return foreign_cohort_digest
        case 3:
            # No digest: a member never resized holds no previous one either.
            return None
        case 1:
            position = attack_random.randrange(len(own_digest))
            replacement = "0" if own_digest[position] != "0" else "1"
            return own_digest[:position] + replacement + own_digest[position + 1 :]
        case 2:
            return own_digest.upper() if own_digest != own_digest.upper() else own_digest[:-1]
        case _:
            return wrong_typed(attack_random)


class AttackContext:
    """What a seed's attacks are built from: the attacked member's cohort
    digest and member id, a foreign cohort's digest and a foreign
    cluster's uuid."""

    def __init__(
        self,
        target: ClusterMembership,
        foreign_cohort_digest: str,
        foreign_cluster_uuid: str,
    ) -> None:
        self.target = target
        self.own_digest = target._cohort_digest
        self.foreign_cohort_digest = foreign_cohort_digest
        self.foreign_cluster_uuid = foreign_cluster_uuid


def foreign_founding(
    attack_random: random.Random,
    context: AttackContext,
    digest: object,
    stranger: str,
) -> FoundCluster:
    """A founding the attacked member must not adopt: another cohort's
    naming it; its own cohort's naming only others; or -- once it holds a
    group -- a rival founding of its own cohort naming it (a formed
    cluster is never founded beside)."""
    target = context.target
    others = sorted({stranger, foreign_member_id(attack_random)} - {target.member_id})
    match attack_random.randrange(3 if target._group is not None else 2):
        case 0:
            return FoundCluster(
                cluster_uuid=context.foreign_cluster_uuid,
                founding_voters=sorted([target.member_id, *others]),
                founders_digest=digest,
            )
        case 1:
            return FoundCluster(
                cluster_uuid=context.foreign_cluster_uuid, founding_voters=others, founders_digest=context.own_digest
            )
        case _:
            return FoundCluster(
                cluster_uuid=context.foreign_cluster_uuid,
                founding_voters=sorted([target.member_id, *others]),
                founders_digest=context.own_digest,
            )


def foreign_message(attack_random: random.Random, context: AttackContext) -> tuple[str, Message]:
    """A message from a node that is not of the attacked cluster."""
    digest = foreign_digest(attack_random, context.own_digest, context.foreign_cohort_digest)
    stranger = foreign_member_id(attack_random)
    foreign_group = attack_random.choice(
        (f"cluster:{context.foreign_cluster_uuid}", "cluster:None", "cluster:", wrong_typed(attack_random))
    )
    term = attack_random.choice((1, 10**6, 2**63, 0))
    match attack_random.randrange(7):
        case 0:
            return CLUSTER_HELLO_ACTION, ClusterHello(member_id=stranger, founders_digest=digest)
        case 1:
            return FOUND_CLUSTER_ACTION, foreign_founding(attack_random, context, digest, stranger)
        case 2:
            return CLUSTER_JOIN_ACTION, ClusterJoinRequest(
                member_id=stranger, founders_digest=digest, schema_version=attack_random.choice((1, 2, 99))
            )
        case 3:
            return CLUSTER_REQUEST_VOTE_ACTION, RequestVote(
                job_id=foreign_group,
                term=term,
                candidate_id=stranger,
                last_log_index=10**6,
                last_log_term=term,
                pre_vote=attack_random.random() < 0.5,
            )
        case 4:
            entries = [
                RaftLogEntry(
                    term=term,
                    index=index,
                    command=stranger.encode(),
                    command_type="cluster_claim_address",
                    job_id=str(foreign_group),
                    hlc=None,
                )
                for index in range(1, attack_random.randint(1, 4))
            ]
            return CLUSTER_APPEND_ENTRIES_ACTION, AppendEntries(
                job_id=foreign_group,
                term=term,
                leader_id=stranger,
                prev_log_index=0,
                prev_log_term=0,
                entries=entries,
                leader_commit=len(entries),
            )
        case 5:
            return CLUSTER_INSTALL_SNAPSHOT_ACTION, InstallSnapshot(
                job_id=foreign_group,
                term=term,
                leader_id=stranger,
                last_included_index=10**6,
                last_included_term=term,
                configuration=RaftConfiguration(voters=frozenset({stranger})).dump(),
                data=b'{"holders": [], "mode": "frozen", "cohort": [], "previous_cohort_digest": null}',
            )
        case _:
            return CLUSTER_WATCH_ACTION, ClusterWatchRequest(
                cluster_uuid=attack_random.choice((context.foreign_cluster_uuid, None, "")),
                after_index=attack_random.choice((0, -1, 10**9)),
                wait_seconds=WATCH_WAIT_SECONDS,
            )


def attack_payload(attack_random: random.Random, context: AttackContext) -> tuple[str, bytes, bool]:
    """A foreign message, delivered intact, damaged on the wire, or sent
    to the door of another message type. Returns the action, the payload
    and whether it reached its own door intact."""
    action, message = foreign_message(attack_random, context)
    payload = message.dump()
    match attack_random.randrange(3):
        case 0:
            return action, payload, True
        case _ if isinstance(message, FoundCluster) and message.founders_digest == context.own_digest:
            # A founding of the member's own cohort refused only for what
            # it names: at the greeting door its digest is a greeting's.
            return action, payload, True
        case 1:
            return action, corrupt(attack_random, payload), False
        case _:
            misdirected_action, _other = foreign_message(attack_random, context)
            return misdirected_action, payload, misdirected_action == action


def assert_refused(seed: int, action: str, outcome: bytes | None | Exception, intact: bool) -> None:
    """A foreign message is refused at its door; a damaged or misdirected
    one is refused or raises cleanly."""
    label = f"seed {seed}: {action}"
    if isinstance(outcome, Exception):
        # A message no member could have sent may fail to decode; a
        # foreign message intact at its own door decodes and is refused.
        assert not intact, f"{label}: an intact foreign message raised {outcome!r}"
        return
    if action in (CLUSTER_REQUEST_VOTE_ACTION, CLUSTER_APPEND_ENTRIES_ACTION, CLUSTER_INSTALL_SNAPSHOT_ACTION):
        assert outcome is None, f"{label}: a foreign Raft RPC was answered"
        return
    assert outcome is not None, f"{label}: no reply"
    match action:
        case "cluster_hello":
            reply = ClusterHelloReply.load(outcome)
            assert reply.refusal is not None, f"{label}: a foreign greeting was answered: {reply}"
            assert reply.cluster_uuid is None and reply.founding_voters == [], f"{label}: refusal leaked {reply}"
        case "found_cluster":
            assert FoundClusterReply.load(outcome).adopted is False, f"{label}: a foreign founding was adopted"
        case "cluster_join":
            reply = ClusterJoinReply.load(outcome)
            assert reply.accepted is False and reply.cluster_uuid is None, f"{label}: a foreign join accepted"
        case _:
            reply = ClusterWatchReply.load(outcome)
            # A watcher of another cluster resumes from a snapshot -- never
            # from events of a log it does not follow.
            assert reply.events == [] and (not reply.served or reply.snapshot), f"{label}: watch served {reply}"


async def deliver_attack(
    seed: int,
    clock: VirtualClock,
    network: MismatchNetwork,
    attack_random: random.Random,
    context: AttackContext,
) -> str:
    action, payload, intact = attack_payload(attack_random, context)
    target = context.target
    before = fingerprint(target)
    sent_before = len(network.sent)
    started_at = clock.monotonic()
    try:
        outcome: bytes | None | Exception = await handler_for(target, action)(payload)
    except Exception as handler_error:
        outcome = handler_error
    assert_refused(seed, action, outcome, intact)
    assert fingerprint(target) == before, (
        f"seed {seed}: {action} ({'intact' if intact else 'damaged'}) changed {target.member_id}: "
        f"{[(old, new) for old, new in zip(before, fingerprint(target)) if old != new]}"
    )
    # Refusing takes no time and sends nothing: a foreign join is never
    # greeted back, a foreign watch never parks.
    assert clock.monotonic() == started_at, f"seed {seed}: {action} parked {clock.monotonic() - started_at}s"
    assert network.sent[sent_before:] == [], f"seed {seed}: {action} sent {network.sent[sent_before:]}"
    return action


def cluster_identity(members: list[ClusterMembership]) -> tuple[object, ...]:
    return (
        {member.cluster_uuid for member in members},
        {member.voters for member in members},
        {member._cohort_digest for member in members},
        {tuple(sorted(member._address_holders.items())) for member in members},
        {member.mode for member in members},
    )


def test_formed_cluster_refuses_foreign_and_corrupt_messages_at_every_door() -> None:
    def run(seed: int) -> dict[str, int]:
        async def scenario(clock: VirtualClock) -> dict[str, int]:
            network = MismatchNetwork(seed, clock)
            task_runner = TaskRunner()
            logger = Logger()
            members = await form_cluster(network, clock, task_runner, logger, "home", COHORT_PORTS)
            foreigners = await form_cluster(network, clock, task_runner, logger, "away", FOREIGN_COHORT_PORTS)
            identity, foreign_identity = cluster_identity(members), cluster_identity(foreigners)
            attack_random = random.Random(seed)
            # The control: the attacked doors are open to the cluster's own.
            control_reply = ClusterHelloReply.load(
                await members[0].handle_hello(
                    ClusterHello(member_id=members[1].member_id, founders_digest=members[1]._cohort_digest).dump()
                )
            )
            assert control_reply.refusal is None and control_reply.cluster_uuid == members[0].cluster_uuid
            attacks_by_action: dict[str, int] = {}
            for _attack in range(ATTACKS_PER_SEED):
                context = AttackContext(
                    attack_random.choice(members), foreigners[0]._cohort_digest, foreigners[0].cluster_uuid
                )
                action = await deliver_attack(seed, clock, network, attack_random, context)
                attacks_by_action[action] = attacks_by_action.get(action, 0) + 1
                await clock.sleep(attack_random.uniform(*ATTACK_GAP_BOUNDS_SECONDS))
            # The attacked cluster is the cluster it was, its legitimate
            # traffic never raised, and the foreign one was never touched.
            assert cluster_settled(members), f"seed {seed}: {[m.formation for m in members]}"
            assert cluster_identity(members) == identity, f"seed {seed}: the cluster changed"
            assert cluster_identity(foreigners) == foreign_identity, f"seed {seed}: the foreign cluster changed"
            assert network.handler_errors == [], f"seed {seed}: {network.handler_errors}"
            for membership in members + foreigners:
                await membership.stop()
            await task_runner.shutdown()
            return attacks_by_action

        return _simulate(scenario, seed, run_until=2 * FORMATION_DEADLINE_SECONDS + ATTACKS_PER_SEED + 10.0)

    totals: dict[str, int] = {}
    for seed in SEEDS:
        for action, count in run(seed).items():
            totals[action] = totals.get(action, 0) + count
    # Every door was attacked, many times over.
    assert set(totals) == {
        CLUSTER_HELLO_ACTION,
        FOUND_CLUSTER_ACTION,
        CLUSTER_JOIN_ACTION,
        CLUSTER_WATCH_ACTION,
        CLUSTER_REQUEST_VOTE_ACTION,
        CLUSTER_APPEND_ENTRIES_ACTION,
        CLUSTER_INSTALL_SNAPSHOT_ACTION,
    }
    assert min(totals.values()) > len(SEEDS) * 10, totals


def damaged_founding(attack_random: random.Random, own_member_id: str, own_digest: str) -> FoundCluster:
    """A founding that names this member under its own cohort's digest but
    carries co-founders no member could be -- or is another cohort's."""
    co_founders = [foreign_member_id(attack_random) for _co_founder in range(attack_random.randint(1, 3))]
    damaged_index = attack_random.randrange(len(co_founders))
    co_founders[damaged_index] = attack_random.choice(
        (
            co_founders[damaged_index].replace("#", ""),
            co_founders[damaged_index].replace("@", ""),
            co_founders[damaged_index].rsplit(":", 1)[0],
            co_founders[damaged_index] + "x",
            "",
            "#",
        )
    )
    match attack_random.randrange(3):
        case 0:
            founding_voters: object = [own_member_id, *co_founders]
        case 1:
            # One string holding this member's id: ``in`` matches it as a
            # substring.
            founding_voters = own_member_id + "|" + co_founders[0]
        case _:
            founding_voters = [own_member_id, wrong_typed(attack_random)]
    return FoundCluster(
        cluster_uuid=f"cluster-forged-{attack_random.randrange(1_000)}",
        founding_voters=founding_voters,
        founders_digest=attack_random.choice((own_digest, own_digest, wrong_typed(attack_random))),
    )


def test_discovering_member_never_adopts_a_foreign_or_damaged_founding() -> None:
    def run(seed: int) -> int:
        async def scenario(clock: VirtualClock) -> int:
            network = MismatchNetwork(seed, clock)
            task_runner = TaskRunner()
            logger = Logger()
            # The rest of its cohort has not started: it stays discovering.
            lone = await start_member(
                network, clock, task_runner, logger, "lone", (HOST, COHORT_PORTS[0]), COHORT_PORTS
            )
            foreigners = await form_cluster(network, clock, task_runner, logger, "away", FOREIGN_COHORT_PORTS)
            attack_random = random.Random(seed)
            context = AttackContext(lone, foreigners[0]._cohort_digest, foreigners[0].cluster_uuid)
            damaged_rejected = 0
            for _attack in range(ATTACKS_PER_SEED):
                if attack_random.random() < 0.5:
                    await deliver_attack(seed, clock, network, attack_random, context)
                else:
                    founding = damaged_founding(attack_random, lone.member_id, lone._cohort_digest)
                    before = fingerprint(lone)
                    try:
                        reply = FoundClusterReply.load(await lone.handle_found(founding.dump()))
                        assert reply.adopted is False, f"seed {seed}: adopted damaged founding {founding}"
                    except (ValueError, TypeError, AttributeError):
                        # Its voters are no member ids: refused by raising
                        # (the server answers an error), before any change.
                        pass
                    assert fingerprint(lone) == before, (
                        f"seed {seed}: damaged founding {founding} changed the member: "
                        f"{[(old, new) for old, new in zip(before, fingerprint(lone)) if old != new]}"
                    )
                    damaged_rejected += 1
                await clock.sleep(attack_random.uniform(*ATTACK_GAP_BOUNDS_SECONDS))
            assert lone.formation == FORMATION_DISCOVERING and lone._group is None
            assert lone._cluster_uuid is None, f"seed {seed}: holds cluster {lone._cluster_uuid} without a group"
            # The control: a well-formed founding of its own cohort naming
            # it is adopted -- the door the damaged ones knocked on is open.
            peer_ids = [
                str(ClusterMemberId(node_id=f"peer-{port}", host=HOST, port=port, participation=0))
                for port in COHORT_PORTS[1:]
            ]
            reply = FoundClusterReply.load(
                await lone.handle_found(
                    FoundCluster(
                        cluster_uuid="cluster-genuine",
                        founding_voters=sorted([lone.member_id, *peer_ids]),
                        founders_digest=lone._cohort_digest,
                    ).dump()
                )
            )
            assert reply.adopted and lone._cluster_uuid == "cluster-genuine"
            assert network.handler_errors == [], f"seed {seed}: {network.handler_errors}"
            for membership in [lone, *foreigners]:
                await membership.stop()
            await task_runner.shutdown()
            return damaged_rejected

        return _simulate(scenario, seed, run_until=FORMATION_DEADLINE_SECONDS + ATTACKS_PER_SEED + 10.0)

    assert sum(run(seed) for seed in SEEDS) > len(SEEDS) * ATTACKS_PER_SEED // 4


def test_node_never_joins_a_cluster_configured_with_another_cohort() -> None:
    """A node whose cohort's other addresses are held by a formed cluster of
    a different cohort greets it every round, is refused every time, and
    neither joins it nor is taken in: the cluster's state never moves."""

    def run(seed: int) -> None:
        async def scenario(clock: VirtualClock) -> None:
            network = MismatchNetwork(seed, clock)
            task_runner = TaskRunner()
            logger = Logger()
            neighbours = await form_cluster(network, clock, task_runner, logger, "neighbour", OVERLAPPING_COHORT_PORTS)
            neighbour_identity = cluster_identity(neighbours)
            sent_before = len(network.sent)
            stranger = await start_member(
                network, clock, task_runner, logger, "stranger", (HOST, COHORT_PORTS[0]), COHORT_PORTS
            )
            await clock.sleep(random.Random(seed).uniform(5, 10) * FORMATION_INTERVAL_SECONDS)
            stranger_requests = [
                (destination, action) for sender, destination, action in network.sent[sent_before:]
                if sender == stranger._address
            ]
            assert {action for _destination, action in stranger_requests} == {CLUSTER_HELLO_ACTION}, (
                f"seed {seed}: {stranger_requests}"
            )
            # The neighbours never greet it back to take it in.
            assert all(
                destination != stranger._address
                for sender, destination, _action in network.sent[sent_before:]
                if sender != stranger._address
            )
            assert stranger.formation == FORMATION_DISCOVERING and stranger._discovering_seen == 1
            assert cluster_settled(neighbours) and cluster_identity(neighbours) == neighbour_identity
            assert network.handler_errors == [], f"seed {seed}: {network.handler_errors}"
            for membership in [stranger, *neighbours]:
                await membership.stop()
            await task_runner.shutdown()

        _simulate(scenario, seed, run_until=FORMATION_DEADLINE_SECONDS + 20.0)

    for seed in SEEDS:
        run(seed)


def test_a_watch_never_holds_its_poll_past_the_members_ceiling() -> None:
    """A watcher of the cluster names how long its long poll may wait; the
    member holds it at most ``WATCH_WAIT_SECONDS`` (its ceiling). An
    infinite wait once parked the poll for good; NaN or a negative wait is
    answered at once."""

    async def scenario(clock: VirtualClock) -> list[tuple[float, float]]:
        network = MismatchNetwork(0, clock)
        task_runner = TaskRunner()
        logger = Logger()
        members = await form_cluster(network, clock, task_runner, logger, "home", COHORT_PORTS)
        watched = members[0]
        held: list[tuple[float, float]] = []
        for asked_wait in (math.inf, 10**9, -1.0, math.nan, WATCH_WAIT_SECONDS / 2):
            started = clock.monotonic()
            reply = ClusterWatchReply.load(
                await watched.handle_watch(
                    ClusterWatchRequest(
                        cluster_uuid=watched.cluster_uuid,
                        after_index=watched._group.last_applied_index,
                        wait_seconds=asked_wait,
                    ).dump()
                )
            )
            assert reply.served, reply
            held.append((asked_wait, clock.monotonic() - started))
        assert watched._open_watches == 0
        for membership in members:
            await membership.stop()
        await task_runner.shutdown()
        return held

    held = _simulate(scenario, 0, run_until=2 * FORMATION_DEADLINE_SECONDS + 10 * WATCH_WAIT_SECONDS)
    for asked_wait, held_seconds in held:
        assert held_seconds <= WATCH_WAIT_SECONDS, (asked_wait, held_seconds)
        if math.isnan(asked_wait) or asked_wait < 0:
            assert held_seconds == 0.0, (asked_wait, held_seconds)
    # A wait under the ceiling is the watcher's own.
    assert held[-1][1] == WATCH_WAIT_SECONDS / 2
