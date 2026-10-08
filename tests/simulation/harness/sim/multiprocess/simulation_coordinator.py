"""
SimulationCoordinator — global virtual-time authority + message router
for multi-process deterministic simulation.

See the package docstring for the algorithm. In short: admit each child
process (spawn it, collect its readiness report — hosted addresses,
next-event time, initial outbound), then run a conservative lockstep
loop — grant every child the same window, barrier on all their reports,
route their outbound datagrams to deliveries at ``send_time + latency``
ordered by ``(delivery_time, origin_seq)``, and advance global time to
the minimum of all next-event times and the earliest pending delivery —
until every child is idle and no deliveries remain. Then STOP all and
collect their results.

Children are admitted at two points: the initial ``add_process`` specs
before the first window, and *dynamically* at any window barrier when a
running child requests further processes (the ``ProcessSpawner`` seam —
``LocalServerPool`` spawning its pool executors is the canonical case).
A dynamically admitted child's ``SimulationLoop`` starts at the global
virtual time of the admitting barrier, so its clock — and therefore
every message it sends — is coherent with the rest of the simulation.
"""

import bisect
import heapq
import multiprocessing
import os
import random
from typing import Callable

from .child_runtime import run_child_loop


# Round virtual times to this many decimals when composing delivery
# times so repeated ``send_time + latency`` additions can't accumulate
# floating-point drift across runs. Identical IEEE ops are already
# deterministic; this is belt-and-suspenders for the determinism gate.
_TIME_QUANTUM = 9


class SimulationCoordinator:
    """Own global virtual time; route cross-process datagrams deterministically.

    ``latency`` is the fixed positive cross-process delivery delay in
    virtual seconds — it is the lookahead that guarantees a datagram sent
    in one window is delivered in a strictly later one, so the barrier
    never reorders within an instant. Add processes with ``add_process``,
    then ``run`` returns ``{process_id: result}`` (including any children
    admitted dynamically through spawn requests).

    ``seed`` is the run's base random seed: every admitted child gets its
    own ``SeededRandom`` seeded ``seed + <admission index>`` — distinct
    per process, identical across replays (admission order is part of
    the deterministic schedule).

    Fault injection: ``schedule_kill(process_id, at_time)`` SIGKILLs a
    child at a virtual instant. Kill semantics are crisp — a process
    killed at K executes nothing at or after K (its last granted window
    ended strictly before K); its addresses leave the route map (silence:
    datagrams and stream traffic toward it drop, the power-off model),
    it produces no RESULT (absent from ``run``'s dict), and every
    surviving child learns ``(kill_time, process_id, exitcode)`` so the
    production exit-code paths (``LocalServerPool.get_process_exitcodes``
    feeding the worker pool-health loop) observe the death exactly as
    they would a real subprocess exit.

    Event-triggered faults: ``schedule_on_event(process_id, matches_event,
    on_event)`` calls ``on_event(row)`` once, at the barrier after
    ``process_id`` first appends a matching row to its published result
    log, and ``on_event`` may schedule further faults (kills, restarts,
    pauses, network rules) and admissions (``schedule_admission``) at
    instants derived from the observed row — so a scenario strikes
    relative to what the run DID (a workflow became active, a leader was
    elected) instead of at a hardcoded instant that only one schedule
    honors. Anything scheduled mid-run must land strictly after the
    barrier it is scheduled at (the windows up to it already executed).
    """

    _KILLED_EXITCODE = -9  # SIGKILL, as ProcessPoolExecutor would report

    def __init__(
        self,
        latency: float = 0.001,
        max_virtual_time: float | None = None,
        seed: int = 1,
        barrier_timeout_seconds: float = 300.0,
    ) -> None:
        if latency <= 0.0:
            raise ValueError("latency must be strictly positive (lookahead)")
        self._latency = latency
        # Safety bound: stop advancing once global virtual time would
        # exceed this (a runaway scenario — e.g. a process stuck retrying
        # forever — should surface, not hang the test host). ``None``
        # means unbounded (run to natural quiescence).
        self._max_virtual_time = max_virtual_time
        self._seed = seed
        # Wall-clock deadman for every child pipe read. A child that
        # wedges (spawn failure, import crash mid-handshake, a genuine
        # deadlock) would otherwise block the coordinator's barrier
        # ``recv`` FOREVER, hanging the whole test host with zero
        # output. Virtual time bounds virtual runaways
        # (``max_virtual_time``); this bounds WALL runaways — the run
        # dies loudly naming the unresponsive child.
        self._barrier_timeout_seconds = barrier_timeout_seconds
        self._specs: list[tuple] = []
        self._kill_schedule: list[tuple] = []
        # Restart schedule: (at_time, process_id, down_seconds,
        # fsync_reorder_seed). Earlier generations' results are kept
        # under ``{process_id}.gen{n}`` in the run results.
        self._restart_schedule: list[tuple] = []
        # Pause schedule: (at_time, resume_time, process_id) —
        # SIGSTOP-style freezes (see ``schedule_pause``).
        self._pause_schedule: list[tuple] = []
        self._generation_results: dict = {}
        # child process id -> the process that requested its spawn
        # (executor pools). Restarting a parent with live spawned
        # children is unsupported (no cascade) and raises.
        self._spawned_by: dict = {}
        # Monotone admission counter — the per-child seed index. NOT
        # ``len(processes)``: a respawned generation overwrites its
        # process entry, so dict size would repeat an index and hand
        # two children identical RNG streams.
        self._admission_counter = 0
        # Deterministic network-fault schedule (partition / drop /
        # delay / duplicate), evaluated per datagram at enqueue time.
        # Fault randomness (drop and duplicate draws, delay jitter)
        # comes from a coordinator-owned generator derived from the run
        # seed — independent of every child's stream, identical across
        # replays of the same schedule.
        self._partition_rules: list[tuple] = []
        self._drop_rules: list[tuple] = []
        self._delay_rules: list[tuple] = []
        self._duplicate_rules: list[tuple] = []
        self._corrupt_rules: list[tuple] = []
        self._fault_random = random.Random((seed << 16) ^ 0x5EEDFA17)
        # Event triggers (``schedule_on_event``) still waiting for their
        # row: (process_id, matches_event, on_event), registration order.
        self._armed_event_triggers: list[tuple] = []
        # Every row a watched process reported, for the loud report of a
        # trigger whose event never happened.
        self._watched_rows: dict[str, list] = {}
        # Mid-run admissions (``schedule_admission``): (at_time,
        # process_id, entry, entry_args).
        self._admission_schedule: list[tuple] = []
        # The last window edge whose window the children executed; None
        # until ``run`` grants one. Faults scheduled mid-run must land
        # strictly after it.
        self._executed_through_time: float | None = None

    def add_process(self, process_id, entry, *entry_args) -> None:
        """Register a child process present at simulation start.

        ``entry`` must be a top-level (picklable) callable
        ``entry(ctx, *entry_args)``; it runs in the spawned process and
        sets up that process's servers/behavior on the ``ChildContext``.
        """
        self._specs.append((process_id, entry, entry_args))

    def schedule_kill(self, process_id, at_time: float) -> None:
        """Schedule ``process_id``'s abrupt death (SIGKILL) at virtual
        ``at_time``.

        The process may be one admitted dynamically mid-run (a pool
        executor); it must exist when the kill fires — an unknown or
        already-dead victim raises rather than silently no-oping.
        """
        self._reject_executed_instant(at_time)
        self._kill_schedule.append((at_time, process_id))

    def schedule_restart(
        self,
        process_id,
        at_time: float,
        *,
        down_seconds: float = 1.0,
        fsync_reorder_seed: int | None = None,
    ) -> None:
        """Schedule a power-loss + reboot of ``process_id`` at virtual
        ``at_time``.

        The victim's ``SimFilesystem`` collapses to durable content
        (volatile writes lost — power loss, not clean shutdown), and a
        NEW process generation re-spawns from the same entry/args at
        ``at_time + down_seconds`` with that surviving disk. Survivors
        observe the death exactly like a kill (silence + process-exit
        event) and the reboot exactly like a late joiner.

        ``fsync_reorder_seed`` arms the reordering-crash fault for the
        power loss: a seeded SUBSET of each file's un-fsynced segments
        survives (torn, out of order) instead of a clean truncation —
        the disk the new generation boots from contains that debris.

        Restarting a process with live spawned children (a worker with
        its executor pool) raises — kill the children first or restart
        a leaf; cascade restart is deliberately unsupported.
        """
        self._reject_executed_instant(at_time)
        self._restart_schedule.append(
            (at_time, process_id, down_seconds, fsync_reorder_seed)
        )

    def schedule_pause(
        self,
        process_id,
        at_time: float,
        resume_time: float,
    ) -> None:
        """Schedule a SIGSTOP-style freeze of ``process_id`` for the
        virtual window ``[at_time, resume_time)``.

        During the freeze the victim executes NOTHING in global time:
        it receives no window grants (it stays blocked at its barrier —
        a real freeze, not a cooperative sleep), its due deliveries and
        process-exit events are buffered in a per-victim queue instead
        of delivered, and its next-event time is excluded from the
        global ``min()`` so the rest of the cluster advances without it
        — every other node observes pure silence, exactly like a
        stopped process. At ``resume_time`` the victim is granted one
        window at the current global time carrying EVERYTHING buffered:
        it executes its entire frozen span inside that single window,
        so all of its accumulated output reaches the world in a burst
        at-or-after the resume instant — thawed-process semantics as
        every survivor observes them. Determinism is free: the schedule
        is data and the buffers fill in deterministic pop order.

        Model note (the one divergence from a literal SIGSTOP): during
        the catch-up window the victim's own timers fire at their
        originally scheduled LOCAL virtual times — its clock sweeps the
        frozen span rather than jumping over it — so the victim's own
        clock READS during the sweep are pre-resume values. Every
        schedule-visible effect (silence during the window, the burst
        after it, detection/lease/fencing behavior at the survivors) is
        freeze-faithful; a scenario that hinges on the victim's own
        clock jumping (a thawed leader locally observing its lease
        already expired) should pair the pause with per-node wall skew
        (``VirtualClock.set_wall_offset``) or assert from the
        survivors' side.

        Interactions (each deliberate, none silent):

        * Pausing a process that is dead at ``at_time`` — killed,
          restarted-and-still-down, or never admitted — raises:
          freezing a corpse is a scenario bug. A process id whose
          RESTARTED generation is live again at ``at_time`` is a valid
          victim (the pause freezes whichever incarnation is running).
        * Overlapping pause windows on one victim raise at the second
          activation. Back-to-back windows sharing an edge (resume at
          T, next pause at T) compose into one continuous freeze.
        * ``schedule_kill`` inside the window kills the frozen victim
          without a thaw — SIGKILL of a stopped process. Its buffered
          deliveries are discarded, its frozen span is never executed,
          and it produces no RESULT (like any kill).
        * ``schedule_restart`` inside the window power-losses the
          frozen victim: the disk/result snapshot reflects execution up
          to the freeze instant (a stopped process does no IO),
          buffered deliveries die with the incarnation, and the
          rebooted generation starts UN-paused.
        * A pause window still open when the run ends (only reachable
          via the ``max_virtual_time`` ceiling — an armed resume always
          keeps the run alive otherwise) never thaws: the victim's
          RESULT reflects execution up to the freeze instant.
        """
        if resume_time <= at_time:
            raise ValueError(
                "pause resume_time must be strictly after at_time "
                f"(got at_time={at_time}, resume_time={resume_time})"
            )
        self._reject_executed_instant(at_time)
        self._pause_schedule.append((at_time, resume_time, process_id))

    def schedule_partition(
        self,
        process_a,
        process_b,
        at_time: float,
        heal_time: float | None = None,
        bidirectional: bool = True,
    ) -> None:
        """Partition two processes: datagrams SENT between them during
        ``[at_time, heal_time)`` drop silently (the cable-cut model).

        ``heal_time=None`` means the partition never heals.
        ``bidirectional=False`` cuts only ``process_a -> process_b``
        (asymmetric loss, the one-way-drop scenario class).
        """
        self._reject_executed_instant(at_time)
        self._partition_rules.append(
            (at_time, heal_time, process_a, process_b, bidirectional)
        )

    def schedule_drop_rate(
        self,
        src,
        dst,
        probability: float,
        at_time: float = 0.0,
        until_time: float | None = None,
    ) -> None:
        """Drop each matching DATAGRAM sent during the window with
        ``probability`` — UDP packet-loss semantics (stream frames are
        never dropped: real TCP masks packet loss via retransmission;
        use ``schedule_partition`` for faults that break streams).
        ``src`` / ``dst`` are process ids; ``None`` wildcards that
        side. Draws come from the coordinator's seeded fault generator,
        so the loss pattern replays identically. First matching rule
        (declaration order) wins.
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError("drop probability must be within [0.0, 1.0]")
        self._reject_executed_instant(at_time)
        self._drop_rules.append((at_time, until_time, src, dst, probability))

    def schedule_delay(
        self,
        src,
        dst,
        extra_seconds: float,
        at_time: float = 0.0,
        until_time: float | None = None,
        jitter_seconds: float = 0.0,
    ) -> None:
        """Add ``extra_seconds`` (plus seeded uniform jitter up to
        ``jitter_seconds``) to matching datagrams SENT during the
        window. Strictly additive on top of the base ``latency`` so the
        lookahead guarantee — delivery in a strictly later window —
        always holds. First matching rule (declaration order) wins.
        """
        if extra_seconds < 0.0 or jitter_seconds < 0.0:
            raise ValueError("delay and jitter must be non-negative")
        self._reject_executed_instant(at_time)
        self._delay_rules.append(
            (at_time, until_time, src, dst, extra_seconds, jitter_seconds)
        )

    def schedule_duplicate(
        self,
        src,
        dst,
        probability: float,
        at_time: float = 0.0,
        until_time: float | None = None,
    ) -> None:
        """Duplicate matching DATAGRAMS sent during the window with
        ``probability`` — the copy arrives one extra latency later (a
        strictly later window), modeling UDP duplication (reliable
        streams never deliver a frame twice, so stream traffic is
        exempt). First matching rule (declaration order) wins.
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError("duplicate probability must be within [0.0, 1.0]")
        self._reject_executed_instant(at_time)
        self._duplicate_rules.append(
            (at_time, until_time, src, dst, probability)
        )

    def schedule_corrupt(
        self,
        src,
        dst,
        probability: float,
        at_time: float = 0.0,
        until_time: float | None = None,
    ) -> None:
        """Corrupt matching DATAGRAMS sent during the window with
        ``probability``: one seeded byte of the payload is XOR-flipped
        with a seeded non-zero mask and the frame is DELIVERED corrupt
        — the on-wire bit-rot fault whose invariant is the receiver's
        reject path (auth/parse must discard the frame whole; a flipped
        datagram must be indistinguishable from loss at the protocol
        level, never half-applied).

        Streams are exempt: TCP checksums the wire, so a corrupted
        segment is dropped by the kernel and retransmitted — on-wire
        corruption NEVER reaches a production TCP reader as
        delivered-corrupt bytes; its only observable is added latency,
        which ``schedule_delay`` models. Delivering corrupt stream
        frames would therefore exercise a non-production schedule.

        Byte-index and mask draws come from the coordinator's seeded
        fault generator, so WHICH byte flips (and to what) replays
        identically. Zero-length datagrams pass through unchanged (no
        bytes to rot). A drawn duplicate of a corrupted frame carries
        the same corrupted bytes (the copy is enqueued after
        corruption — one on-wire event, duplicated in the network).
        First matching rule (declaration order) wins.
        """
        if not 0.0 <= probability <= 1.0:
            raise ValueError("corrupt probability must be within [0.0, 1.0]")
        self._reject_executed_instant(at_time)
        self._corrupt_rules.append(
            (at_time, until_time, src, dst, probability)
        )

    def schedule_on_event(
        self,
        process_id,
        matches_event: Callable[[tuple], bool],
        on_event: Callable[[tuple], None],
    ) -> None:
        """Call ``on_event(row)`` once, for the FIRST row ``process_id``
        appends to its published result log that ``matches_event``
        accepts.

        Rows reach the coordinator at the barrier after the window that
        appended them, so ``on_event`` runs at that window's edge — the
        row's own instant, since a sampler row is stamped with the
        window's event time — before the next window is granted. It may
        schedule faults and admissions; each must land strictly after
        the current edge (``schedule_*`` raises otherwise), so derive
        instants from the row's own timestamp plus a positive delay
        (one coordinator latency is the earliest instant any reaction
        could take effect). A trigger whose event never happens fails
        the run LOUDLY at its end, naming the process and its rows —
        a fault that silently never fired would leave the scenario
        asserting a run it did not have.
        """
        self._armed_event_triggers.append((process_id, matches_event, on_event))
        self._watched_rows.setdefault(process_id, [])

    def schedule_admission(
        self, process_id, at_time: float, entry, *entry_args
    ) -> None:
        """Admit ``process_id`` (``entry(ctx, *entry_args)``, exactly as
        ``add_process``) at virtual ``at_time`` instead of at start.

        The child joins at that window edge like any mid-run admission
        (its ``SimulationLoop`` starts at ``at_time``) — the late-joiner
        whose join instant an event trigger derives from the run.
        """
        self._reject_executed_instant(at_time)
        self._admission_schedule.append((at_time, process_id, entry, entry_args))

    def _reject_executed_instant(self, at_time: float) -> None:
        """Refuse a fault or admission at an instant the run already executed."""
        if (
            self._executed_through_time is not None
            and at_time <= self._executed_through_time
        ):
            raise ValueError(
                f"cannot schedule at {at_time}: the run already executed "
                f"through {self._executed_through_time} (schedule strictly "
                "after the barrier the trigger fired at)"
            )

    def run(self) -> dict:
        # Pin hash randomization for every spawned child. Python
        # randomizes str/bytes hashing per process (PYTHONHASHSEED),
        # which reorders ``set`` / ``dict`` iteration — so any production
        # decision that iterates an unordered collection (e.g. routing
        # over datacenter ids) would differ run-to-run, breaking replay.
        # Hash randomization is an environmental non-determinism input
        # exactly like the wall clock and the RNG seed; the coordinator
        # already controls those (virtual clock, per-child seed) and must
        # control this one too. ``spawn`` children read PYTHONHASHSEED at
        # interpreter startup from the inherited environment, so setting
        # it here — before any child is spawned — fixes their hashing.
        # Forced (not ``setdefault``) so an ambient ``PYTHONHASHSEED=random``
        # can't reintroduce non-determinism; the coordinator itself makes
        # no hash-ordered decisions, so its own already-fixed seed is
        # irrelevant.
        os.environ["PYTHONHASHSEED"] = "0"

        spawn_context = multiprocessing.get_context("spawn")
        connections: dict = {}
        processes: dict = {}

        try:
            return self._drive(spawn_context, connections, processes)
        finally:
            # Children still alive here are blocked at their barrier
            # waiting for a GRANT/STOP that will never come — on the
            # normal path every child was STOPped and has exited, so
            # this only fires when ``_drive`` raised. Joining a live
            # child directly would block forever, swallowing the real
            # exception into a permanent hang and leaking the whole
            # process tree; kill first so the failure stays loud.
            for process in processes.values():
                if process.is_alive():
                    process.kill()
                process.join()

    # -- internals ------------------------------------------------------

    def _drive(self, spawn_context, connections: dict, processes: dict) -> dict:
        next_times: dict = {}
        address_to_process: dict = {}
        # entry/args by process id — restarts respawn from the SAME
        # spec. Mid-run spawns register during admission.
        self._spec_registry = {
            process_id: (entry, entry_args)
            for process_id, entry, entry_args in self._specs
        }
        pending: list = []  # heap: (delivery_time, seq, dst_process, dst_addr, src_addr, data)
        sequence = 0

        # Admit the initial children (and any children their setup
        # immediately requests) at virtual time 0.
        sequence = self._admit(
            spawn_context,
            connections,
            processes,
            next_times,
            address_to_process,
            pending,
            sequence,
            list(self._specs),
            start_time=0.0,
        )

        # Kills fire in (time, schedule order): ``_absorb_scheduled``
        # inserts each entry after every same-instant entry before it,
        # keeping same-instant kills in the order the scenario declared
        # them — including entries an event trigger adds mid-run.
        remaining_kills: list = []
        remaining_restarts: list = []
        remaining_admissions: list = []
        # (respawn_time, process_id, initial_disk) — restarts waiting
        # out their down window.
        pending_respawns: list = []
        # Pause bookkeeping (``schedule_pause``): activations fire in
        # (time, schedule order) like kills; armed thaws sit in a heap
        # keyed (resume_time, arming order). Membership in the delivery
        # buffer map IS the paused set — the two buffer maps are
        # co-created and co-removed per victim.
        remaining_pauses: list = []
        absorbed_counts = [0, 0, 0, 0]
        pending_resumes: list = []
        resume_sequence = 0
        paused_delivery_buffers: dict = {}
        paused_process_event_buffers: dict = {}

        # Lockstep.
        while True:
            self._absorb_scheduled(
                absorbed_counts,
                (
                    (self._kill_schedule, remaining_kills),
                    (self._restart_schedule, remaining_restarts),
                    (self._pause_schedule, remaining_pauses),
                    (self._admission_schedule, remaining_admissions),
                ),
            )
            # A frozen victim's next-event time is excluded from the
            # global minimum — time advances without it (its armed
            # resume below keeps the run alive until the thaw).
            candidates = [
                next_time
                for process_id, next_time in next_times.items()
                if next_time is not None
                and process_id not in paused_delivery_buffers
            ]
            if pending:
                candidates.append(pending[0][0])
            if remaining_kills:
                candidates.append(remaining_kills[0][0])
            if remaining_restarts:
                candidates.append(remaining_restarts[0][0])
            if pending_respawns:
                candidates.append(pending_respawns[0][0])
            if remaining_pauses:
                candidates.append(remaining_pauses[0][0])
            if pending_resumes:
                candidates.append(pending_resumes[0][0])
            if remaining_admissions:
                candidates.append(remaining_admissions[0][0])
            if not candidates:
                break
            target_time = min(candidates)
            if (
                self._max_virtual_time is not None
                and target_time > self._max_virtual_time
            ):
                break

            # Apply due kills BEFORE granting: a process killed at K has
            # executed only windows ending strictly before K, so it runs
            # nothing at or after its death instant. Survivors learn of
            # each death via the GRANT's process events.
            kill_events: list = []
            while remaining_kills and remaining_kills[0][0] <= target_time:
                kill_time, victim_id = remaining_kills.pop(0)
                self._kill_child(
                    victim_id, connections, processes, next_times, address_to_process
                )
                # SIGKILL of a stopped process: the frozen span is
                # never executed and its buffered traffic dies with it.
                self._discard_pause_state(
                    victim_id,
                    paused_delivery_buffers,
                    paused_process_event_buffers,
                )
                kill_events.append((kill_time, victim_id, self._KILLED_EXITCODE))

            # Restarts fire like kills (before granting) — the victim
            # has run only windows ending strictly before the restart
            # instant, and survivors observe the death identically. The
            # reboot is queued for the down window's end.
            while remaining_restarts and remaining_restarts[0][0] <= target_time:
                restart_time, victim_id, down_seconds, fsync_reorder_seed = (
                    remaining_restarts.pop(0)
                )
                initial_disk = self._snapshot_and_stop_child(
                    victim_id,
                    fsync_reorder_seed,
                    connections,
                    processes,
                    next_times,
                    address_to_process,
                )
                # Power loss of a frozen victim: buffered traffic dies
                # with the incarnation; the reboot starts un-paused.
                self._discard_pause_state(
                    victim_id,
                    paused_delivery_buffers,
                    paused_process_event_buffers,
                )
                kill_events.append(
                    (restart_time, victim_id, self._KILLED_EXITCODE)
                )
                heapq.heappush(
                    pending_respawns,
                    (restart_time + down_seconds, victim_id, initial_disk),
                )

            # Thaws fire before pause activations so back-to-back pause
            # windows sharing an edge compose into one continuous
            # freeze (the activation re-buffers the thawed state below
            # before any grant fires).
            thawed_deliveries: dict = {}
            thawed_process_events: dict = {}
            while pending_resumes and pending_resumes[0][0] <= target_time:
                _resume_time, _resume_seq, victim_id = heapq.heappop(
                    pending_resumes
                )
                if victim_id not in paused_delivery_buffers:
                    # The victim was killed or restarted during its
                    # freeze; its pause state was discarded then (the
                    # documented interaction) — this armed thaw is
                    # stale bookkeeping, not a scenario event.
                    continue
                thawed_deliveries[victim_id] = paused_delivery_buffers.pop(
                    victim_id
                )
                thawed_process_events[victim_id] = (
                    paused_process_event_buffers.pop(victim_id)
                )

            while remaining_pauses and remaining_pauses[0][0] <= target_time:
                _pause_time, resume_time, victim_id = remaining_pauses.pop(0)
                if victim_id in thawed_deliveries:
                    # Back-to-back windows sharing this instant: the
                    # victim re-freezes before its thaw grant fires —
                    # the windows compose into one continuous freeze.
                    paused_delivery_buffers[victim_id] = (
                        thawed_deliveries.pop(victim_id)
                    )
                    paused_process_event_buffers[victim_id] = (
                        thawed_process_events.pop(victim_id)
                    )
                elif victim_id in paused_delivery_buffers:
                    raise ValueError(
                        f"cannot pause {victim_id!r}: it is already "
                        "paused — overlapping pause windows on one "
                        "victim are unsupported"
                    )
                elif victim_id not in connections:
                    raise ValueError(
                        "cannot pause unknown or already-dead process "
                        f"{victim_id!r}"
                    )
                else:
                    paused_delivery_buffers[victim_id] = []
                    paused_process_event_buffers[victim_id] = []
                heapq.heappush(
                    pending_resumes, (resume_time, resume_sequence, victim_id)
                )
                resume_sequence += 1

            # Frozen victims miss this window's GRANT, so they would
            # miss its process-exit events too — buffer those alongside
            # the deliveries and replay them at the thaw.
            if kill_events:
                for buffered_events in paused_process_event_buffers.values():
                    buffered_events.extend(kill_events)

            # Snapshot the children granted this window — admission at
            # the barrier below grows ``connections``, and the new
            # children receive their first grant next window. Frozen
            # victims are excluded: they stay blocked at their barrier,
            # which is the freeze.
            granted = [
                (process_id, connection)
                for process_id, connection in connections.items()
                if process_id not in paused_delivery_buffers
            ]

            due = {process_id: [] for process_id, _ in granted}
            while pending and pending[0][0] <= target_time:
                delivery_time, _seq, dst_process, dst_addr, src_addr, data = (
                    heapq.heappop(pending)
                )
                # Deliveries already in flight toward a since-killed
                # process drop silently — bytes to a dead host. Toward
                # a FROZEN process they buffer for the thaw grant
                # instead: the host is alive, its NIC queue holds.
                if dst_process in due:
                    due[dst_process].append(
                        (delivery_time, dst_addr, src_addr, data)
                    )
                elif dst_process in paused_delivery_buffers:
                    paused_delivery_buffers[dst_process].append(
                        (delivery_time, dst_addr, src_addr, data)
                    )

            for process_id, connection in granted:
                # A victim thawing this window receives its whole
                # buffered freeze — deliveries first (they predate this
                # window's due set), then this window's due list; same
                # concatenation for process-exit events.
                inbound_deliveries = thawed_deliveries.pop(process_id, [])
                inbound_deliveries.extend(due[process_id])
                process_events = thawed_process_events.pop(process_id, [])
                process_events.extend(kill_events)
                connection.send(
                    ("GRANT", target_time, inbound_deliveries, process_events)
                )

            # Barrier: collect every report before advancing global time.
            # Two passes — merge every process's newly registered
            # addresses first, then enqueue all outbound — so an event
            # sent toward an address registered in this same window
            # (e.g. a server that started mid-run) routes rather than
            # dropping: everything registered by time T is routable at T.
            spawn_requests: list = []
            outbound_batches: list = []
            reported_rows: list = []
            for process_id, connection in granted:
                tag, next_time, outbound, spawns, new_addresses, new_rows = (
                    self._recv(connection, process_id, "window report")
                )
                assert tag == "REPORT", tag
                reported_rows.append((process_id, new_rows))
                next_times[process_id] = next_time
                for address in new_addresses:
                    self._merge_address(address_to_process, address, process_id)
                outbound_batches.append(outbound)
                for spawn in spawns:
                    self._spawned_by[spawn[0]] = process_id
                spawn_requests.extend(spawns)

            for outbound in outbound_batches:
                for send_time, src, dst, data in outbound:
                    sequence = self._enqueue(
                        pending, sequence, address_to_process,
                        send_time, src, dst, data,
                    )

            # The window through ``target_time`` has executed: triggers
            # its rows fire may only schedule strictly after it.
            self._executed_through_time = target_time
            for process_id, new_rows in reported_rows:
                self._observe_rows(process_id, new_rows)

            # Reboots due at this window edge join exactly like late
            # joiners — same admission path, plus the surviving disk.
            respawn_requests: list = []
            while pending_respawns and pending_respawns[0][0] <= target_time:
                _respawn_time, process_id, initial_disk = heapq.heappop(
                    pending_respawns
                )
                entry, entry_args = self._spec_registry[process_id]
                respawn_requests.append(
                    (process_id, entry, entry_args, initial_disk)
                )
            # Scheduled late joiners due at this edge join the same way.
            while remaining_admissions and remaining_admissions[0][0] <= target_time:
                _admission_time, process_id, entry, entry_args = (
                    remaining_admissions.pop(0)
                )
                respawn_requests.append((process_id, entry, entry_args))

            # Admit requested children at the barrier, starting their
            # virtual clocks at the window edge every report agreed on.
            sequence = self._admit(
                spawn_context,
                connections,
                processes,
                next_times,
                address_to_process,
                pending,
                sequence,
                respawn_requests + spawn_requests,
                start_time=target_time,
            )

        self._raise_on_unfired_triggers()

        # Shutdown barrier: collect results.
        results: dict = {}
        for process_id, connection in connections.items():
            connection.send(("STOP",))
            tag, result = self._recv(connection, process_id, "shutdown result")
            assert tag == "RESULT", tag
            results[process_id] = result
        # Earlier generations of restarted processes, in restart order:
        # ``{process_id}.gen1`` is the first generation's result. The
        # bare ``process_id`` key is always the LIVE (final) generation;
        # a process still in its down window at shutdown has only its
        # ``.genN`` entries.
        for process_id, generation_results in self._generation_results.items():
            for index, generation_result in enumerate(generation_results, 1):
                results[f"{process_id}.gen{index}"] = generation_result
        return results

    def _admit(
        self,
        spawn_context,
        connections: dict,
        processes: dict,
        next_times: dict,
        address_to_process: dict,
        pending: list,
        sequence: int,
        requests: list,
        start_time: float,
    ) -> int:
        """Spawn ``requests`` as child processes joining at ``start_time``.

        Batch-parallel and deterministic: every process in a batch is
        spawned before any readiness report is collected (so interpreter
        startup overlaps in wall-clock), then the reports are consumed in
        request order — the logical schedule depends only on that order,
        never on spawn timing. A child's setup may itself request further
        children (a worker node starting its executor pool); those nested
        batches are admitted the same way until none remain.

        All admission outbound is enqueued only after the *entire*
        admission settles, so datagrams between children of the same
        admission (pool executors dialing a leader admitted moments
        before, or each other) route against the complete address map —
        every process joining at ``start_time`` is up at ``start_time``.
        Returns the updated delivery-sequence counter.
        """
        outbound_batches: list[list] = []
        batch = requests

        while batch:
            started = []
            for request in batch:
                process_id, entry, entry_args = request[0], request[1], request[2]
                initial_disk = request[3] if len(request) > 3 else None
                if process_id in connections:
                    raise ValueError(
                        f"duplicate simulation process id: {process_id!r} — "
                        "process ids must be unique across the whole run"
                    )
                self._spec_registry[process_id] = (entry, entry_args)
                # Per-child seed: distinct per process, reproducible
                # across replays (the admission counter is monotone and
                # admission order is deterministic). A REBOOTED
                # generation gets a fresh index — a new process,
                # deterministically derived like any other.
                child_seed = self._seed + self._admission_counter
                self._admission_counter += 1
                parent_connection, child_connection = spawn_context.Pipe()
                process = spawn_context.Process(
                    target=run_child_loop,
                    args=(
                        child_connection,
                        entry,
                        entry_args,
                        start_time,
                        child_seed,
                        initial_disk,
                    ),
                )
                process.start()
                connections[process_id] = parent_connection
                processes[process_id] = process
                started.append((process_id, parent_connection))

            next_batch: list = []
            for process_id, parent_connection in started:
                tag, addresses, next_time, outbound, spawns, new_rows = (
                    self._recv(parent_connection, process_id, "admission readiness")
                )
                assert tag == "READY", tag
                self._observe_rows(process_id, new_rows)
                next_times[process_id] = next_time
                for address in addresses:
                    self._merge_address(address_to_process, address, process_id)
                outbound_batches.append(outbound)
                next_batch.extend(spawns)

            batch = next_batch

        for outbound in outbound_batches:
            for send_time, src, dst, data in outbound:
                sequence = self._enqueue(
                    pending, sequence, address_to_process,
                    send_time, src, dst, data,
                )

        return sequence

    @staticmethod
    def _absorb_scheduled(absorbed_counts: list, schedules: tuple) -> None:
        """Move every newly scheduled entry into its time-ordered remaining
        list (after same-instant entries — declaration order holds);
        ``absorbed_counts[index]`` tracks how much of each schedule moved."""
        for index, (schedule, remaining) in enumerate(schedules):
            for scheduled in schedule[absorbed_counts[index] :]:
                bisect.insort_right(remaining, scheduled, key=lambda entry: entry[0])
            absorbed_counts[index] = len(schedule)

    def _observe_rows(self, process_id, new_rows: list) -> None:
        """Record a watched process's new rows and fire the armed triggers
        each row matches, in row order."""
        if process_id not in self._watched_rows:
            return
        self._watched_rows[process_id].extend(new_rows)
        for row in new_rows:
            self._fire_triggers_matching(process_id, row)

    def _fire_triggers_matching(self, process_id, row: tuple) -> None:
        """Disarm and fire every armed trigger on ``process_id`` that ``row`` matches."""
        matched = [
            trigger
            for trigger in self._armed_event_triggers
            if trigger[0] == process_id and trigger[1](row)
        ]
        for trigger in matched:
            self._armed_event_triggers.remove(trigger)
            trigger[2](row)

    def _raise_on_unfired_triggers(self) -> None:
        """Fail the run loudly when a trigger's event never happened."""
        if self._armed_event_triggers:
            raise RuntimeError(
                "event-triggered faults never fired: "
                + "; ".join(
                    f"{process_id!r} never reported a row matching "
                    f"{matches_event.__qualname__} "
                    f"(rows: {self._watched_rows[process_id]})"
                    for process_id, matches_event, _on_event in self._armed_event_triggers
                )
            )

    def _snapshot_and_stop_child(
        self,
        victim_id,
        fsync_reorder_seed,
        connections: dict,
        processes: dict,
        next_times: dict,
        address_to_process: dict,
    ) -> dict:
        """Power-loss a child for restart: collect its crash-surviving
        durable disk (and this generation's result), then remove it
        from the simulation exactly like a kill.

        The victim is blocked at the window barrier, so the SNAPSHOT
        exchange cannot race any of its virtual execution.
        """
        live_children = sorted(
            child_id
            for child_id, parent_id in self._spawned_by.items()
            if parent_id == victim_id and child_id in connections
        )
        if live_children:
            raise ValueError(
                f"cannot restart {victim_id!r}: it has live spawned "
                f"children {live_children} — cascade restart is "
                "unsupported (kill them first or restart a leaf)"
            )

        connection = connections.pop(victim_id, None)
        if connection is None:
            raise ValueError(
                f"cannot restart unknown or already-dead process {victim_id!r}"
            )

        connection.send(("SNAPSHOT", fsync_reorder_seed))
        tag, initial_disk, generation_result = self._recv(
            connection, victim_id, "restart snapshot"
        )
        assert tag == "SNAPSHOT_RESULT", tag
        self._generation_results.setdefault(victim_id, []).append(
            generation_result
        )
        connection.close()
        next_times.pop(victim_id, None)

        victim_addresses = [
            address
            for address, process_id in address_to_process.items()
            if process_id == victim_id
        ]
        for address in victim_addresses:
            del address_to_process[address]

        process = processes.pop(victim_id)
        # The child exits after replying — join is deterministic.
        process.join()

        return initial_disk

    def _kill_child(
        self,
        victim_id,
        connections: dict,
        processes: dict,
        next_times: dict,
        address_to_process: dict,
    ) -> None:
        """SIGKILL ``victim_id`` and remove it from the simulation.

        The victim is blocked at the window barrier (``conn.recv``), so
        the wall-clock signal delivery cannot race any of its virtual
        execution — it has run exactly its granted windows and nothing
        more. Its addresses leave the route map (subsequent traffic
        drops, silence semantics) and it will produce no RESULT.
        """
        connection = connections.pop(victim_id, None)
        if connection is None:
            raise ValueError(
                f"cannot kill unknown or already-dead process {victim_id!r}"
            )
        connection.close()
        next_times.pop(victim_id, None)

        victim_addresses = [
            address
            for address, process_id in address_to_process.items()
            if process_id == victim_id
        ]
        for address in victim_addresses:
            del address_to_process[address]

        process = processes[victim_id]
        process.kill()
        # Reap immediately — the process is already dead, so the join is
        # deterministic; run()'s final join over ``processes`` is a no-op
        # for it.
        process.join()

    @staticmethod
    def _discard_pause_state(
        victim_id,
        paused_delivery_buffers: dict,
        paused_process_event_buffers: dict,
    ) -> None:
        """Clear a dead victim's freeze state (no-op when not frozen).

        A kill or restart landing inside a pause window discards the
        buffered traffic — bytes held for a host that lost power — and
        leaves the victim's armed thaw in the resume heap as a stale
        entry the resume loop skips deterministically (the documented
        kill/restart-during-pause semantics, not a silent no-op).
        """
        paused_delivery_buffers.pop(victim_id, None)
        paused_process_event_buffers.pop(victim_id, None)

    @staticmethod
    def _merge_address(address_to_process: dict, address, process_id) -> None:
        """Bind ``address`` to ``process_id`` in the route map.

        Re-announcement by the same process is idempotent-by-value; two
        *different* processes claiming one address is a topology bug
        that must surface loudly, not silently shadow the first owner.
        """
        existing = address_to_process.get(address)
        if existing is not None and existing != process_id:
            raise ValueError(
                f"simulation address collision: {address!r} is hosted by "
                f"{existing!r} and re-announced by {process_id!r}"
            )
        address_to_process[address] = process_id

    def _recv(self, connection, process_id, phase: str):
        """Barrier read with a wall-clock deadman.

        A wedged child (spawn failure, import crash mid-handshake, a
        genuine deadlock) must fail the run LOUDLY, naming itself —
        never hang the coordinator's barrier forever with zero output.
        ``run``'s ``finally`` kills the remaining tree on the raise.
        """
        if self._barrier_timeout_seconds is not None and not connection.poll(
            self._barrier_timeout_seconds
        ):
            raise RuntimeError(
                f"simulation child {process_id!r} unresponsive during "
                f"{phase} for {self._barrier_timeout_seconds}s of wall "
                "time — killing the run (wall-clock deadman)"
            )
        return connection.recv()

    @staticmethod
    def _window_matches(
        at_time: float, until_time: float | None, send_time: float
    ) -> bool:
        return send_time >= at_time and (
            until_time is None or send_time < until_time
        )

    @staticmethod
    def _endpoint_matches(rule_endpoint, process_id) -> bool:
        return rule_endpoint is None or rule_endpoint == process_id

    def _enqueue(
        self, pending, sequence, address_to_process, send_time, src, dst, data
    ) -> int:
        """Schedule one outbound datagram for delivery; return next seq.

        Drops silently when no process hosts ``dst`` (closed-port
        semantics), matching real UDP. Applies the scheduled network
        faults — every cross-process datagram passes through here, so
        this is the single deterministic fault chokepoint: partitions
        and drop rules discard, corrupt rules flip one seeded payload
        byte of datagrams that survived the drop stage (the frame is
        DELIVERED corrupt — the reject-path fault; streams exempt, see
        ``schedule_corrupt``), delay rules stretch the delivery time
        (always additive, preserving the lookahead guarantee), and
        duplicate rules enqueue a second copy one latency later. Fault
        windows key on SEND time; probabilistic draws and jitter come
        from the coordinator's seeded fault generator in enqueue order
        (drop, then corrupt, then delay, then duplicate), so the exact
        fault pattern — including WHICH byte flipped — replays
        byte-identically.
        """
        dst_process = address_to_process.get(dst)
        if dst_process is None:
            return sequence
        src_process = address_to_process.get(src)

        for at_time, heal_time, process_a, process_b, bidirectional in (
            self._partition_rules
        ):
            if not self._window_matches(at_time, heal_time, send_time):
                continue
            cut = (src_process == process_a and dst_process == process_b) or (
                bidirectional
                and src_process == process_b
                and dst_process == process_a
            )
            if cut:
                return sequence

        # Probabilistic loss and duplication model UDP semantics, so
        # they apply to DATAGRAMS only: real packet loss is invisible
        # above TCP (retransmission), and a reliable stream can never
        # deliver a frame twice. Partitions (cable cuts) and delay
        # (physical latency) apply to stream traffic too.
        is_datagram = data[0] == "dgram"

        if is_datagram:
            for at_time, until_time, rule_src, rule_dst, probability in (
                self._drop_rules
            ):
                if not self._window_matches(at_time, until_time, send_time):
                    continue
                if self._endpoint_matches(
                    rule_src, src_process
                ) and self._endpoint_matches(rule_dst, dst_process):
                    if self._fault_random.random() < probability:
                        return sequence
                    break  # first matching rule decides

        # On-wire bit rot applies to DATAGRAMS only (see
        # ``schedule_corrupt``: TCP checksums turn stream corruption
        # into retransmission latency, never delivered-corrupt bytes),
        # and only to frames that survived the drop stage — a dropped
        # frame has no wire bytes to rot.
        if is_datagram:
            for at_time, until_time, rule_src, rule_dst, probability in (
                self._corrupt_rules
            ):
                if not self._window_matches(at_time, until_time, send_time):
                    continue
                if self._endpoint_matches(
                    rule_src, src_process
                ) and self._endpoint_matches(rule_dst, dst_process):
                    if self._fault_random.random() < probability:
                        data = self._flip_seeded_byte(data)
                    break  # first matching rule decides

        extra_delay = 0.0
        for at_time, until_time, rule_src, rule_dst, extra, jitter in (
            self._delay_rules
        ):
            if not self._window_matches(at_time, until_time, send_time):
                continue
            if self._endpoint_matches(
                rule_src, src_process
            ) and self._endpoint_matches(rule_dst, dst_process):
                extra_delay = extra
                if jitter > 0.0:
                    extra_delay += self._fault_random.uniform(0.0, jitter)
                break  # first matching rule decides

        delivery_time = round(
            send_time + self._latency + extra_delay, _TIME_QUANTUM
        )
        heapq.heappush(
            pending,
            (delivery_time, sequence, dst_process, dst, src, data),
        )
        sequence += 1

        if is_datagram:
            for at_time, until_time, rule_src, rule_dst, probability in (
                self._duplicate_rules
            ):
                if not self._window_matches(at_time, until_time, send_time):
                    continue
                if self._endpoint_matches(
                    rule_src, src_process
                ) and self._endpoint_matches(rule_dst, dst_process):
                    if self._fault_random.random() < probability:
                        duplicate_time = round(
                            delivery_time + self._latency, _TIME_QUANTUM
                        )
                        heapq.heappush(
                            pending,
                            (
                                duplicate_time,
                                sequence,
                                dst_process,
                                dst,
                                src,
                                data,
                            ),
                        )
                        sequence += 1
                    break  # first matching rule decides

        return sequence

    def _flip_seeded_byte(self, data):
        """Return the datagram tuple with one seeded payload byte
        XOR-flipped.

        The flip mask is drawn from ``[1, 255]`` so the byte always
        CHANGES — an XOR with zero would be a "corruption" that
        delivered the frame intact, silently weakening any assertion
        built on it. A zero-length payload has no bytes to rot and
        passes through unchanged (no index/mask draws consumed).
        """
        payload = data[1]
        if not payload:
            return data
        corrupt_index = self._fault_random.randrange(len(payload))
        flip_mask = self._fault_random.randrange(1, 256)
        corrupted_payload = (
            payload[:corrupt_index]
            + bytes([payload[corrupt_index] ^ flip_mask])
            + payload[corrupt_index + 1 :]
        )
        return ("dgram", corrupted_payload)
