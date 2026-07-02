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

import heapq
import multiprocessing

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
    """

    _KILLED_EXITCODE = -9  # SIGKILL, as ProcessPoolExecutor would report

    def __init__(
        self,
        latency: float = 0.001,
        max_virtual_time: float | None = None,
        seed: int = 1,
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
        self._specs: list[tuple] = []
        self._kill_schedule: list[tuple] = []

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
        self._kill_schedule.append((at_time, process_id))

    def run(self) -> dict:
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

        # Kills fire in (time, schedule order); stable sort keeps
        # same-instant kills in the order the scenario declared them.
        remaining_kills = sorted(self._kill_schedule, key=lambda kill: kill[0])

        # Lockstep.
        while True:
            candidates = [t for t in next_times.values() if t is not None]
            if pending:
                candidates.append(pending[0][0])
            if remaining_kills:
                candidates.append(remaining_kills[0][0])
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
                kill_events.append((kill_time, victim_id, self._KILLED_EXITCODE))

            # Snapshot the children granted this window — admission at
            # the barrier below grows ``connections``, and the new
            # children receive their first grant next window.
            granted = list(connections.items())

            due = {process_id: [] for process_id, _ in granted}
            while pending and pending[0][0] <= target_time:
                delivery_time, _seq, dst_process, dst_addr, src_addr, data = (
                    heapq.heappop(pending)
                )
                # Deliveries already in flight toward a since-killed
                # process drop silently — bytes to a dead host.
                if dst_process in due:
                    due[dst_process].append(
                        (delivery_time, dst_addr, src_addr, data)
                    )

            for process_id, connection in granted:
                connection.send(
                    ("GRANT", target_time, due[process_id], kill_events)
                )

            # Barrier: collect every report before advancing global time.
            # Two passes — merge every process's newly registered
            # addresses first, then enqueue all outbound — so an event
            # sent toward an address registered in this same window
            # (e.g. a server that started mid-run) routes rather than
            # dropping: everything registered by time T is routable at T.
            spawn_requests: list = []
            outbound_batches: list = []
            for process_id, connection in granted:
                tag, next_time, outbound, spawns, new_addresses = (
                    connection.recv()
                )
                assert tag == "REPORT", tag
                next_times[process_id] = next_time
                for address in new_addresses:
                    self._merge_address(address_to_process, address, process_id)
                outbound_batches.append(outbound)
                spawn_requests.extend(spawns)

            for outbound in outbound_batches:
                for send_time, src, dst, data in outbound:
                    sequence = self._enqueue(
                        pending, sequence, address_to_process,
                        send_time, src, dst, data,
                    )

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
                spawn_requests,
                start_time=target_time,
            )

        # Shutdown barrier: collect results.
        results: dict = {}
        for process_id, connection in connections.items():
            connection.send(("STOP",))
            tag, result = connection.recv()
            assert tag == "RESULT", tag
            results[process_id] = result
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
            for process_id, entry, entry_args in batch:
                if process_id in connections:
                    raise ValueError(
                        f"duplicate simulation process id: {process_id!r} — "
                        "process ids must be unique across the whole run"
                    )
                # Per-child seed: distinct per process, reproducible
                # across replays (``len(processes)`` is the admission
                # index, and admission order is deterministic).
                child_seed = self._seed + len(processes)
                parent_connection, child_connection = spawn_context.Pipe()
                process = spawn_context.Process(
                    target=run_child_loop,
                    args=(child_connection, entry, entry_args, start_time, child_seed),
                )
                process.start()
                connections[process_id] = parent_connection
                processes[process_id] = process
                started.append((process_id, parent_connection))

            next_batch: list = []
            for process_id, parent_connection in started:
                tag, addresses, next_time, outbound, spawns = (
                    parent_connection.recv()
                )
                assert tag == "READY", tag
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

    def _enqueue(
        self, pending, sequence, address_to_process, send_time, src, dst, data
    ) -> int:
        """Schedule one outbound datagram for delivery; return next seq.

        Drops silently when no process hosts ``dst`` (closed-port
        semantics), matching real UDP.
        """
        dst_process = address_to_process.get(dst)
        if dst_process is None:
            return sequence
        delivery_time = round(send_time + self._latency, _TIME_QUANTUM)
        heapq.heappush(
            pending,
            (delivery_time, sequence, dst_process, dst, src, data),
        )
        return sequence + 1
