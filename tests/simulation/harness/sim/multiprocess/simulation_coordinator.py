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
    """

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

    def add_process(self, process_id, entry, *entry_args) -> None:
        """Register a child process present at simulation start.

        ``entry`` must be a top-level (picklable) callable
        ``entry(ctx, *entry_args)``; it runs in the spawned process and
        sets up that process's servers/behavior on the ``ChildContext``.
        """
        self._specs.append((process_id, entry, entry_args))

    def run(self) -> dict:
        spawn_context = multiprocessing.get_context("spawn")
        connections: dict = {}
        processes: dict = {}

        try:
            return self._drive(spawn_context, connections, processes)
        finally:
            for process in processes.values():
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

        # Lockstep.
        while True:
            candidates = [t for t in next_times.values() if t is not None]
            if pending:
                candidates.append(pending[0][0])
            if not candidates:
                break
            target_time = min(candidates)
            if (
                self._max_virtual_time is not None
                and target_time > self._max_virtual_time
            ):
                break

            # Snapshot the children granted this window — admission at
            # the barrier below grows ``connections``, and the new
            # children receive their first grant next window.
            granted = list(connections.items())

            due = {process_id: [] for process_id, _ in granted}
            while pending and pending[0][0] <= target_time:
                delivery_time, _seq, dst_process, dst_addr, src_addr, data = (
                    heapq.heappop(pending)
                )
                due[dst_process].append((delivery_time, dst_addr, src_addr, data))

            for process_id, connection in granted:
                connection.send(("GRANT", target_time, due[process_id]))

            # Barrier: collect every report before advancing global time.
            spawn_requests: list = []
            for process_id, connection in granted:
                tag, next_time, outbound, spawns = connection.recv()
                assert tag == "REPORT", tag
                next_times[process_id] = next_time
                for send_time, src, dst, data in outbound:
                    sequence = self._enqueue(
                        pending, sequence, address_to_process,
                        send_time, src, dst, data,
                    )
                spawn_requests.extend(spawns)

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
                    address_to_process[address] = process_id
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
