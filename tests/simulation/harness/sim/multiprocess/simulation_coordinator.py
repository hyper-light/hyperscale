"""
SimulationCoordinator — global virtual-time authority + message router
for multi-process deterministic simulation.

See the package docstring for the algorithm. In short: spawn each child
process, collect its readiness report (hosted addresses, next-event time,
initial outbound), then run a conservative lockstep loop — grant every
child the same window, barrier on all their reports, route their outbound
datagrams to deliveries at ``send_time + latency`` ordered by
``(delivery_time, origin_seq)``, and advance global time to the minimum of
all next-event times and the earliest pending delivery — until every child
is idle and no deliveries remain. Then STOP all and collect their results.
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
    then ``run`` returns ``{process_id: result}``.
    """

    def __init__(self, latency: float = 0.001) -> None:
        if latency <= 0.0:
            raise ValueError("latency must be strictly positive (lookahead)")
        self._latency = latency
        self._specs: list[tuple] = []

    def add_process(self, process_id, entry, *entry_args) -> None:
        """Register a child process.

        ``entry`` must be a top-level (picklable) callable
        ``entry(ctx, *entry_args)``; it runs in the spawned process and
        sets up that process's servers/behavior on the ``ChildContext``.
        """
        self._specs.append((process_id, entry, entry_args))

    def run(self) -> dict:
        context = multiprocessing.get_context("spawn")
        connections: dict = {}
        processes: dict = {}

        for process_id, entry, entry_args in self._specs:
            parent_conn, child_conn = context.Pipe()
            process = context.Process(
                target=run_child_loop,
                args=(child_conn, entry, entry_args),
            )
            process.start()
            connections[process_id] = parent_conn
            processes[process_id] = process

        try:
            return self._drive(connections)
        finally:
            for process in processes.values():
                process.join()

    # -- internals ------------------------------------------------------

    def _drive(self, connections: dict) -> dict:
        next_times: dict = {}
        address_to_process: dict = {}
        pending: list = []  # heap: (delivery_time, seq, dst_process, dst_addr, src_addr, data)
        sequence = 0

        # Readiness barrier: every child reports its hosted addresses.
        for process_id, connection in connections.items():
            tag, addresses, next_time, outbound = connection.recv()
            assert tag == "READY", tag
            next_times[process_id] = next_time
            for address in addresses:
                address_to_process[address] = process_id
            for send_time, src, dst, data in outbound:
                sequence = self._enqueue(
                    pending, sequence, address_to_process, send_time, src, dst, data
                )

        # Lockstep.
        while True:
            candidates = [t for t in next_times.values() if t is not None]
            if pending:
                candidates.append(pending[0][0])
            if not candidates:
                break
            target_time = min(candidates)

            due = {process_id: [] for process_id in connections}
            while pending and pending[0][0] <= target_time:
                delivery_time, _seq, dst_process, dst_addr, src_addr, data = (
                    heapq.heappop(pending)
                )
                due[dst_process].append((delivery_time, dst_addr, src_addr, data))

            for process_id, connection in connections.items():
                connection.send(("GRANT", target_time, due[process_id]))

            # Barrier: collect every report before advancing global time.
            for process_id, connection in connections.items():
                tag, next_time, outbound = connection.recv()
                assert tag == "REPORT", tag
                next_times[process_id] = next_time
                for send_time, src, dst, data in outbound:
                    sequence = self._enqueue(
                        pending, sequence, address_to_process,
                        send_time, src, dst, data,
                    )

        # Shutdown barrier: collect results.
        results: dict = {}
        for process_id, connection in connections.items():
            connection.send(("STOP",))
            tag, result = connection.recv()
            assert tag == "RESULT", tag
            results[process_id] = result
        return results

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
