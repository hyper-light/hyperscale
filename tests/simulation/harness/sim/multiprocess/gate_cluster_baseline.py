"""
GateClusterBaseline — the role layout and job timeline of a FAULT-FREE
run of the canonical gate-cluster topology, read from that run's own
milestones.

Which gate wins the initial election, which gate accepts the
submission, and when the job runs are all functions of the seed (and
of every change that shifts the deterministic schedule). A fault
scenario that hardcodes them stops testing what it claims the moment
the schedule moves. The SIM is deterministic, and a scheduled fault
cannot change anything before its own instant, so the fault-free twin
of a scenario (same seed, same topology) IS the scenario's timeline up
to the fault: its roles and instants are exactly the ones the faulted
run lives through, and its completion is the counterfactual an
"unperturbed by the fault" bound compares against.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GateClusterBaseline:
    """Roles and job instants of one fault-free gate-cluster run.

    ``from_results`` derives every field from the run's milestones
    (gate-leader / submit-target / client status / worker active-count
    rows) and refuses a run whose shape breaks the scenarios' premise:
    a fault-free run must settle leadership on one gate BEFORE the job
    is accepted and hold it to the end (a startup hand-off — a gate that
    claimed before its datacenters were reachable leaving leadership to
    a ready peer, AD-19 — settles first), accept the submission, and
    complete the job.
    """

    leader_gate: str
    leader_claimed_at: float
    submission_gate: str
    submission_gate_index: int
    bystander_gates: tuple[str, ...]
    submitted_at: float
    running_at: float
    completion_at: float
    worker_active_at: float
    worker_drained_at: float

    @property
    def roles_are_distinct(self) -> bool:
        """Whether the leader and the submission gate are different gates —
        the three-role layout (leader, submission gate, pure follower)."""
        return self.leader_gate != self.submission_gate

    @property
    def follower_gate(self) -> str:
        """The first gate that is neither the leader nor the submission gate."""
        return self.bystander_gates[0]

    @property
    def mid_execution_at(self) -> float:
        """The midpoint of the worker's live execution of the job: a fault
        scheduled here provably lands inside the running workflow."""
        return (self.worker_active_at + self.worker_drained_at) / 2.0

    @classmethod
    def from_results(
        cls,
        results: dict,
        gate_process_ids: tuple[str, ...],
    ) -> "GateClusterBaseline":
        """Derive the baseline from a fault-free run's results."""
        client_log = results["client"]
        submitted_at = cls._first_row(client_log, "job-submitted")[1]
        leader_claimed_at, leader_gate = cls._settled_leadership(
            results, gate_process_ids, submitted_at
        )

        submission_gate_index = cls._first_row(client_log, "submit-target")[1]
        submission_gate = gate_process_ids[submission_gate_index]
        finished_row = cls._first_row(client_log, "job-finished")
        if finished_row[1] != "completed":
            raise ValueError(f"fault-free gate run must complete its job: {client_log}")

        worker_counts = [
            row for row in results["worker"] if row[0] == "workflows-active" and row[2] > 0.0
        ]
        return cls(
            leader_gate=leader_gate,
            leader_claimed_at=leader_claimed_at,
            submission_gate=submission_gate,
            submission_gate_index=submission_gate_index,
            bystander_gates=tuple(
                gate_process_id
                for gate_process_id in gate_process_ids
                if gate_process_id not in (leader_gate, submission_gate)
            ),
            submitted_at=submitted_at,
            running_at=[
                row[2] for row in client_log if row[0] == "status-seen" and row[1] == "running"
            ][0],
            completion_at=finished_row[2],
            worker_active_at=[row[2] for row in worker_counts if row[1] > 0][0],
            worker_drained_at=[row[2] for row in worker_counts if row[1] == 0][0],
        )

    @staticmethod
    def _settled_leadership(
        results: dict,
        gate_process_ids: tuple[str, ...],
        submitted_at: float,
    ) -> tuple[float, str]:
        """``(claimed_at, gate)`` of the leadership the run settled on before
        ``submitted_at`` and held to the end; raises when there is none."""
        leadership_moves = sorted(
            (row[2], row[1], gate_process_id)
            for gate_process_id in gate_process_ids
            for row in results[gate_process_id]
            if row[0] == "gate-leader" and row[2] > 0.0
        )
        final_holders = [
            gate_process_id
            for gate_process_id in gate_process_ids
            if [row[1] for row in results[gate_process_id] if row[0] == "gate-leader"][-1] == 1
        ]
        if (
            len(final_holders) != 1
            or not leadership_moves
            or leadership_moves[-1][1:] != (1, final_holders[0])
            or leadership_moves[-1][0] >= submitted_at
        ):
            raise ValueError(
                "fault-free gate run must settle leadership on one gate before "
                f"the job is accepted at {submitted_at} and hold it to the end; "
                f"observed leadership moves {leadership_moves}"
            )
        return leadership_moves[-1][0], final_holders[0]

    @staticmethod
    def _first_row(log: list, tag: str) -> tuple:
        """The first row of ``log`` tagged ``tag``; raises when there is none."""
        if not (rows := [row for row in log if row[0] == tag]):
            raise ValueError(f"fault-free gate run has no {tag!r} milestone: {log}")
        return rows[0]
