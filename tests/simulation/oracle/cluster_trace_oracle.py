"""
ClusterTraceOracle — the G3 cross-node state checker for multi-process
SIM runs.

Today's committed invariants judge only the CLIENT's view
(``JobStatusOracle`` plus per-suite loud-outcome rules), so wrongness
that never reaches the client is invisible: a manager recording
``completed`` while the client saw ``failed``, one job executing in
two datacenters, two gates holding the leader flag over the same
instants, a gate's terminal record disagreeing with its manager. This
oracle closes that gap by judging the MERGED per-node milestone trace
of one run — the coordinator's results dict exactly as
``SimulationCoordinator.run()`` returns it (``process_id -> milestone
list``; earlier generations of restarted processes under
``{process_id}.gen{n}``; SIGKILLed children ABSENT, because killed
children never report).

WHY POST-RUN TRACE CHECKING EQUALS CONTINUOUS CHECKING: every child of
one simulation shares ONE coherent virtual timeline (the coordinator's
lockstep windows), so per-node milestone logs merge into a single
global trace ordered by virtual time. The run is deterministic — the
seed reproduces the identical trace byte for byte — and the watchers
record every state change they sample, so the trace IS the run's
observable history: an invariant violated at any virtual instant is
present in the merged trace at that instant. Checking the complete
trace after the run therefore reaches exactly the verdict a continuous
online checker would have reached at every instant, without perturbing
the schedule under test (an in-run checker would itself consume
virtual time and shift the schedule it judges). Erased evidence is the
one caveat, and it is explicit: a SIGKILLed child's log never exists,
so callers pass ``killed_process_ids`` and every check degrades to the
judgments its surviving evidence supports — ABSENT evidence is
tolerated, CONTRADICTORY evidence never is.

Milestone VALUES only — the oracle never requires node ids or
snowflakes (the replay contract forbids them in logs). Vocabulary
consumed (committed rows plus the G3 node-side additions):

* gate:    ``("gate-leader", flag, t)`` 0/1 transitions;
           ``("dc-health", dc_id, health, t)`` (or the single-DC
           positional ``("dc-health", health, t)``);
           ``("gate-job-terminal", status, t)``
* manager: ``("job-terminal", status, t)``
* worker:  ``("workflows-active", count, t)`` sampled transitions;
           ``("workflow-executed"/"workflow-failed", name, t)``
* client:  ``("status-seen", status, t)`` and ``("job-finished",
           status, t)`` — ONE job's stream (``JobLogSplitter`` feeds
           multi-job logs through here per job)
* any:     ``("determinism-audit-unswapped", [module names])``

Every check is individually callable so the committed suites can
migrate one invariant at a time (``vopr_mdc``'s placement/health
checks and ``vopr_gates``'s leader-convergence check are mirrored
here verbatim in semantics); ``check_cluster_trace`` composes the full
set for new suites.
"""

from collections.abc import Iterable, Mapping

from hyperscale.distributed.jobs.job_status_order import JobStatusOrder

# Production writes two spellings for the same terminal outcome:
# managers record the enum's ``timeout``, the gate timeout tracker
# records ``timed_out``. Agreement checks compare NORMALIZED spellings
# so the live vocabulary never false-positives as disagreement.
_TERMINAL_SPELLINGS = {"timed_out": "timeout"}

_AUDIT_TAG = "determinism-audit-unswapped"

_EXPLICIT_ATTEMPT_TAGS = ("workflow-executed", "workflow-failed")


class ClusterTraceOracle:
    """Judge the merged cross-node milestone trace of one SIM run.

    Configuration is init state (the run TOPOLOGY: which process ids
    are gates, managers, per-datacenter workers, and the client, plus
    the scenario's declared retry budget and leader stability window);
    per-run data (the results dict, which processes were SIGKILLed,
    the run ceiling) arrives through the check methods, which each
    return human-readable violations (empty = the trace is coherent).
    """

    __slots__ = (
        "_order",
        "_client_process_id",
        "_gate_process_ids",
        "_manager_process_ids",
        "_worker_process_ids_by_datacenter",
        "_retry_budget",
        "_leader_stability_window_seconds",
    )

    def __init__(
        self,
        client_process_id: str = "client",
        gate_process_ids: Iterable[str] = (),
        manager_process_ids: Iterable[str] = (),
        worker_process_ids_by_datacenter: Mapping[str, Iterable[str]] | None = None,
        retry_budget: int | None = None,
        leader_stability_window_seconds: float = 30.0,
    ) -> None:
        """Describe the run topology this oracle judges.

        * ``retry_budget`` — the scenario's declared retry allowance:
          the merged trace may show at most ``retry_budget + 1``
          workflow execution starts (the first attempt plus retries).
          ``None`` means the scenario declares no bound. Scenarios
          whose jobs carry multiple workflows scale the declared
          number accordingly — the bound is expressed in execution
          STARTS, the unit the worker milestones record.
        * ``leader_stability_window_seconds`` — the tail window of the
          run in which any ``gate-leader`` transition counts as
          flapping rather than recovery (the committed ``vopr_gates``
          value is the default).
        """
        self._order = JobStatusOrder()
        self._client_process_id = client_process_id
        self._gate_process_ids = tuple(gate_process_ids)
        self._manager_process_ids = tuple(manager_process_ids)
        self._worker_process_ids_by_datacenter = {
            datacenter_id: tuple(worker_process_ids)
            for datacenter_id, worker_process_ids in (
                worker_process_ids_by_datacenter or {}
            ).items()
        }
        self._retry_budget = retry_budget
        self._leader_stability_window_seconds = leader_stability_window_seconds

    # ------------------------------------------------------------------
    # Trace plumbing
    # ------------------------------------------------------------------

    @staticmethod
    def _generation_logs(
        results: Mapping[str, list[tuple]], process_id: str
    ) -> list[list[tuple]]:
        """One process's milestone logs in lifetime order.

        ``{process_id}.gen1`` is the first generation, the bare
        ``process_id`` key the live (final) one; a process still in
        its down window at shutdown has only ``.genN`` entries, and a
        SIGKILLed process has none at all.
        """
        ordered_logs: list[list[tuple]] = []
        generation_index = 1
        while isinstance(
            generation_log := results.get(f"{process_id}.gen{generation_index}"),
            list,
        ):
            ordered_logs.append(generation_log)
            generation_index += 1
        if isinstance(live_log := results.get(process_id), list):
            ordered_logs.append(live_log)
        return ordered_logs

    @staticmethod
    def _rows(ordered_logs: list[list[tuple]], tag: str) -> list[tuple]:
        """All rows bearing ``tag`` across the logs, in lifetime order."""
        return [
            row
            for generation_log in ordered_logs
            for row in generation_log
            if row and row[0] == tag
        ]

    @staticmethod
    def _latest_recorded_instant(results: Mapping[str, list[tuple]]) -> float:
        """The run's last recorded virtual instant — the default end
        bound for still-open leadership intervals (rows carry their
        timestamp as the final element; non-numeric finals, like the
        audit row's module list, are not instants)."""
        recorded_instants = [
            row[-1]
            for process_log in results.values()
            if isinstance(process_log, list)
            for row in process_log
            if row
            and isinstance(row[-1], (int, float))
            and not isinstance(row[-1], bool)
        ]
        return max(recorded_instants, default=0.0)

    def _normalize_terminal(self, status: str) -> str:
        return _TERMINAL_SPELLINGS.get(status, status)

    # ------------------------------------------------------------------
    # (a) Gate-leader exclusivity — interval overlap
    # ------------------------------------------------------------------

    def check_gate_leader_exclusivity(
        self,
        results: Mapping[str, list[tuple]],
        killed_process_ids: Iterable[str] = (),
        run_end_time: float | None = None,
    ) -> list[str]:
        """At most one gate holds the leader flag at any instant.

        Intervals are rebuilt from each surviving gate's
        ``("gate-leader", flag, t)`` 0/1 transitions: a flag-1 row
        opens a claim, the next flag-0 row closes it. A claim still
        open at the end of the LIVE generation extends to
        ``run_end_time`` (default: the run's last recorded instant) —
        so two gates ENDING the run as leader is itself an overlap. A
        claim open when a non-final generation power-lost closes at
        that generation's last recorded instant: the erasure of the
        exact loss instant makes this an under-approximation, never a
        false positive. SIGKILLed gates left no log at all — their
        history is erased, and only surviving evidence is judged.
        """
        killed = set(killed_process_ids)
        effective_end_time = (
            run_end_time
            if run_end_time is not None
            else self._latest_recorded_instant(results)
        )
        intervals_by_gate = {
            gate_process_id: self._leader_intervals(
                self._generation_logs(results, gate_process_id),
                effective_end_time,
            )
            for gate_process_id in self._gate_process_ids
            if gate_process_id not in killed
        }

        violations: list[str] = []
        surviving_gate_ids = sorted(intervals_by_gate)
        for first_position, first_gate_id in enumerate(surviving_gate_ids):
            for second_gate_id in surviving_gate_ids[first_position + 1 :]:
                violations.extend(
                    self._interval_overlaps(
                        first_gate_id,
                        intervals_by_gate[first_gate_id],
                        second_gate_id,
                        intervals_by_gate[second_gate_id],
                    )
                )
        return violations

    @staticmethod
    def _leader_intervals(
        ordered_logs: list[list[tuple]], run_end_time: float
    ) -> list[tuple[float, float]]:
        """Half-open ``[gain, loss)`` leadership intervals of one gate."""
        intervals: list[tuple[float, float]] = []
        for log_position, generation_log in enumerate(ordered_logs):
            held_since: float | None = None
            last_recorded_instant: float | None = None
            for row in generation_log:
                if (
                    row
                    and isinstance(row[-1], (int, float))
                    and not isinstance(row[-1], bool)
                ):
                    last_recorded_instant = row[-1]
                if not row or row[0] != "gate-leader":
                    continue
                _tag, leader_flag, instant = row
                if leader_flag == 1 and held_since is None:
                    held_since = instant
                elif leader_flag == 0 and held_since is not None:
                    intervals.append((held_since, instant))
                    held_since = None
            if held_since is None:
                continue
            if log_position == len(ordered_logs) - 1:
                intervals.append((held_since, run_end_time))
            else:
                # Power loss erased the exact loss instant; the last
                # recorded milestone is the latest instant the
                # surviving evidence supports the claim.
                claim_end = max(held_since, last_recorded_instant or held_since)
                intervals.append((held_since, claim_end))
        return intervals

    @staticmethod
    def _interval_overlaps(
        first_gate_id: str,
        first_intervals: list[tuple[float, float]],
        second_gate_id: str,
        second_intervals: list[tuple[float, float]],
    ) -> list[str]:
        return [
            f"gates {first_gate_id} and {second_gate_id} both held the "
            f"leader flag over "
            f"[{max(first_start, second_start):g}, "
            f"{min(first_end, second_end):g}) — leadership must be "
            "exclusive at every instant"
            for first_start, first_end in first_intervals
            for second_start, second_end in second_intervals
            if min(first_end, second_end) > max(first_start, second_start)
        ]

    # ------------------------------------------------------------------
    # Gate-leader convergence — the committed vopr_gates mirror
    # ------------------------------------------------------------------

    def check_gate_leader_convergence(
        self,
        results: Mapping[str, list[tuple]],
        ceiling: float,
        killed_process_ids: Iterable[str] = (),
    ) -> list[str]:
        """EVENTUAL leader convergence + terminal stability window —
        the semantics of ``vopr_gates``'s
        ``_check_gate_leader_convergence``, reusable by any suite:

        * exactly ONE surviving gate ends the run believing it is
          leader — zero means the tier wedged leaderless, two or more
          means split-brain;
        * no surviving gate's leader flag changes inside the final
          stability window — a late transition is churn, not recovery;
        * a surviving gate with no milestone log, or one that never
          reported a leader flag, is itself a violation (killed gates
          are skipped — their absence is kill semantics, not silence).
        """
        violations: list[str] = []
        killed = set(killed_process_ids)
        stability_deadline = ceiling - self._leader_stability_window_seconds

        final_leader_flags: dict[str, int] = {}
        for gate_process_id in self._gate_process_ids:
            if gate_process_id in killed:
                continue
            ordered_logs = self._generation_logs(results, gate_process_id)
            if not ordered_logs:
                violations.append(
                    f"gate {gate_process_id} produced no milestone log"
                )
                continue
            leader_transitions = self._rows(ordered_logs, "gate-leader")
            if not leader_transitions:
                violations.append(
                    f"gate {gate_process_id} never reported a leader flag"
                )
                continue
            final_leader_flags[gate_process_id] = leader_transitions[-1][1]
            late_transitions = [
                row for row in leader_transitions if row[2] > stability_deadline
            ]
            if late_transitions:
                violations.append(
                    f"gate {gate_process_id} leadership still moving inside "
                    f"the final {self._leader_stability_window_seconds:g}s "
                    f"stability window: {late_transitions}"
                )

        surviving_leader_count = sum(final_leader_flags.values())
        if final_leader_flags and surviving_leader_count != 1:
            violations.append(
                "surviving gate tier must converge to exactly one leader, "
                f"got {surviving_leader_count}: {final_leader_flags}"
            )
        return violations

    # ------------------------------------------------------------------
    # (b) Per-job client/server terminal agreement
    # ------------------------------------------------------------------

    def check_terminal_agreement(
        self,
        results: Mapping[str, list[tuple]],
        killed_process_ids: Iterable[str] = (),
    ) -> list[str]:
        """Client, manager, and gate terminal records must agree.

        Judged wherever the evidence EXISTS (server-side terminal
        milestones are the G3 node-side additions — logs without them
        pass vacuously): managers must agree among themselves, gates
        among themselves, gates with managers, and every server record
        with the client-observed terminal. Server-to-server checks run
        even when the client was SIGKILLed — silent wrongness that
        never reaches a client is exactly what this check exists for.
        The whole client history is treated as ONE job's stream; feed
        multi-job logs through ``JobLogSplitter`` first. Timeout
        spellings (``timeout``/``timed_out``) compare equal, and a
        server terminal row carrying non-terminal or unknown
        vocabulary is itself a violation — never swallowed.
        """
        manager_records, violations = self._server_terminal_records(
            results, self._manager_process_ids, "job-terminal"
        )
        gate_records, gate_vocabulary_violations = self._server_terminal_records(
            results, self._gate_process_ids, "gate-job-terminal"
        )
        violations.extend(gate_vocabulary_violations)

        distinct_manager_terminals = {
            status for _process_id, status in manager_records
        }
        if len(distinct_manager_terminals) > 1:
            violations.append(
                f"managers disagree on the job's terminal: {manager_records}"
            )
        distinct_gate_terminals = {
            status for _process_id, status in gate_records
        }
        if len(distinct_gate_terminals) > 1:
            violations.append(
                f"gates disagree on the job's terminal: {gate_records}"
            )
        if (
            len(distinct_manager_terminals) == 1
            and len(distinct_gate_terminals) == 1
            and distinct_manager_terminals != distinct_gate_terminals
        ):
            violations.append(
                "gate/manager terminal disagreement: "
                f"managers={manager_records} gates={gate_records}"
            )

        if self._client_process_id in set(killed_process_ids):
            return violations  # client evidence erased by SIGKILL

        client_terminal = self._client_terminal_status(results)
        if client_terminal is None:
            return violations  # no client terminal evidence to compare

        normalized_client_terminal = self._normalize_terminal(client_terminal)
        violations.extend(
            f"{process_id} recorded terminal {recorded_status!r} while the "
            f"client observed {client_terminal!r}"
            for process_id, recorded_status in manager_records + gate_records
            if recorded_status != normalized_client_terminal
        )
        return violations

    def _server_terminal_records(
        self,
        results: Mapping[str, list[tuple]],
        process_ids: tuple[str, ...],
        tag: str,
    ) -> tuple[list[tuple[str, str]], list[str]]:
        """``(process_id, normalized_terminal)`` records for ``tag``,
        plus violations for rows carrying non-terminal vocabulary."""
        records: list[tuple[str, str]] = []
        vocabulary_violations: list[str] = []
        for process_id in process_ids:
            for row in self._rows(self._generation_logs(results, process_id), tag):
                recorded_status = row[1]
                if not isinstance(recorded_status, str) or not self._order.is_terminal(
                    recorded_status
                ):
                    vocabulary_violations.append(
                        f"{process_id} recorded {tag} with non-terminal "
                        f"status {recorded_status!r} — unknown vocabulary "
                        "is never swallowed"
                    )
                    continue
                records.append(
                    (process_id, self._normalize_terminal(recorded_status))
                )
        return records, vocabulary_violations

    def _client_terminal_status(
        self, results: Mapping[str, list[tuple]]
    ) -> str | None:
        """The client-observed terminal: the delivered ``job-finished``
        status, else the last terminal ``status-seen``, else None."""
        client_logs = self._generation_logs(results, self._client_process_id)
        finished_rows = self._rows(client_logs, "job-finished")
        if finished_rows:
            return finished_rows[0][1]
        terminal_observations = [
            row[1]
            for row in self._rows(client_logs, "status-seen")
            if isinstance(row[1], str) and self._order.is_terminal(row[1])
        ]
        return terminal_observations[-1] if terminal_observations else None

    # ------------------------------------------------------------------
    # (c) Workflow execution counts
    # ------------------------------------------------------------------

    def check_workflow_execution(
        self,
        results: Mapping[str, list[tuple]],
        killed_process_ids: Iterable[str] = (),
    ) -> list[str]:
        """Execution starts: at least one for a completed job, at most
        ``retry_budget + 1`` where the scenario declares a budget.

        Starts are counted per worker generation from explicit
        ``workflow-executed``/``workflow-failed`` rows when present,
        else from positive jumps of the sampled ``workflows-active``
        count (each unit of increase is one start; the 0.25s sampling
        cadence can only UNDER-count, which keeps the upper bound
        sound). The >=1-for-completed demand is waived when execution
        evidence may be erased — any configured worker SIGKILLed or
        reporting no log (the committed multi-DC caveat: a dc_loss can
        destroy the log of a worker that already executed the job).
        The retry-budget bound needs no waiver: erasure only lowers
        the observed count.
        """
        violations: list[str] = []
        killed = set(killed_process_ids)
        execution_starts_by_datacenter = self._execution_starts_by_datacenter(
            results
        )
        total_execution_starts = sum(execution_starts_by_datacenter.values())

        if (
            self._retry_budget is not None
            and total_execution_starts > self._retry_budget + 1
        ):
            violations.append(
                f"{total_execution_starts} workflow execution starts exceed "
                f"the declared retry budget ({self._retry_budget} retries + "
                "the first attempt)"
            )

        client_terminal = self._client_terminal_status(results)
        client_completed = (
            client_terminal is not None
            and self._normalize_terminal(client_terminal) == "completed"
        )
        configured_worker_ids = [
            worker_process_id
            for worker_process_ids in self._worker_process_ids_by_datacenter.values()
            for worker_process_id in worker_process_ids
        ]
        execution_evidence_intact = all(
            worker_process_id not in killed
            and bool(self._generation_logs(results, worker_process_id))
            for worker_process_id in configured_worker_ids
        )
        if (
            client_completed
            and configured_worker_ids
            and execution_evidence_intact
            and total_execution_starts == 0
        ):
            violations.append(
                "client observed completion but no worker shows a workflow "
                "execution start"
            )
        return violations

    def _execution_starts_by_datacenter(
        self, results: Mapping[str, list[tuple]]
    ) -> dict[str, int]:
        return {
            datacenter_id: sum(
                self._execution_starts_in_log(generation_log)
                for worker_process_id in worker_process_ids
                for generation_log in self._generation_logs(
                    results, worker_process_id
                )
            )
            for datacenter_id, worker_process_ids in (
                self._worker_process_ids_by_datacenter.items()
            )
        }

    @staticmethod
    def _execution_starts_in_log(generation_log: list[tuple]) -> int:
        """Execution starts one worker generation's log evidences."""
        explicit_attempts = sum(
            1
            for row in generation_log
            if row and row[0] in _EXPLICIT_ATTEMPT_TAGS
        )
        if explicit_attempts:
            return explicit_attempts

        starts = 0
        previous_active_count = 0
        for row in generation_log:
            if not row or row[0] != "workflows-active":
                continue
            active_count = row[1]
            if active_count > previous_active_count:
                starts += active_count - previous_active_count
            previous_active_count = active_count
        return starts

    # ------------------------------------------------------------------
    # (d) Single-datacenter placement
    # ------------------------------------------------------------------

    def check_single_datacenter_placement(
        self, results: Mapping[str, list[tuple]]
    ) -> list[str]:
        """A single-DC job executes in at most ONE datacenter — the
        committed ``vopr_mdc`` exactly-once-placement semantics
        (production has deliberately no mid-flight cross-DC
        re-dispatch). Erasure needs no waiver here: a SIGKILLed
        worker's missing log can only HIDE an execution, never invent
        one, so the at-most-one bound stays sound.
        """
        executing_datacenters = sorted(
            datacenter_id
            for datacenter_id, execution_starts in (
                self._execution_starts_by_datacenter(results).items()
            )
            if execution_starts > 0
        )
        if len(executing_datacenters) > 1:
            return [
                "single-DC job executed in multiple datacenters "
                f"{executing_datacenters} — placement must be exactly-once"
            ]
        return []

    # ------------------------------------------------------------------
    # (e) Datacenter-health convergence — the committed vopr_mdc mirror
    # ------------------------------------------------------------------

    def check_datacenter_health_convergence(
        self,
        results: Mapping[str, list[tuple]],
        expected_health_by_datacenter: Mapping[str, str],
        killed_process_ids: Iterable[str] = (),
    ) -> list[str]:
        """Every surviving gate's FINAL classification of every
        datacenter equals the expectation the schedule implies
        (``vopr_mdc`` semantics: ``unhealthy`` for a lost DC,
        ``healthy`` for every other once faults ended — transient
        flaps are legitimate, final divergence is not). A surviving
        gate with no classification for an expected datacenter is a
        violation; SIGKILLed gates are skipped.
        """
        violations: list[str] = []
        killed = set(killed_process_ids)
        for gate_process_id in self._gate_process_ids:
            if gate_process_id in killed:
                continue
            final_health = self._final_health_by_datacenter(
                self._rows(
                    self._generation_logs(results, gate_process_id), "dc-health"
                ),
                expected_health_by_datacenter,
            )
            violations.extend(
                f"gate {gate_process_id} final classification of "
                f"{datacenter_id} is {final_health.get(datacenter_id)!r}, "
                f"expected {expected_health!r}"
                for datacenter_id, expected_health in sorted(
                    expected_health_by_datacenter.items()
                )
                if final_health.get(datacenter_id) != expected_health
            )
        return violations

    @staticmethod
    def _final_health_by_datacenter(
        health_rows: list[tuple],
        expected_health_by_datacenter: Mapping[str, str],
    ) -> dict[str, str]:
        """Last classification per datacenter. Supports both live row
        shapes: ``("dc-health", dc_id, health, t)`` and the single-DC
        positional ``("dc-health", health, t)`` — the latter only when
        exactly one datacenter is expected (anything else is ambiguous
        evidence, a caller error raised loudly)."""
        final_health: dict[str, str] = {}
        for row in health_rows:
            if len(row) == 4:
                final_health[row[1]] = row[2]
            elif len(row) == 3 and len(expected_health_by_datacenter) == 1:
                (sole_datacenter_id,) = expected_health_by_datacenter
                final_health[sole_datacenter_id] = row[1]
            else:
                raise ValueError(
                    f"dc-health row {row!r} carries no datacenter id but "
                    f"{len(expected_health_by_datacenter)} datacenters are "
                    "expected — ambiguous evidence"
                )
        return final_health

    # ------------------------------------------------------------------
    # Determinism-audit absence — the G4 helper
    # ------------------------------------------------------------------

    @staticmethod
    def check_determinism_audit_absence(
        results: Mapping[str, list[tuple]],
    ) -> list[str]:
        """No child result (any generation) may carry a
        ``("determinism-audit-unswapped", [modules])`` row — a live
        wall-coupled nondeterminism source. Every process's rows are
        checked, exactly the committed VOPR runners' rule."""
        return [
            f"determinism audit: {process_id} carries unswapped seams "
            f"{row[1:]}"
            for process_id, process_log in results.items()
            if isinstance(process_log, list)
            for row in process_log
            if row and row[0] == _AUDIT_TAG
        ]

    # ------------------------------------------------------------------
    # Composite
    # ------------------------------------------------------------------

    def check_cluster_trace(
        self,
        results: Mapping[str, list[tuple]],
        killed_process_ids: Iterable[str] = (),
        expected_health_by_datacenter: Mapping[str, str] | None = None,
        ceiling: float | None = None,
        run_end_time: float | None = None,
    ) -> list[str]:
        """Run the full cross-node check set on one run's results.

        Always: audit absence, gate-leader exclusivity, terminal
        agreement, workflow execution counts. When more than one
        datacenter is configured: single-DC placement. When
        ``expected_health_by_datacenter`` is given: health
        convergence. When ``ceiling`` is given: leader convergence +
        stability window.
        """
        killed = tuple(killed_process_ids)
        violations = list(self.check_determinism_audit_absence(results))
        violations.extend(
            self.check_gate_leader_exclusivity(results, killed, run_end_time)
        )
        violations.extend(self.check_terminal_agreement(results, killed))
        violations.extend(self.check_workflow_execution(results, killed))
        if len(self._worker_process_ids_by_datacenter) > 1:
            violations.extend(self.check_single_datacenter_placement(results))
        if expected_health_by_datacenter is not None:
            violations.extend(
                self.check_datacenter_health_convergence(
                    results, expected_health_by_datacenter, killed
                )
            )
        if ceiling is not None:
            violations.extend(
                self.check_gate_leader_convergence(results, ceiling, killed)
            )
        return violations
