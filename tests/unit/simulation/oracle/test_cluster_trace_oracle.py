"""
ClusterTraceOracle — the G3 cross-node checker's self-tests, G6 style:
every check is proven able to FAIL by a synthetic VIOLATING history
(overlapping leader intervals, manager-completed-vs-client-failed,
completion with no execution evidence, two-DC execution, diverged
final health, a planted determinism-audit row), and proven tolerant of
ERASED evidence (SIGKILLed children never report). Never canaried via
production-code mutation — the violating traces are synthetic result
dicts in the coordinator's exact output shape, including ``.genN``
generation keys.
"""

import pytest

from tests.simulation.oracle import ClusterTraceOracle


def _gate_topology_oracle(**overrides) -> ClusterTraceOracle:
    configuration = {
        "client_process_id": "client",
        "gate_process_ids": ("gate-a", "gate-b", "gate-c"),
        "manager_process_ids": ("manager-east", "manager-west"),
        "worker_process_ids_by_datacenter": {
            "dc-east": ("worker-east",),
            "dc-west": ("worker-west",),
        },
    }
    configuration.update(overrides)
    return ClusterTraceOracle(**configuration)


class TestGateLeaderExclusivity:
    def test_clean_handoff_passes(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [
                ("gate-leader", 0, 1.0),
                ("gate-leader", 1, 5.0),
                ("gate-leader", 0, 10.0),
            ],
            "gate-b": [
                ("gate-leader", 0, 1.0),
                ("gate-leader", 1, 10.0),
            ],
            "gate-c": [("gate-leader", 0, 1.0), ("gate-peers", 2, 40.0)],
        }
        assert oracle.check_gate_leader_exclusivity(results) == []

    def test_overlapping_leader_intervals_fire(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [
                ("gate-leader", 1, 5.0),
                ("gate-leader", 0, 20.0),
            ],
            "gate-b": [
                ("gate-leader", 1, 15.0),
                ("gate-leader", 0, 25.0),
            ],
        }
        violations = oracle.check_gate_leader_exclusivity(results)
        assert len(violations) == 1
        assert "gate-a" in violations[0]
        assert "gate-b" in violations[0]
        assert "exclusive" in violations[0]

    def test_two_final_leaders_overlap_to_run_end_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 1, 5.0)],
            "gate-b": [("gate-leader", 1, 50.0)],
            "gate-c": [("gate-leader", 0, 1.0), ("gate-peers", 2, 90.0)],
        }
        violations = oracle.check_gate_leader_exclusivity(results)
        assert len(violations) == 1
        assert "[50, 90)" in violations[0]

    def test_killed_gate_contributes_no_intervals(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 1, 5.0)],
        }
        assert (
            oracle.check_gate_leader_exclusivity(
                results, killed_process_ids=("gate-b", "gate-c")
            )
            == []
        )

    def test_generation_boundary_closes_open_interval(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            # gen1 power-lost while leader: the claim closes at the
            # generation's last recorded instant (12.0), not run end.
            "gate-a.gen1": [
                ("gate-leader", 1, 10.0),
                ("gate-peers", 2, 12.0),
            ],
            "gate-a": [("gate-leader", 0, 40.0)],
            "gate-b": [
                ("gate-leader", 1, 13.0),
                ("gate-leader", 0, 60.0),
            ],
        }
        assert oracle.check_gate_leader_exclusivity(results) == []

    def test_generation_interval_overlap_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a.gen1": [
                ("gate-leader", 1, 10.0),
                ("gate-peers", 2, 12.0),
            ],
            "gate-a": [("gate-leader", 0, 40.0)],
            "gate-b": [
                ("gate-leader", 1, 11.0),
                ("gate-leader", 0, 60.0),
            ],
        }
        violations = oracle.check_gate_leader_exclusivity(results)
        assert len(violations) == 1
        assert "[11, 12)" in violations[0]

    def test_explicit_run_end_time_bounds_open_intervals(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 1, 5.0)],
            "gate-b": [("gate-leader", 1, 50.0)],
        }
        assert (
            oracle.check_gate_leader_exclusivity(results, run_end_time=40.0)
            == []
        )


class TestGateLeaderConvergence:
    def test_single_final_leader_passes(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 0, 1.0), ("gate-leader", 1, 5.0)],
            "gate-b": [("gate-leader", 0, 1.0)],
            "gate-c": [("gate-leader", 0, 1.0)],
        }
        assert oracle.check_gate_leader_convergence(results, ceiling=100.0) == []

    def test_zero_final_leaders_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 0, 1.0)],
            "gate-b": [("gate-leader", 0, 1.0)],
            "gate-c": [("gate-leader", 0, 1.0)],
        }
        violations = oracle.check_gate_leader_convergence(results, ceiling=100.0)
        assert len(violations) == 1
        assert "exactly one leader" in violations[0]
        assert "got 0" in violations[0]

    def test_two_final_leaders_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 1, 5.0)],
            "gate-b": [("gate-leader", 1, 6.0)],
            "gate-c": [("gate-leader", 0, 1.0)],
        }
        violations = oracle.check_gate_leader_convergence(results, ceiling=100.0)
        assert len(violations) == 1
        assert "got 2" in violations[0]

    def test_late_transition_inside_stability_window_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 0, 1.0), ("gate-leader", 1, 85.0)],
            "gate-b": [("gate-leader", 0, 1.0)],
            "gate-c": [("gate-leader", 0, 1.0)],
        }
        violations = oracle.check_gate_leader_convergence(results, ceiling=100.0)
        assert len(violations) == 1
        assert "stability window" in violations[0]

    def test_killed_gate_skipped_and_missing_log_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 0, 1.0), ("gate-leader", 1, 5.0)],
        }
        violations = oracle.check_gate_leader_convergence(
            results, ceiling=100.0, killed_process_ids=("gate-b",)
        )
        assert len(violations) == 1
        assert "gate-c" in violations[0]
        assert "no milestone log" in violations[0]

    def test_gate_with_no_leader_rows_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "gate-a": [("gate-leader", 1, 5.0)],
            "gate-b": [("gate-started", 1.0)],
            "gate-c": [("gate-leader", 0, 1.0)],
        }
        violations = oracle.check_gate_leader_convergence(results, ceiling=100.0)
        assert len(violations) == 1
        assert "never reported a leader flag" in violations[0]


class TestTerminalAgreement:
    def test_full_agreement_passes(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
            "gate-a": [("gate-job-terminal", "completed", 21.5)],
            "client": [
                ("status-seen", "running", 9.0),
                ("job-finished", "completed", 22.0),
            ],
        }
        assert oracle.check_terminal_agreement(results) == []

    def test_manager_completed_client_failed_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
            "client": [
                ("status-seen", "running", 9.0),
                ("job-finished", "failed", 22.0),
            ],
        }
        violations = oracle.check_terminal_agreement(results)
        assert len(violations) == 1
        assert "manager-east" in violations[0]
        assert "'completed'" in violations[0]
        assert "'failed'" in violations[0]

    def test_gate_manager_disagreement_fires_without_client(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
            "gate-a": [("gate-job-terminal", "failed", 21.5)],
        }
        violations = oracle.check_terminal_agreement(
            results, killed_process_ids=("client",)
        )
        assert len(violations) == 1
        assert "gate/manager terminal disagreement" in violations[0]

    def test_timeout_spellings_are_equivalent(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "timeout", 40.0)],
            "gate-a": [("gate-job-terminal", "timed_out", 40.5)],
            "client": [("job-finished", "timeout", 41.0)],
        }
        assert oracle.check_terminal_agreement(results) == []

    def test_unknown_server_terminal_vocabulary_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "exploded", 21.0)],
            "client": [("job-finished", "completed", 22.0)],
        }
        violations = oracle.check_terminal_agreement(results)
        assert len(violations) == 1
        assert "non-terminal" in violations[0]
        assert "'exploded'" in violations[0]

    def test_managers_disagreeing_with_each_other_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
            "manager-west": [("job-terminal", "failed", 21.5)],
        }
        violations = oracle.check_terminal_agreement(
            results, killed_process_ids=("client",)
        )
        assert len(violations) == 1
        assert "managers disagree" in violations[0]

    def test_no_server_milestones_is_vacuous(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("manager-started", 2.0)],
            "gate-a": [("gate-started", 1.0)],
            "client": [("job-finished", "completed", 22.0)],
        }
        assert oracle.check_terminal_agreement(results) == []

    def test_killed_client_skips_client_comparison(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
        }
        assert (
            oracle.check_terminal_agreement(
                results, killed_process_ids=("client",)
            )
            == []
        )

    def test_generation_terminal_rows_are_seen(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            # The disagreeing record lives in the manager's PRE-restart
            # generation — generation keys are part of the trace.
            "manager-east.gen1": [("job-terminal", "completed", 21.0)],
            "manager-east": [("manager-started", 30.0)],
            "client": [("job-finished", "failed", 35.0)],
        }
        violations = oracle.check_terminal_agreement(results)
        assert len(violations) == 1
        assert "manager-east" in violations[0]

    def test_client_terminal_from_status_seen_when_no_finish(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east": [("job-terminal", "completed", 21.0)],
            "client": [
                ("status-seen", "running", 9.0),
                ("status-seen", "failed", 22.0),
            ],
        }
        violations = oracle.check_terminal_agreement(results)
        assert len(violations) == 1
        assert "'failed'" in violations[0]


class TestWorkflowExecution:
    def test_completed_with_execution_passes(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
                ("workflows-active", 0, 20.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [("job-finished", "completed", 21.0)],
        }
        assert oracle.check_workflow_execution(results) == []

    def test_completed_without_execution_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [("workflows-active", 0, 2.5)],
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [("job-finished", "completed", 21.0)],
        }
        violations = oracle.check_workflow_execution(results)
        assert len(violations) == 1
        assert "no worker shows a workflow execution start" in violations[0]

    def test_erased_worker_evidence_suppresses_requirement(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            # worker-east SIGKILLed after executing: its log never
            # reports — the committed multi-DC caveat.
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [("job-finished", "completed", 21.0)],
        }
        assert (
            oracle.check_workflow_execution(
                results, killed_process_ids=("worker-east",)
            )
            == []
        )

    def test_retry_budget_exceeded_fires(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=1)
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
                ("workflows-active", 0, 12.0),
                ("workflows-active", 1, 15.0),
                ("workflows-active", 0, 18.0),
                ("workflows-active", 1, 21.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [("job-finished", "completed", 25.0)],
        }
        violations = oracle.check_workflow_execution(results)
        assert len(violations) == 1
        assert "exceed the declared retry budget" in violations[0]

    def test_retry_budget_respected_passes(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=1)
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
                ("workflows-active", 0, 12.0),
                ("workflows-active", 1, 15.0),
                ("workflows-active", 0, 18.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [("job-finished", "completed", 25.0)],
        }
        assert oracle.check_workflow_execution(results) == []

    def test_explicit_workflow_executed_rows_counted(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=1)
        results = {
            "worker-east": [
                ("workflow-executed", "SimSoakWorkflow", 9.0),
                ("workflow-failed", "SimSoakWorkflow", 15.0),
                ("workflow-executed", "SimSoakWorkflow", 21.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
        }
        violations = oracle.check_workflow_execution(results)
        assert len(violations) == 1
        assert "3 workflow execution starts" in violations[0]

    def test_restart_generation_starts_accumulate(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=0)
        results = {
            "worker-east.gen1": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
            ],
            "worker-east": [
                ("workflows-active", 1, 30.0),
                ("workflows-active", 0, 40.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
        }
        violations = oracle.check_workflow_execution(results)
        assert len(violations) == 1
        assert "2 workflow execution starts" in violations[0]

    def test_active_count_jump_counts_each_start(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=0)
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 2, 9.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
        }
        violations = oracle.check_workflow_execution(results)
        assert len(violations) == 1
        assert "2 workflow execution starts" in violations[0]


class TestSingleDatacenterPlacement:
    def test_single_datacenter_execution_passes(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
        }
        assert oracle.check_single_datacenter_placement(results) == []

    def test_two_datacenter_execution_fires(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
            ],
            "worker-west": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.5),
            ],
        }
        violations = oracle.check_single_datacenter_placement(results)
        assert len(violations) == 1
        assert "multiple datacenters" in violations[0]
        assert "dc-east" in violations[0]
        assert "dc-west" in violations[0]

    def test_no_execution_evidence_passes(self) -> None:
        oracle = _gate_topology_oracle()
        assert oracle.check_single_datacenter_placement({}) == []


class TestDatacenterHealthConvergence:
    def test_expected_final_health_passes(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a",))
        results = {
            "gate-a": [
                ("dc-health", "dc-east", "healthy", 6.0),
                ("dc-health", "dc-west", "healthy", 6.0),
                ("dc-health", "dc-east", "unhealthy", 45.0),
            ],
        }
        assert (
            oracle.check_datacenter_health_convergence(
                results,
                {"dc-east": "unhealthy", "dc-west": "healthy"},
            )
            == []
        )

    def test_diverged_final_health_fires(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a",))
        results = {
            "gate-a": [
                ("dc-health", "dc-east", "healthy", 6.0),
                ("dc-health", "dc-east", "unhealthy", 45.0),
            ],
        }
        violations = oracle.check_datacenter_health_convergence(
            results, {"dc-east": "healthy"}
        )
        assert len(violations) == 1
        assert "final classification" in violations[0]
        assert "'unhealthy'" in violations[0]

    def test_missing_classification_fires(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a",))
        results = {"gate-a": [("gate-started", 1.0)]}
        violations = oracle.check_datacenter_health_convergence(
            results, {"dc-east": "healthy"}
        )
        assert len(violations) == 1
        assert "None" in violations[0]

    def test_killed_gate_skipped(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a", "gate-b"))
        results = {
            "gate-a": [("dc-health", "dc-east", "healthy", 6.0)],
        }
        assert (
            oracle.check_datacenter_health_convergence(
                results,
                {"dc-east": "healthy"},
                killed_process_ids=("gate-b",),
            )
            == []
        )

    def test_positional_single_dc_rows_supported(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a",))
        results = {
            # worker_manager_demo.gate_entry's single-DC shape:
            # ("dc-health", health, t) with no datacenter id.
            "gate-a": [
                ("dc-health", "initializing", 1.0),
                ("dc-health", "healthy", 6.0),
            ],
        }
        assert (
            oracle.check_datacenter_health_convergence(
                results, {"dc-east": "healthy"}
            )
            == []
        )

    def test_positional_rows_with_multiple_datacenters_raise(self) -> None:
        oracle = _gate_topology_oracle(gate_process_ids=("gate-a",))
        results = {"gate-a": [("dc-health", "healthy", 6.0)]}
        with pytest.raises(ValueError, match="ambiguous evidence"):
            oracle.check_datacenter_health_convergence(
                results, {"dc-east": "healthy", "dc-west": "healthy"}
            )


class TestDeterminismAuditAbsence:
    def test_planted_audit_row_is_reported(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [
                ("worker-started", 2.0),
                ("determinism-audit-unswapped", ["hyperscale.taskex"]),
            ],
            "client": [("job-finished", "completed", 21.0)],
        }
        violations = oracle.check_determinism_audit_absence(results)
        assert len(violations) == 1
        assert "worker-east" in violations[0]
        assert "hyperscale.taskex" in violations[0]

    def test_clean_results_pass(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "worker-east": [("worker-started", 2.0)],
            "client": [("job-finished", "completed", 21.0)],
        }
        assert oracle.check_determinism_audit_absence(results) == []

    def test_audit_row_in_generation_log_is_reported(self) -> None:
        oracle = _gate_topology_oracle()
        results = {
            "manager-east.gen1": [
                ("determinism-audit-unswapped", ["hyperscale.logging"]),
            ],
            "manager-east": [("manager-started", 30.0)],
        }
        violations = oracle.check_determinism_audit_absence(results)
        assert len(violations) == 1
        assert "manager-east.gen1" in violations[0]


class TestClusterTraceComposite:
    def test_clean_trace_passes_every_check(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=1)
        results = {
            "gate-a": [
                ("gate-started", 1.0),
                ("gate-leader", 0, 1.0),
                ("gate-leader", 1, 4.0),
                ("dc-health", "dc-east", "healthy", 6.0),
                ("dc-health", "dc-west", "healthy", 6.0),
                ("gate-job-terminal", "completed", 21.5),
            ],
            "gate-b": [
                ("gate-leader", 0, 1.1),
                ("dc-health", "dc-east", "healthy", 6.1),
                ("dc-health", "dc-west", "healthy", 6.1),
            ],
            "gate-c": [
                ("gate-leader", 0, 1.2),
                ("dc-health", "dc-east", "healthy", 6.2),
                ("dc-health", "dc-west", "healthy", 6.2),
            ],
            "manager-east": [
                ("manager-started", 2.0),
                ("worker-registered", 5.0),
                ("job-terminal", "completed", 21.0),
            ],
            "manager-west": [("manager-started", 2.0)],
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
                ("workflows-active", 0, 20.0),
            ],
            "worker-west": [("workflows-active", 0, 2.5)],
            "client": [
                ("job-submitted", 8.0),
                ("status-seen", "running", 9.5),
                ("job-finished", "completed", 22.0),
            ],
        }
        assert (
            oracle.check_cluster_trace(
                results,
                expected_health_by_datacenter={
                    "dc-east": "healthy",
                    "dc-west": "healthy",
                },
                ceiling=100.0,
            )
            == []
        )

    def test_composite_names_every_planted_violation(self) -> None:
        oracle = _gate_topology_oracle(retry_budget=0)
        results = {
            "gate-a": [
                ("gate-leader", 1, 4.0),
                ("dc-health", "dc-east", "unhealthy", 6.0),
                ("dc-health", "dc-west", "healthy", 6.0),
                ("gate-job-terminal", "failed", 21.5),
            ],
            "gate-b": [("gate-leader", 1, 10.0)],
            "gate-c": [("gate-leader", 0, 1.2)],
            "manager-east": [
                ("job-terminal", "completed", 21.0),
                ("determinism-audit-unswapped", ["hyperscale.taskex"]),
            ],
            "manager-west": [("manager-started", 2.0)],
            "worker-east": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.0),
            ],
            "worker-west": [
                ("workflows-active", 0, 2.5),
                ("workflows-active", 1, 9.5),
            ],
            "client": [("job-finished", "failed", 22.0)],
        }
        violations = oracle.check_cluster_trace(
            results,
            expected_health_by_datacenter={
                "dc-east": "healthy",
                "dc-west": "healthy",
            },
            ceiling=100.0,
        )
        violation_text = "\n".join(violations)
        assert "determinism audit" in violation_text
        assert "exclusive at every instant" in violation_text
        assert "gate/manager terminal disagreement" in violation_text
        assert "while the client observed" in violation_text
        assert "exceed the declared retry budget" in violation_text
        assert "multiple datacenters" in violation_text
        assert "final classification" in violation_text
        assert "exactly one leader" in violation_text
