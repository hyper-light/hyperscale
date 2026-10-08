"""
JobStatusOracle — the history checker itself.

Self-tests for the oracle's judgments: legal histories (with skips)
pass; regressions, post-terminal observations, finished/observed
disagreement, non-terminal finishes, and double result delivery are
each named violations. The oracle judges what the client SAW — these
are the invariants every VOPR schedule is now held to.
"""

from tests.simulation.oracle import JobStatusOracle


class TestLegalHistories:
    def test_full_lifecycle_linearizes(self) -> None:
        oracle = JobStatusOracle()
        assert (
            oracle.check_history(
                [
                    "submitted",
                    "queued",
                    "dispatching",
                    "running",
                    "completing",
                    "completed",
                ],
                finished_status="completed",
            )
            == []
        )

    def test_forward_skips_are_legal(self) -> None:
        oracle = JobStatusOracle()
        assert (
            oracle.check_history(
                ["submitted", "running", "completed"],
                finished_status="completed",
            )
            == []
        )

    def test_repeated_same_status_is_legal(self) -> None:
        oracle = JobStatusOracle()
        assert (
            oracle.check_history(["running", "running", "completed"]) == []
        )

    def test_faulted_outcome_is_legal_when_terminal(self) -> None:
        oracle = JobStatusOracle()
        assert (
            oracle.check_history(
                ["submitted", "running", "failed"], finished_status="failed"
            )
            == []
        )


class TestViolations:
    def test_rank_regression_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(["running", "submitted"])
        assert len(violations) == 1
        assert "regressed" in violations[0]

    def test_observation_after_terminal_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(["completed", "running"])
        assert len(violations) == 1
        assert "absorbing" in violations[0]

    def test_terminal_to_different_terminal_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(["completed", "cancelled"])
        assert len(violations) == 1
        assert "absorbing" in violations[0]

    def test_unknown_status_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(["running", "exploded"])
        assert len(violations) == 1
        assert "unknown status" in violations[0]

    def test_finished_disagreeing_with_observed_terminal(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(
            ["running", "completed"], finished_status="failed"
        )
        assert len(violations) == 1
        assert "disagrees" in violations[0]

    def test_non_terminal_finish_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        violations = oracle.check_history(
            ["running"], finished_status="running"
        )
        assert len(violations) == 1
        assert "non-terminal" in violations[0]


class TestClientLogAdapter:
    def test_milestone_log_shape(self) -> None:
        oracle = JobStatusOracle()
        client_log = [
            ("job-submitted", 8.2),
            ("status-seen", "submitted", 8.2),
            ("status-seen", "running", 8.7),
            ("job-finished", "completed", 8.8),
        ]
        assert oracle.check_client_log(client_log) == []

    def test_double_finish_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        client_log = [
            ("status-seen", "running", 1.0),
            ("job-finished", "completed", 2.0),
            ("job-finished", "completed", 3.0),
        ]
        violations = oracle.check_client_log(client_log)
        assert len(violations) == 1
        assert "exactly-once" in violations[0]

    def test_regression_in_log_is_flagged(self) -> None:
        oracle = JobStatusOracle()
        client_log = [
            ("status-seen", "running", 1.0),
            ("status-seen", "submitted", 1.5),
            ("job-finished", "completed", 2.0),
        ]
        violations = oracle.check_client_log(client_log)
        assert len(violations) == 1
        assert "regressed" in violations[0]
