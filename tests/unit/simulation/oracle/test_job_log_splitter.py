"""
JobLogSplitter — the K2 per-job adapter's self-tests, G6 style: legal
prefixed logs split into streams ``JobStatusOracle`` accepts; a
violating per-job stream is proven to FIRE through the split (the
mis-split canary — a regression inside job 2's stream is attributed to
job 2); near-miss prefixes that would silently mis-split are reported;
unjudgeable rows raise. Single-job unprefixed logs must pass through
as job 0 byte-identical — the committed suites' shape.
"""

import pytest

from tests.simulation.oracle import JobLogSplitter, JobStatusOracle


class TestSplit:
    def test_unprefixed_log_passes_through_as_job_zero(self) -> None:
        splitter = JobLogSplitter()
        client_log = [
            ("job-submitted", 8.2),
            ("status-seen", "running", 8.7),
            ("job-finished", "completed", 8.8),
        ]
        assert splitter.split(client_log) == {0: client_log}

    def test_prefixed_rows_split_by_index_with_prefix_stripped(self) -> None:
        splitter = JobLogSplitter()
        client_log = [
            ("job1-job-submitted", 8.0),
            ("job2-job-submitted", 8.5),
            ("job1-status-seen", "running", 9.0),
            ("job2-status-seen", "running", 9.5),
            ("job1-job-finished", "completed", 12.0),
            ("job2-job-finished", "completed", 13.0),
        ]
        assert splitter.split(client_log) == {
            1: [
                ("job-submitted", 8.0),
                ("status-seen", "running", 9.0),
                ("job-finished", "completed", 12.0),
            ],
            2: [
                ("job-submitted", 8.5),
                ("status-seen", "running", 9.5),
                ("job-finished", "completed", 13.0),
            ],
        }

    def test_job_lifecycle_tags_are_not_prefixes(self) -> None:
        splitter = JobLogSplitter()
        client_log = [
            ("job-submitted", 8.0),
            ("job-finished", "completed", 12.0),
        ]
        # "job-" carries no digits: these are lifecycle tags, not
        # prefixes — they must stay unsplit and unstripped.
        assert splitter.split(client_log) == {0: client_log}

    def test_multi_digit_indices(self) -> None:
        splitter = JobLogSplitter()
        streams = splitter.split([("job10-status-seen", "running", 9.0)])
        assert streams == {10: [("status-seen", "running", 9.0)]}

    def test_leading_zero_index(self) -> None:
        splitter = JobLogSplitter()
        streams = splitter.split([("job03-status-seen", "running", 9.0)])
        assert streams == {3: [("status-seen", "running", 9.0)]}

    def test_ambient_rows_route_to_job_zero(self) -> None:
        splitter = JobLogSplitter()
        client_log = [
            ("client-error", "ValueError", 1.0),
            ("job1-status-seen", "running", 9.0),
        ]
        assert splitter.split(client_log) == {
            0: [("client-error", "ValueError", 1.0)],
            1: [("status-seen", "running", 9.0)],
        }

    def test_order_preserved_within_streams(self) -> None:
        splitter = JobLogSplitter()
        client_log = [
            ("job1-status-seen", "submitted", 8.0),
            ("job2-status-seen", "submitted", 8.1),
            ("job1-status-seen", "running", 9.0),
            ("job2-status-seen", "running", 9.1),
        ]
        streams = splitter.split(client_log)
        assert [row[1] for row in streams[1]] == ["submitted", "running"]
        assert [row[1] for row in streams[2]] == ["submitted", "running"]

    def test_empty_log_yields_no_streams(self) -> None:
        assert JobLogSplitter().split([]) == {}

    def test_split_streams_feed_job_status_oracle(self) -> None:
        splitter = JobLogSplitter()
        oracle = JobStatusOracle()
        client_log = [
            ("job1-status-seen", "submitted", 8.0),
            ("job2-status-seen", "submitted", 8.1),
            ("job1-status-seen", "running", 9.0),
            ("job1-job-finished", "completed", 12.0),
            ("job2-status-seen", "failed", 10.0),
            ("job2-job-finished", "failed", 10.1),
        ]
        streams = splitter.split(client_log)
        assert oracle.check_client_log(streams[1]) == []
        assert oracle.check_client_log(streams[2]) == []


class TestRowValidation:
    def test_non_tuple_row_raises(self) -> None:
        with pytest.raises(ValueError, match="no string tag"):
            JobLogSplitter().split([["status-seen", "running", 9.0]])

    def test_empty_row_raises(self) -> None:
        with pytest.raises(ValueError, match="no string tag"):
            JobLogSplitter().split([()])

    def test_non_string_tag_raises(self) -> None:
        with pytest.raises(ValueError, match="no string tag"):
            JobLogSplitter().split([(42, "running", 9.0)])


class TestPrefixHygiene:
    def test_bare_job_index_tag_flagged(self) -> None:
        violations = JobLogSplitter().check_prefix_hygiene([("job3", 1.0)])
        assert len(violations) == 1
        assert "mis-split" in violations[0]

    def test_missing_dash_flagged(self) -> None:
        violations = JobLogSplitter().check_prefix_hygiene(
            [("job3status-seen", "running", 9.0)]
        )
        assert len(violations) == 1
        assert "'job3status-seen'" in violations[0]

    def test_empty_rest_flagged(self) -> None:
        violations = JobLogSplitter().check_prefix_hygiene([("job3-", 1.0)])
        assert len(violations) == 1
        assert "mis-split" in violations[0]

    def test_wellformed_and_lifecycle_tags_not_flagged(self) -> None:
        assert (
            JobLogSplitter().check_prefix_hygiene(
                [
                    ("job1-status-seen", "running", 9.0),
                    ("job-submitted", 8.0),
                    ("job-finished", "completed", 12.0),
                    ("status-seen", "running", 8.7),
                ]
            )
            == []
        )

    def test_job_zero_prefix_mixed_with_unprefixed_lifecycle_flagged(
        self,
    ) -> None:
        violations = JobLogSplitter().check_prefix_hygiene(
            [
                ("job0-status-seen", "running", 9.0),
                ("job-finished", "completed", 12.0),
            ]
        )
        assert len(violations) == 1
        assert "ambiguous evidence" in violations[0]


class TestSplitAndCheck:
    def test_violating_stream_attributed_to_its_job(self) -> None:
        # The mis-split canary: job 2's history REGRESSES; the
        # violation must fire and name job 2, while job 1 stays clean.
        violations = JobLogSplitter().split_and_check(
            [
                ("job1-status-seen", "submitted", 8.0),
                ("job2-status-seen", "running", 8.1),
                ("job1-status-seen", "running", 9.0),
                ("job2-status-seen", "submitted", 9.1),
                ("job1-job-finished", "completed", 12.0),
                ("job2-job-finished", "completed", 13.0),
            ]
        )
        assert len(violations) == 1
        assert violations[0].startswith("job 2: ")
        assert "regressed" in violations[0]

    def test_double_finish_within_one_job_fires(self) -> None:
        violations = JobLogSplitter().split_and_check(
            [
                ("job1-status-seen", "running", 9.0),
                ("job1-job-finished", "completed", 12.0),
                ("job1-job-finished", "completed", 13.0),
            ]
        )
        assert len(violations) == 1
        assert violations[0].startswith("job 1: ")
        assert "exactly-once" in violations[0]

    def test_hygiene_violations_included(self) -> None:
        violations = JobLogSplitter().split_and_check(
            [
                ("job2status-seen", "running", 9.0),
            ]
        )
        assert len(violations) == 1
        assert "mis-split" in violations[0]

    def test_clean_multi_job_log_passes(self) -> None:
        assert (
            JobLogSplitter().split_and_check(
                [
                    ("job1-job-submitted", 8.0),
                    ("job2-job-submitted", 8.5),
                    ("job1-status-seen", "running", 9.0),
                    ("job2-status-seen", "running", 9.5),
                    ("job1-job-finished", "completed", 12.0),
                    ("job2-job-finished", "failed", 13.0),
                ]
            )
            == []
        )

    def test_unprefixed_single_job_log_checks_as_job_zero(self) -> None:
        violations = JobLogSplitter().split_and_check(
            [
                ("status-seen", "completed", 9.0),
                ("status-seen", "running", 10.0),
            ]
        )
        assert len(violations) == 1
        assert violations[0].startswith("job 0: ")
        assert "absorbing" in violations[0]

    def test_injected_oracle_is_used(self) -> None:
        splitter = JobLogSplitter(status_oracle=JobStatusOracle())
        violations = splitter.split_and_check(
            [
                ("job1-status-seen", "running", 9.0),
                ("job1-status-seen", "submitted", 10.0),
            ]
        )
        assert len(violations) == 1
        assert violations[0].startswith("job 1: ")
        assert "regressed" in violations[0]
