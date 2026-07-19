"""
JobLogSplitter — the K2 per-job adapter between multi-job client
milestone logs and the single-job ``JobStatusOracle``.

Concurrent multi-job client entries prefix each job's milestones with
the job's index — ``("job<k>-status-seen", status, t)``,
``("job<k>-job-finished", status, t)`` — so N overlapping lifetimes
share one replay-stable log. The splitter routes every prefixed row
into its job's stream with the prefix STRIPPED, leaving each stream in
exactly the shape ``JobStatusOracle.check_client_log`` judges; rows
without a job prefix (today's single-job logs, and ambient rows like
``("client-error", ...)``) pass through unchanged as job 0, so
existing single-job logs split to ``{0: log}`` verbatim.

The lifecycle tags that merely BEGIN with "job" — ``job-submitted``,
``job-finished`` — are not prefixes (a prefix is ``job`` + digits +
``-``), and near-miss tags that LOOK job-prefixed but do not parse
(``"job3"``, ``"job3status-seen"``, ``"job3-"``) are reported by
``check_prefix_hygiene`` instead of silently mis-splitting into job 0
— a typo'd prefix must never quietly remove a job's evidence from
checking. Rows that are not tagged tuples at all raise: unjudgeable
evidence is a caller error, never swallowed.

Pure function-style: no per-call state, deterministic outputs, order
preserved within every stream.
"""

import re

from .job_status_oracle import JobStatusOracle

_JOB_PREFIX_PATTERN = re.compile(r"^job(\d+)-(.+)$")
_JOB_PREFIX_NEAR_MISS_PATTERN = re.compile(r"^job\d")

# The unprefixed client lifecycle vocabulary (the committed client
# entries' tags). Used only for the hygiene warning below — a log
# mixing job0-prefixed rows with unprefixed lifecycle rows would merge
# two conventions into stream 0.
_JOB_LIFECYCLE_TAGS = frozenset(
    {
        "submit-rejected",
        "submit-abandoned",
        "submit-target",
        "job-submitted",
        "status-seen",
        "wait-timeout",
        "wait-timed-out",
        "job-finished",
    }
)


class JobLogSplitter:
    """Split one client milestone log into per-job unprefixed streams.

    ``split`` returns ``{job_index: stream}``; ``check_prefix_hygiene``
    reports tags that would mis-split; ``split_and_check`` composes
    both with a per-stream ``JobStatusOracle`` pass into one flat
    violation list — the one-call adoption path for multi-job suites.
    """

    __slots__ = ("_status_oracle",)

    def __init__(self, status_oracle: JobStatusOracle | None = None) -> None:
        self._status_oracle = (
            status_oracle if status_oracle is not None else JobStatusOracle()
        )

    def split(self, client_log: list[tuple]) -> dict[int, list[tuple]]:
        """Route every row to its job's stream, prefixes stripped.

        ``("job2-status-seen", "running", t)`` lands in stream 2 as
        ``("status-seen", "running", t)``; unprefixed rows land in
        stream 0 unchanged. Row order is preserved within each stream;
        an empty log yields no streams.
        """
        streams: dict[int, list[tuple]] = {}
        for row in client_log:
            job_index, stream_row = self._route(row)
            streams.setdefault(job_index, []).append(stream_row)
        return streams

    def check_prefix_hygiene(self, client_log: list[tuple]) -> list[str]:
        """Report rows whose tags would mis-split.

        * a tag matching ``job<digits>`` without a well-formed
          ``job<k>-<tag>`` shape (``"job3"``, ``"job3status-seen"``,
          ``"job3-"``) would silently fall through to job 0;
        * ``job0-``-prefixed rows coexisting with unprefixed lifecycle
          rows merge two conventions into stream 0 — ambiguous
          evidence.
        """
        violations = [
            f"row {row!r}: tag {self._row_tag(row)!r} looks job-prefixed "
            "but does not parse as job<k>-<tag> — it would silently "
            "mis-split into job 0"
            for row in client_log
            if _JOB_PREFIX_NEAR_MISS_PATTERN.match(self._row_tag(row))
            and not _JOB_PREFIX_PATTERN.match(self._row_tag(row))
        ]

        job_zero_prefixed = [
            row
            for row in client_log
            if (matched := _JOB_PREFIX_PATTERN.match(row[0]))
            and int(matched.group(1)) == 0
        ]
        unprefixed_lifecycle = [
            row for row in client_log if row[0] in _JOB_LIFECYCLE_TAGS
        ]
        if job_zero_prefixed and unprefixed_lifecycle:
            violations.append(
                "log mixes job0-prefixed rows with unprefixed lifecycle "
                f"rows — both merge into stream 0, ambiguous evidence: "
                f"prefixed={job_zero_prefixed} "
                f"unprefixed={unprefixed_lifecycle}"
            )
        return violations

    def split_and_check(self, client_log: list[tuple]) -> list[str]:
        """Hygiene plus a per-job ``JobStatusOracle`` pass, flattened.

        Every violation is attributed to its job (``"job <k>: ..."``)
        so a fault mid-first-job corrupting the second is named, not
        just detected.
        """
        violations = self.check_prefix_hygiene(client_log)
        for job_index, stream in sorted(self.split(client_log).items()):
            violations.extend(
                f"job {job_index}: {violation}"
                for violation in self._status_oracle.check_client_log(stream)
            )
        return violations

    def _route(self, row: tuple) -> tuple[int, tuple]:
        """``(job_index, stream_row)`` for one milestone row."""
        if prefixed := _JOB_PREFIX_PATTERN.match(self._row_tag(row)):
            return int(prefixed.group(1)), (prefixed.group(2), *row[1:])
        return 0, row

    @staticmethod
    def _row_tag(row: tuple) -> str:
        if not isinstance(row, tuple) or not row or not isinstance(row[0], str):
            raise ValueError(
                f"milestone row {row!r} has no string tag — refusing to "
                "split unjudgeable evidence"
            )
        return row[0]
