"""What a harness teardown found: the supervisor's cleanup errors and the
invariant violation still pending when the test body ended.

Neither may be dropped. When the body already failed, the report is
attached to that failure as ``__notes__`` (PEP 678) so the original
exception keeps its type for ``pytest.raises`` and its traceback; when the
body passed, the report itself becomes the failure.
"""

from dataclasses import dataclass

from tests.simulation.harness.invariants import InvariantViolation


@dataclass(slots=True, frozen=True)
class CleanupReport:
    """Teardown findings of one harness run."""

    cleanup_errors: tuple[str, ...]
    invariant_violation: InvariantViolation | None

    def notes(self) -> list[str]:
        """One note per finding, the invariant violation first."""
        violation_notes = (
            [f"harness invariant violation: {type(self.invariant_violation).__name__}: {self.invariant_violation}"]
            if self.invariant_violation is not None
            else []
        )
        return violation_notes + [f"harness cleanup error: {cleanup_error}" for cleanup_error in self.cleanup_errors]

    def attach_to(self, failure: BaseException) -> None:
        """Add every finding to an existing failure as a note."""
        for note in self.notes():
            failure.add_note(note)

    def failure(self) -> BaseException | None:
        """The exception a passing body must fail with, or None when teardown was clean.

        A pending invariant violation is raised itself, carrying the
        cleanup errors as notes; cleanup errors alone become one
        RuntimeError listing them all.
        """
        if self.invariant_violation is not None:
            for cleanup_error in self.cleanup_errors:
                self.invariant_violation.add_note(f"harness cleanup error: {cleanup_error}")
            return self.invariant_violation
        if not self.cleanup_errors:
            return None
        joined_errors = "\n  - ".join(self.cleanup_errors)
        return RuntimeError(f"harness cleanup reported errors:\n  - {joined_errors}")
