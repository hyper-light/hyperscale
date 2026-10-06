"""
WorkflowLifecycleOracle — checks a manager's observed workflow lifecycle
against AD-54.

The spec is ``VALID_TRANSITIONS`` (production's own table — the oracle
verifies the OBSERVED HISTORIES, which no production component judges):

* every applied transition is an edge of the table, and none is refused;
* each workflow's history is continuous — a transition leaves the state
  the previous one entered — and starts with registration into PENDING
  (or an install from a snapshot);
* COMPLETED, AGGREGATED and CANCELLED are absorbing;
* an observable FAILED is terminal: the retry path leaves FAILED for
  FAILED_CANCELING_DEPENDENTS, then FAILED_READY_FOR_RETRY, then PENDING,
  all at the instant it entered FAILED;
* at every watcher sample each workflow's ``WorkflowInfo.status`` equals
  its state's projection, and every lifecycle record belongs to a
  workflow;
* a job's records leave with the job, and a workflow whose records left
  had reached a terminal state.

Histories are the rows ``observe_workflow_lifecycle`` records:
``("lifecycle", job ordinal, workflow name, from, to, accepted,
installed, t)`` plus the ``(tag, count, t)`` watcher rows.
"""

from hyperscale.distributed.workflow import VALID_TRANSITIONS, WorkflowState

ABSORBING_STATES = frozenset(
    {
        WorkflowState.COMPLETED,
        WorkflowState.AGGREGATED,
        WorkflowState.CANCELLED,
    }
)
FINISHED_STATES = ABSORBING_STATES | {WorkflowState.FAILED}
RETRY_CHAIN = (
    WorkflowState.FAILED,
    WorkflowState.FAILED_CANCELING_DEPENDENTS,
    WorkflowState.FAILED_READY_FOR_RETRY,
    WorkflowState.PENDING,
)
ZERO_COUNT_TAGS = (
    "lifecycle-orphaned-records",
    "lifecycle-mismatched",
    "lifecycle-refused",
)


class WorkflowLifecycleOracle:
    """Judge one manager's observed workflow lifecycle.

    ``check_manager_log`` returns human-readable violations (empty = the
    observed lifecycle satisfies AD-54). ``workflow_histories`` returns
    each workflow's taken transitions, for scenario-specific assertions.
    """

    __slots__ = ()

    def workflow_histories(
        self,
        manager_log: list[tuple],
    ) -> dict[tuple[int, str | None], list[tuple[str | None, str, float]]]:
        """Each workflow's accepted transitions in order, keyed by (job
        ordinal, workflow name), as (from, to, t)."""
        histories: dict[tuple[int, str | None], list[tuple[str | None, str, float]]] = {}
        for entry in manager_log:
            if entry[0] != "lifecycle" or not entry[5]:
                continue
            _tag, job_ordinal, workflow_name, from_value, to_value, _accepted, _installed, at_time = entry
            histories.setdefault((job_ordinal, workflow_name), []).append((from_value, to_value, at_time))
        return histories

    def check_manager_log(
        self,
        manager_log: list[tuple],
        expect_released: bool = True,
    ) -> list[str]:
        violations: list[str] = []
        current_states: dict[tuple[int, str | None], WorkflowState] = {}
        failed_at: dict[tuple[int, str | None], float] = {}

        for entry in manager_log:
            if entry[0] != "lifecycle":
                continue
            _tag, job_ordinal, workflow_name, from_value, to_value, accepted, installed, at_time = entry
            key = (job_ordinal, workflow_name)
            label = f"t={at_time} job {job_ordinal} workflow {workflow_name!r}"
            if not accepted:
                violations.append(f"{label}: refused {from_value} -> {to_value}")
                continue

            from_state = None if from_value is None else WorkflowState(from_value)
            to_state = WorkflowState(to_value)
            previous_state = current_states.get(key)
            if from_state != previous_state:
                violations.append(
                    f"{label}: history broken -- left {from_value}, but its last "
                    f"state was {None if previous_state is None else previous_state.value}"
                )
            if previous_state in ABSORBING_STATES:
                violations.append(f"{label}: moved {previous_state.value} -> {to_value} after an absorbing state")
            elif from_state is None:
                if not installed and to_state != WorkflowState.PENDING:
                    violations.append(f"{label}: registered into {to_value}, not pending")
            elif not installed and to_state not in VALID_TRANSITIONS[from_state]:
                violations.append(f"{label}: {from_value} -> {to_value} is not an AD-54 edge")

            if previous_state in RETRY_CHAIN[:-1] and failed_at.get(key) != at_time:
                violations.append(
                    f"{label}: {previous_state.value} -> {to_value} left the retry chain's "
                    f"instant ({failed_at.get(key)}) -- an observable FAILED must be terminal"
                )
            if to_state == WorkflowState.FAILED:
                failed_at[key] = at_time
            current_states[key] = to_state

        for entry in manager_log:
            if entry[0] in ZERO_COUNT_TAGS and entry[1] != 0:
                violations.append(f"t={entry[2]}: {entry[0]} = {entry[1]}")

        if expect_released:
            record_counts = [entry[1] for entry in manager_log if entry[0] == "lifecycle-records"]
            if not record_counts or record_counts[-1] != 0:
                violations.append(f"lifecycle records never released: {record_counts[-1:]}")
            violations.extend(
                f"job {job_ordinal} workflow {workflow_name!r} ended in {state.value}, never finished"
                for (job_ordinal, workflow_name), state in current_states.items()
                if state not in FINISHED_STATES
            )

        return violations
