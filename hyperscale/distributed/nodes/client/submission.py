"""
Job submission for HyperscaleClient.

Handles job submission with retry logic, leader redirection, and protocol negotiation.
"""

import asyncio
from typing import Callable, NoReturn

import cloudpickle

from hyperscale.core.jobs.protocols.constants import MAX_DECOMPRESSED_SIZE
from hyperscale.distributed.errors import MessageTooLargeError
from hyperscale.distributed.idempotency.idempotency_key import (
    IdempotencyKeyGenerator,
)
from hyperscale.distributed.jobs.logical_id_generator import (
    LogicalIdGenerator,
)
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.distributed.models import (
    JobSubmission,
    JobAck,
    JobStatusPush,
    WorkflowResultPush,
    ReporterResultPush,
    RateLimitResponse,
)
from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.config import ClientConfig, TRANSIENT_ERRORS
from hyperscale.logging import Logger

from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom


_DEFAULT_CLOCK: Clock = RealClock()
_DEFAULT_RANDOM: Random = RealRandom()

# Submission outcomes that end the retry loop: "success", and
# "permanent_failure" (a permanent rejection already raised its error).
_FINISHED_SUBMISSION_RESULTS = frozenset({"success", "permanent_failure"})


def _prepend_redirect_history(history: list[str], tail: str) -> str:
    """Join a redirect trail with a trailing error message.

    Used by ``_submit_with_redirects`` to preserve the origin
    "Not DC leader, retry at leader: X" context when a
    subsequent redirect hop fails with a transport error. The
    returned string is what ``_submit_with_retry`` stores in
    ``last_error`` — and eventually surfaces in the final
    ``RuntimeError("Job submission failed after N retries:
    {last_error}")``. Without prepending the history, the outer
    caller would see only the last transport error and lose the
    semantically-correct answer from the origin (typical case:
    "no leader / quorum lost" under partition scenarios where
    the redirect target is unreachable by construction).

    Empty history is a no-op — the return string matches ``tail``
    unchanged so the non-redirect fast path is unaffected.
    """
    if not history:
        return tail
    return "; ".join(history) + f"; {tail}"


class ClientJobSubmitter:
    """
    Manages job submission with retry logic and leader redirection.

    Submission flow:
    1. Generate job_id and workflow_ids
    2. Extract local reporter configs from workflows
    3. Serialize workflows and reporter configs with cloudpickle
    4. Pre-submission size validation (5MB limit)
    5. Build JobSubmission message with protocol version
    6. Initialize job tracking structures
    7. Retry loop with exponential backoff:
       - Cycle through all targets (gates/managers)
       - Follow leader redirects (up to max_redirects)
       - Detect transient errors and retry
       - Permanent rejection fails immediately
    8. Store negotiated capabilities on success
    9. Return job_id
    """

    def __init__(
        self,
        state: ClientState,
        config: ClientConfig,
        logger: Logger,
        targets,  # ClientTargetSelector
        tracker,  # ClientJobTracker
        protocol,  # ClientProtocol
        send_tcp_func,  # Callable for sending TCP messages
        idempotency_key_generator: IdempotencyKeyGenerator,
        logical_id_generator: LogicalIdGenerator,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._targets = targets
        self._tracker = tracker
        self._protocol = protocol
        self._send_tcp = send_tcp_func
        self._idempotency_key_generator = idempotency_key_generator
        self._logical_id_generator = logical_id_generator

    async def submit_job(
        self,
        workflows: list[tuple[list[str], object]],
        vus: int = 1,
        timeout_seconds: float | None = None,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        reporting_configs: list | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
        retry_budget: int = 0,
        retry_budget_per_workflow: int = 0,
        resource_budget: ResourceBudget | None = None,
        best_effort: bool = False,
        best_effort_min_dcs: int = 0,
        best_effort_deadline_seconds: float = 0.0,
    ) -> str:
        """
        Submit a job for execution.

        Args:
            workflows: List of (dependencies, workflow_instance) tuples
            vus: Virtual users (cores) per workflow
            timeout_seconds: Maximum execution time
            datacenter_count: Number of datacenters to run in (gates only)
            datacenters: Specific datacenters to target (optional)
            on_status_update: Callback for status updates (optional)
            on_progress_update: Callback for streaming progress updates (optional)
            on_workflow_result: Callback for workflow completion results (optional)
            reporting_configs: List of ReporterConfig objects for result submission (optional)
            on_reporter_result: Callback for reporter submission results (optional)
            retry_budget: AD-44 total retry cap for the job (0 = manager default)
            retry_budget_per_workflow: AD-44 per-workflow retry cap (0 = manager default)
            resource_budget: AD-41 limits each workflow is enforced against
                (None = the manager's configured default)
            best_effort: AD-44 complete before every DC reported
            best_effort_min_dcs: completed DCs that end the job (0 = gate default)
            best_effort_deadline_seconds: longest wait for DCs (0 = gate default)

        Returns:
            job_id: Unique identifier for the submitted job

        Raises:
            RuntimeError: If no managers/gates configured or submission fails
            MessageTooLargeError: If serialized workflows exceed 5MB
        """
        # Deterministic-unique (identity + monotonic ns + counter):
        # job ids appear in every downstream message and WAL record, so
        # wall-entropy ids would break SIM byte-identical replay — and
        # the shared seeded Random is off-limits for ids (consuming
        # draws reshuffles the protocol schedule). Uniqueness is the
        # requirement; the AD-40 idempotency key carries anti-replay.
        job_id = self._logical_id_generator.generate("job")

        # Extract reporter configs and generate workflow IDs
        workflows_with_ids, extracted_local_configs = self._prepare_workflows(workflows)

        # Serialize workflows
        workflows_bytes = cloudpickle.dumps(workflows_with_ids)

        # Pre-submission size validation - fail fast before sending
        self._validate_submission_size(workflows_bytes)

        # Serialize reporter configs if provided
        reporting_configs_bytes = self._serialize_reporting_configs(reporting_configs)

        is_explicit_timeout, effective_timeout_for_wire = self._wire_timeout(timeout_seconds)

        # Build submission message
        submission = self._build_job_submission(
            job_id=job_id,
            workflows_bytes=workflows_bytes,
            vus=vus,
            timeout_seconds=effective_timeout_for_wire,
            timeout_seconds_explicit=is_explicit_timeout,
            datacenter_count=datacenter_count,
            datacenters=datacenters or [],
            reporting_configs_bytes=reporting_configs_bytes,
            retry_budget=retry_budget,
            retry_budget_per_workflow=retry_budget_per_workflow,
            resource_budget=resource_budget,
            best_effort=best_effort,
            best_effort_min_dcs=best_effort_min_dcs,
            best_effort_deadline_seconds=best_effort_deadline_seconds,
        )

        # Initialize job tracking
        self._tracker.initialize_job_tracking(
            job_id,
            expected_workflow_ids=self._expected_workflow_ids(workflows_with_ids),
            on_status_update=on_status_update,
            on_progress_update=on_progress_update,
            on_workflow_result=on_workflow_result,
            on_reporter_result=on_reporter_result,
        )

        # Store reporting configs for local file-based reporting
        explicit_local_configs = self._explicit_local_configs(reporting_configs)
        self._state._job_reporting_configs[job_id] = extracted_local_configs + explicit_local_configs

        # Submit with retry logic
        try:
            await self._submit_with_retry(job_id, submission)
            return job_id
        except Exception as error:
            self._tracker.mark_job_failed(job_id, str(error))
            raise

    @staticmethod
    def _serialize_reporting_configs(reporting_configs: list | None) -> bytes:
        """The reporter configs cloudpickled, or empty bytes when there are none."""
        if reporting_configs:
            return cloudpickle.dumps(reporting_configs)
        return b''

    @staticmethod
    def _wire_timeout(timeout_seconds: float | None) -> tuple[bool, float]:
        """Whether the job timeout is explicit, and the timeout to put on the wire (Phase H2)."""
        # Phase H2 — explicit-vs-default detection. ``None`` means
        # "let the manager apply the AD-26/AD-34 override hierarchy
        # (workflow-class timeout > duration × multiplier)." Any
        # positive number is treated as an explicit per-job override
        # the manager honors verbatim.
        is_explicit_timeout = (
            timeout_seconds is not None and timeout_seconds > 0.0
        )
        effective_timeout_for_wire = (
            timeout_seconds if is_explicit_timeout else 0.0
        )
        return (is_explicit_timeout, effective_timeout_for_wire)

    @staticmethod
    def _expected_workflow_ids(workflows_with_ids: list[tuple[str, list[str], object]]) -> frozenset[str]:
        """The ids of the workflows the job is submitted with."""
        return frozenset(
            workflow_id for workflow_id, _, _ in workflows_with_ids
        )

    def _explicit_local_configs(self, reporting_configs: list | None) -> list:
        """The explicitly passed reporter configs that are local file reporter types."""
        return list(filter(self._is_local_reporter_config, reporting_configs or []))

    def _is_local_reporter_config(self, config: object) -> bool:
        """Whether a reporter config is a local file reporter type."""
        return getattr(config, 'reporter_type', None) in self._config.local_reporter_types

    def _prepare_workflows(
        self,
        workflows: list[tuple[list[str], object]],
    ) -> tuple[list[tuple[str, list[str], object]], list]:
        """
        Generate workflow IDs and extract local reporter configs.

        Args:
            workflows: List of (dependencies, workflow_instance) tuples

        Returns:
            (workflows_with_ids, extracted_local_configs) tuple
        """
        workflows_with_ids: list[tuple[str, list[str], object]] = []
        extracted_local_configs: list = []

        for dependencies, workflow_instance in workflows:
            workflow_id = self._logical_id_generator.generate("wf")
            workflows_with_ids.append((workflow_id, dependencies, workflow_instance))

            # Extract reporter config from workflow if present
            extracted_local_configs.extend(self._local_reporter_configs_of(workflow_instance))

        return (workflows_with_ids, extracted_local_configs)

    def _local_reporter_configs_of(self, workflow_instance: object) -> list:
        """The local file reporter configs a workflow carries in its ``reporting``."""
        workflow_reporting = getattr(workflow_instance, 'reporting', None)
        if workflow_reporting is None:
            return []
        # Handle single config or list of configs
        configs_to_check = (
            workflow_reporting
            if isinstance(workflow_reporting, list)
            else [workflow_reporting]
        )
        # Check if this is a local file reporter type
        return list(filter(self._is_local_reporter_config, configs_to_check))

    def _validate_submission_size(self, workflows_bytes: bytes) -> None:
        """
        Validate serialized workflows don't exceed size limit.

        Args:
            workflows_bytes: Serialized workflows

        Raises:
            MessageTooLargeError: If size exceeds MAX_DECOMPRESSED_SIZE (5MB)
        """
        if len(workflows_bytes) > MAX_DECOMPRESSED_SIZE:
            raise MessageTooLargeError(
                f"Serialized workflows exceed maximum size: "
                f"{len(workflows_bytes)} > {MAX_DECOMPRESSED_SIZE} bytes (5MB)"
            )

    def _build_job_submission(
        self,
        job_id: str,
        workflows_bytes: bytes,
        vus: int,
        timeout_seconds: float,
        datacenter_count: int,
        datacenters: list[str],
        reporting_configs_bytes: bytes,
        timeout_seconds_explicit: bool = False,
        retry_budget: int = 0,
        retry_budget_per_workflow: int = 0,
        resource_budget: ResourceBudget | None = None,
        best_effort: bool = False,
        best_effort_min_dcs: int = 0,
        best_effort_deadline_seconds: float = 0.0,
    ) -> JobSubmission:
        """
        Build JobSubmission message with protocol version.

        Args:
            job_id: Job identifier
            workflows_bytes: Serialized workflows
            vus: Virtual users
            timeout_seconds: Timeout (0.0 when not explicit; manager
                applies override hierarchy)
            datacenter_count: DC count
            datacenters: Specific DCs
            reporting_configs_bytes: Serialized reporter configs
            timeout_seconds_explicit: True when the client passed an
                explicit per-job timeout. Phase H2 — distinguishes
                "use this exact value" from "use framework default."

        Returns:
            JobSubmission message
        """
        return JobSubmission(
            job_id=job_id,
            workflows=workflows_bytes,
            vus=vus,
            timeout_seconds=timeout_seconds,
            timeout_seconds_explicit=timeout_seconds_explicit,
            datacenter_count=datacenter_count,
            datacenters=datacenters,
            callback_addr=self._targets.get_callback_addr(),
            reporting_configs=reporting_configs_bytes,
            # Protocol version fields (AD-25)
            protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
            protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            capabilities=self._protocol.get_client_capabilities_string(),
            # AD-40: one key per LOGICAL submission — the retry loop
            # reuses this message across managers/redirects, so a
            # cross-manager retry of the same call cannot duplicate the
            # job; the manager ledger dedups on it.
            idempotency_key=str(self._idempotency_key_generator.generate()),
            # AD-44: 0 lets the manager's configured defaults apply.
            retry_budget=retry_budget,
            retry_budget_per_workflow=retry_budget_per_workflow,
            resource_budget=resource_budget,
            best_effort=best_effort,
            best_effort_min_dcs=best_effort_min_dcs,
            best_effort_deadline_seconds=best_effort_deadline_seconds,
        )

    async def _submit_with_retry(
        self,
        job_id: str,
        submission: JobSubmission,
    ) -> None:
        """
        Submit job with retry logic and leader redirection.

        Args:
            job_id: Job identifier
            submission: JobSubmission message

        Raises:
            RuntimeError: If submission fails after retries
        """
        # AD-28 order: gates then managers, each tier ranked per job, so
        # submissions spread across targets instead of all starting at the
        # first configured gate.
        all_targets = self._submission_targets(job_id)

        # Retry loop with exponential backoff for transient errors
        last_error = None
        max_retries = self._config.submission_max_retries
        max_redirects = self._config.submission_max_redirects_per_attempt

        for retry in range(max_retries + 1):
            # Try each target in ranked order, cycling through on retries
            target_idx = retry % len(all_targets)
            target = all_targets[target_idx]

            # Submit with leader redirect handling
            redirect_result, retry_after_seconds = await self._submit_with_redirects(
                job_id, target, submission, max_redirects
            )

            # "success", or "permanent_failure" (a permanent rejection
            # already raised its error): done.
            if redirect_result in _FINISHED_SUBMISSION_RESULTS:
                return
            # Transient error - retry
            last_error = redirect_result

            await self._backoff_before_retry(retry, max_retries, retry_after_seconds)

        # All retries exhausted
        raise RuntimeError(f"Job submission failed after {max_retries} retries: {last_error}")

    def _submission_targets(self, job_id: str) -> list[tuple[str, int]]:
        """The job's submission targets in ranked order; raises when none are configured."""
        all_targets = self._targets.get_submission_targets(job_id)
        if not all_targets:
            raise RuntimeError("No managers or gates configured")
        return all_targets

    async def _backoff_before_retry(
        self,
        retry: int,
        max_retries: int,
        retry_after_seconds: float,
    ) -> None:
        """
        Sleep before the next retry (AD-21, AD-24).

        A refusal that carried the server's ``retry_after_seconds`` hint
        waits the hint plus up to one more hint of jitter -- never less,
        since the server will refuse again until then, while the jitter
        keeps clients refused together from returning together. The last
        refused attempt waits its hint too, before the failure is raised:
        a caller that resubmits on that failure (a submit loop) otherwise
        reaches the server well inside the hint it was just given.

        An un-hinted transient refusal waits an exponential, equal-jittered
        back-off from the configured base. Every transient failure backs
        off, including one whose error text is empty (a bare timeout);
        after the last un-hinted one no attempt follows, so the failure is
        raised at once.
        """
        if retry_after_seconds > 0.0:
            await _DEFAULT_CLOCK.sleep(retry_after_seconds * (1.0 + _DEFAULT_RANDOM.random()))
            return
        if retry >= max_retries:
            return
        base_delay = self._config.retry_base_delay_seconds * (2**retry)
        await _DEFAULT_CLOCK.sleep(base_delay * (0.5 + _DEFAULT_RANDOM.random()))

    async def _submit_with_redirects(
        self,
        job_id: str,
        target: tuple[str, int],
        submission: JobSubmission,
        max_redirects: int,
    ) -> tuple[str, float]:
        """
        Submit to target with leader redirect handling.

        Args:
            job_id: Job identifier
            target: Initial target (host, port)
            submission: JobSubmission message
            max_redirects: Maximum redirects to follow

        Returns:
            "success", "permanent_failure", or error message (transient),
            with the refusing server's retry hint in seconds (0.0 when it
            gave none).

        Redirect-context preservation: when a manager responds
        ``JobAck(accepted=False, leader_addr=X)`` we follow the
        redirect to ``X``. If the redirect target is unreachable
        (connection refused, timeout, etc.) the outer caller
        surfaces only the *last* error by default — dropping the
        initial context (e.g. ``"Not DC leader, retry at leader:
        X"``) that told us why we were redirecting in the first
        place. That context is diagnostically load-bearing: a
        client debugging "why did my submit fail?" needs to know
        the origin manager considered itself non-leader, not just
        that a follow-up hop couldn't connect. Under
        quorum-isolating partition scenarios the origin's
        response is also the semantically-correct answer (the
        cluster is split; there's no leader that can accept the
        submit). Concatenating the redirect trail into the
        transient-error string preserves both the origin reason
        and the subsequent transport failure — the caller sees
        the full causal chain instead of only its tail.
        """
        redirects = 0
        redirect_history: list[str] = []
        while redirects <= max_redirects:
            final_outcome, redirect_target, retry_after_seconds = await self._submit_hop(
                job_id,
                target,
                submission,
                redirect_history,
                redirects < max_redirects,
            )
            if redirect_target is None:
                return (final_outcome, retry_after_seconds)
            target = redirect_target
            redirects += 1

        return (_prepend_redirect_history(redirect_history, "max_redirects_exceeded"), 0.0)

    async def _submit_hop(
        self,
        job_id: str,
        target: tuple[str, int],
        submission: JobSubmission,
        redirect_history: list[str],
        may_redirect: bool,
    ) -> tuple[str | None, tuple[str, int] | None, float]:
        """Send the submission to one target: its final outcome or the leader to redirect to, and its retry hint."""
        sent_at = _DEFAULT_CLOCK.monotonic()
        response, _ = await self._send_tcp(
            target,
            "job_submission",
            submission.dump(),
            timeout=self._config.submission_timeout_seconds,
        )

        if isinstance(response, Exception):
            self._targets.record_target_failure(target)
            return (_prepend_redirect_history(redirect_history, str(response)), None, 0.0)

        # A rate-limited (AD-32) or shed/quorum-refused (AD-24) submission
        # carries the server's retry hint; the retry loop waits it out.
        if (rate_limit_response := self._rate_limit_response(response)) is not None:
            return (rate_limit_response.error, None, rate_limit_response.retry_after_seconds)

        ack = JobAck.load(response)
        final_outcome, redirect_target = self._ack_outcome(
            job_id,
            target,
            ack,
            sent_at,
            redirect_history,
            may_redirect,
        )
        return (final_outcome, redirect_target, ack.retry_after_seconds)

    @staticmethod
    def _rate_limit_response(response: bytes) -> RateLimitResponse | None:
        """The response as a rate-limit response (AD-32), or None when it is not one."""
        # Check for rate limiting response (AD-32). ``Message.load``
        # is intentionally lax about the deserialized type
        # (it doubles as a restricted-unpickler shim for cloudpickled
        # Workflow payloads that are not ``Message`` subclasses), so
        # ``RateLimitResponse.load(jobAck_bytes)`` happily returns a
        # ``JobAck`` typed as ``RateLimitResponse``. Without the
        # ``isinstance`` guard below, any field shared between the
        # two models (e.g. ``retry_after_seconds``) lets a successful
        # ``JobAck`` silently masquerade as a rate-limit response,
        # which broke submission retry semantics when
        # ``gate_replication_quorum_unavailable`` retry hints were
        # added to ``JobAck``. The narrow ``try`` scope around the
        # load keeps real errors in the rate-limit branch
        # (e.g. ``_DEFAULT_CLOCK.sleep`` cancellation) from being silently
        # swallowed.
        try:
            candidate = RateLimitResponse.load(response)
        except Exception:
            candidate = None
        if isinstance(candidate, RateLimitResponse):
            return candidate
        return None

    def _ack_outcome(
        self,
        job_id: str,
        target: tuple[str, int],
        ack: JobAck,
        sent_at: float,
        redirect_history: list[str],
        may_redirect: bool,
    ) -> tuple[str | None, tuple[str, int] | None]:
        """The outcome of a target's ack: success, a leader redirect, or a rejection."""
        if ack.accepted:
            self._record_accepted_submission(job_id, target, ack, sent_at)
            return ("success", None)

        return self._rejected_ack_outcome(target, ack, redirect_history, may_redirect)

    def _record_accepted_submission(
        self,
        job_id: str,
        target: tuple[str, int],
        ack: JobAck,
        sent_at: float,
    ) -> None:
        """Record the accepting target's latency, the job's target, and the negotiated capabilities."""
        self._targets.record_target_success(
            target,
            (_DEFAULT_CLOCK.monotonic() - sent_at) * 1000.0,
        )

        # Track which server accepted this job for future queries
        self._state.mark_job_target(job_id, target)

        # Store negotiated capabilities (AD-25)
        self._protocol.negotiate_capabilities(
            server_addr=target,
            server_version_major=getattr(ack, 'protocol_version_major', 1),
            server_version_minor=getattr(ack, 'protocol_version_minor', 0),
            server_capabilities_str=getattr(ack, 'capabilities', ''),
        )

    def _rejected_ack_outcome(
        self,
        target: tuple[str, int],
        ack: JobAck,
        redirect_history: list[str],
        may_redirect: bool,
    ) -> tuple[str | None, tuple[str, int] | None]:
        """Follow a rejected ack's leader redirect while redirects remain, else settle the rejection."""
        # Check for leader redirect
        if ack.leader_addr and may_redirect:
            return (None, self._redirect_target(target, ack, redirect_history))

        return (self._rejection_outcome(ack, redirect_history), None)

    @staticmethod
    def _redirect_target(
        target: tuple[str, int],
        ack: JobAck,
        redirect_history: list[str],
    ) -> tuple[str, int]:
        """The leader a rejected ack redirects to, remembering why the origin bounced us."""
        # Remember why the origin bounced us before we
        # follow the hint — the subsequent hop may fail
        # with a lower-level transport error and lose this
        # context.
        if ack.error:
            redirect_history.append(f"{target}: {ack.error}")
        return tuple(ack.leader_addr)

    def _rejection_outcome(self, ack: JobAck, redirect_history: list[str]) -> str:
        """A transient rejection's error with the redirect trail; a permanent rejection raises."""
        # A refusal carrying a retry hint is retryable by the JobAck
        # contract (e.g. the gate's "gate_replication_quorum_unavailable",
        # which names no transient-vocabulary marker); otherwise the
        # error text classifies it.
        if ack.error and self._is_retryable_rejection(ack):
            return _prepend_redirect_history(redirect_history, ack.error)

        self._raise_permanent_rejection(ack, redirect_history)

    @staticmethod
    def _raise_permanent_rejection(ack: JobAck, redirect_history: list[str]) -> NoReturn:
        """Fail a permanently rejected submission with the full redirect trail."""
        # Permanent rejection - fail immediately. The redirect
        # trail (if any) is included so the caller sees the
        # full causal chain, matching the transient-error
        # branch.
        if redirect_history:
            trail = "; ".join(redirect_history)
            raise RuntimeError(f"Job rejected: {trail}; {ack.error}")
        raise RuntimeError(f"Job rejected: {ack.error}")

    @staticmethod
    def _is_retryable_rejection(ack: JobAck) -> bool:
        """
        Whether a rejected ack (with an error) should be retried: it
        carries a retry hint, or its error matches TRANSIENT_ERRORS.
        """
        error_lower = ack.error.lower()
        return ack.retry_after_seconds > 0.0 or any(te in error_lower for te in TRANSIENT_ERRORS)
