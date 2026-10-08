"""Wire model ``TrackingToken`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

from dataclasses import dataclass

# Token components are joined with ``TOKEN_SEPARATOR``. Node ids embed
# their host, so a component may itself contain the separator (an IPv6
# literal) -- such a component, or one that begins with
# ``BRACKET_OPEN``, is written as a bracketed literal (RFC 3986
# section 3.2.2 encloses an IPv6 host in brackets for the same reason),
# with any ``BRACKET_CLOSE`` inside doubled. Every other component is
# written bare, so a token whose components need no brackets is
# byte-identical to the original unbracketed format and every token
# that format wrote still parses (AD-25).
TOKEN_SEPARATOR = ":"
BRACKET_OPEN = "["
BRACKET_CLOSE = "]"
ESCAPED_BRACKET_CLOSE = BRACKET_CLOSE + BRACKET_CLOSE
# Job, workflow, and sub-workflow levels.
MINIMUM_TOKEN_COMPONENTS = 3
MAXIMUM_TOKEN_COMPONENTS = 5


def _format_component(component: str) -> str:
    """One component as written: bracketed when it holds the separator or
    begins with an opening bracket, bare otherwise."""
    if TOKEN_SEPARATOR in component or component.startswith(BRACKET_OPEN):
        return BRACKET_OPEN + component.replace(BRACKET_CLOSE, ESCAPED_BRACKET_CLOSE) + BRACKET_CLOSE
    return component


def format_token_components(components: tuple[str, ...]) -> str:
    """Join ``components`` into a token string, bracketing only those that
    need it (no component needs it in the common case: one join)."""
    joined = TOKEN_SEPARATOR.join(components)
    # Only the joining separators and no opening bracket at all: no
    # component needs brackets.
    if joined.count(TOKEN_SEPARATOR) + joined.count(BRACKET_OPEN) == len(components) - 1:
        return joined
    return TOKEN_SEPARATOR.join([_format_component(component) for component in components])


def _closing_bracket_index(token_str: str, search_from: int) -> int:
    """Index of the bracket closing a literal whose content starts at
    ``search_from``: the first closing bracket not part of a doubled pair."""
    closing_index = token_str.find(BRACKET_CLOSE, search_from)
    # A miss (-1) ends the scan too: the escape cannot start at the
    # string's last character.
    while token_str.startswith(ESCAPED_BRACKET_CLOSE, closing_index):
        closing_index = token_str.find(BRACKET_CLOSE, closing_index + len(ESCAPED_BRACKET_CLOSE))
    if closing_index == -1:
        raise ValueError(f"Invalid token format (unclosed bracketed component): {token_str}")
    return closing_index


def _read_bracketed_component(token_str: str, position: int) -> tuple[str, int]:
    """The bracketed component starting at ``position`` and the index just
    past it, which must be the token's end or a separator."""
    closing_index = _closing_bracket_index(token_str, position + len(BRACKET_OPEN))
    end_index = closing_index + len(BRACKET_CLOSE)
    if end_index != len(token_str) and not token_str.startswith(TOKEN_SEPARATOR, end_index):
        raise ValueError(f"Invalid token format (bracketed component not followed by a separator): {token_str}")
    content = token_str[position + len(BRACKET_OPEN) : closing_index]
    return content.replace(ESCAPED_BRACKET_CLOSE, BRACKET_CLOSE), end_index


def _read_component(token_str: str, position: int) -> tuple[str, int]:
    """The component starting at ``position`` and the index just past it
    (the token's end or the separator that follows it)."""
    if token_str.startswith(BRACKET_OPEN, position):
        return _read_bracketed_component(token_str, position)
    separator_index = token_str.find(TOKEN_SEPARATOR, position)
    end_index = len(token_str) if separator_index == -1 else separator_index
    return token_str[position:end_index], end_index


def _read_bracketed_token(token_str: str) -> list[str]:
    """Split a token holding at least one bracketed component."""
    components: list[str] = []
    component, end_index = _read_component(token_str, 0)
    components.append(component)
    while end_index != len(token_str):
        component, end_index = _read_component(token_str, end_index + len(TOKEN_SEPARATOR))
        components.append(component)
    return components


def parse_token_components(token_str: str) -> list[str]:
    """Split a token string into its components (the inverse of
    ``format_token_components``); a token without brackets -- every token
    of the original format -- is a plain split."""
    if BRACKET_OPEN not in token_str:
        return token_str.split(TOKEN_SEPARATOR)
    return _read_bracketed_token(token_str)


@dataclass(frozen=True)
class TrackingToken:
    """
    Globally unique tracking token for jobs, workflows, and sub-workflows.

    Format: <datacenter>:<manager_id>:<job_id>:<workflow_id>:<worker_id>

    A component holding ``:`` (an IPv6 host inside a node id) or
    beginning with ``[`` is written bracketed -- ``[fe80::1]`` -- with
    any ``]`` inside doubled, so every host form (IPv4, IPv6, DNS)
    round-trips; tokens of the original unbracketed format still parse.

    The token is hierarchical - each level includes all parent components:
    - Job:         datacenter:manager_id:job_id
    - Workflow:    datacenter:manager_id:job_id:workflow_id
    - Sub-workflow: datacenter:manager_id:job_id:workflow_id:worker_id
    """

    datacenter: str
    manager_id: str
    job_id: str
    workflow_id: str | None = None
    worker_id: str | None = None

    @classmethod
    def for_job(cls, datacenter: str, manager_id: str, job_id: str) -> "TrackingToken":
        """Create a job-level token."""
        return cls(datacenter=datacenter, manager_id=manager_id, job_id=job_id)

    @classmethod
    def for_workflow(
        cls,
        datacenter: str,
        manager_id: str,
        job_id: str,
        workflow_id: str,
    ) -> "TrackingToken":
        """Create a workflow-level token."""
        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
        )

    @classmethod
    def for_sub_workflow(
        cls,
        datacenter: str,
        manager_id: str,
        job_id: str,
        workflow_id: str,
        worker_id: str,
    ) -> "TrackingToken":
        """Create a sub-workflow token (dispatched to specific worker)."""
        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
        )

    @classmethod
    def parse(cls, token_str: str) -> "TrackingToken":
        """
        Parse a token string back into a TrackingToken.

        Raises ValueError if the format is invalid.
        """
        parts = parse_token_components(token_str)
        if not MINIMUM_TOKEN_COMPONENTS <= len(parts) <= MAXIMUM_TOKEN_COMPONENTS:
            raise ValueError(
                f"Invalid token format (need {MINIMUM_TOKEN_COMPONENTS} to "
                f"{MAXIMUM_TOKEN_COMPONENTS} parts, got {len(parts)}): {token_str}"
            )

        datacenter = parts[0]
        manager_id = parts[1]
        job_id = parts[2]
        # Absent workflow/worker levels pad to None.
        workflow_id, worker_id = (parts[3:5] + [None, None])[:2]

        return cls(
            datacenter=datacenter,
            manager_id=manager_id,
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
        )

    def __str__(self) -> str:
        """Convert to string format."""
        if self.worker_id:
            return format_token_components(
                (self.datacenter, self.manager_id, self.job_id, self.workflow_id, self.worker_id)
            )
        elif self.workflow_id:
            return format_token_components((self.datacenter, self.manager_id, self.job_id, self.workflow_id))
        else:
            return format_token_components((self.datacenter, self.manager_id, self.job_id))

    @property
    def job_token(self) -> str:
        """Get the job-level token string."""
        return format_token_components((self.datacenter, self.manager_id, self.job_id))

    @property
    def workflow_token(self) -> str | None:
        """Get the workflow-level token string, or None if this is a job token."""
        if not self.workflow_id:
            return None
        return format_token_components((self.datacenter, self.manager_id, self.job_id, self.workflow_id))

    @property
    def is_job_token(self) -> bool:
        """True if this is a job-level token."""
        return self.workflow_id is None

    @property
    def is_workflow_token(self) -> bool:
        """True if this is a workflow-level token (not sub-workflow)."""
        return self.workflow_id is not None and self.worker_id is None

    @property
    def is_sub_workflow_token(self) -> bool:
        """True if this is a sub-workflow token."""
        return self.worker_id is not None

    def to_workflow_token(self, workflow_id: str) -> "TrackingToken":
        """Create a workflow token from this job token."""
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=workflow_id,
        )

    def to_sub_workflow_token(self, worker_id: str) -> "TrackingToken":
        """Create a sub-workflow token from this workflow token."""
        if not self.workflow_id:
            raise ValueError("Cannot create sub-workflow token from job token")
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=self.workflow_id,
            worker_id=worker_id,
        )

    def to_parent_workflow_token(self) -> "TrackingToken":
        """Get the parent workflow token from a sub-workflow token."""
        if not self.is_sub_workflow_token:
            raise ValueError("Not a sub-workflow token")
        return TrackingToken(
            datacenter=self.datacenter,
            manager_id=self.manager_id,
            job_id=self.job_id,
            workflow_id=self.workflow_id,
        )
