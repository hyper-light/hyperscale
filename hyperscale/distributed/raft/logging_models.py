"""
Structured logging models for Raft consensus operations.

Follows the Entry-based pattern from hyperscale/logging/models.
Each level variant carries contextual fields identifying the
node, job, and Raft-specific details.
"""

from hyperscale.logging.models import Entry, LogLevel


# =============================================================================
# Raft Consensus Logging Models
# =============================================================================


class RaftTrace(Entry, kw_only=True):
    """Trace-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.TRACE


class RaftDebug(Entry, kw_only=True):
    """Debug-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.DEBUG


class RaftInfo(Entry, kw_only=True):
    """Info-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.INFO


class RaftWarning(Entry, kw_only=True):
    """Warning-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.WARN


class RaftError(Entry, kw_only=True):
    """Error-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.ERROR


class RaftCritical(Entry, kw_only=True):
    """Critical-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.CRITICAL
