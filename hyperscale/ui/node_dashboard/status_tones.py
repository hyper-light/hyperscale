"""The tone each state the nodes report is drawn in, and the badges and
counts built from them."""

from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.styling.tones import StatusTone

# Each state word a node reports -- a worker's state (WorkerState), an
# overload state (OverloadState), a datacenter's health (DatacenterHealth),
# a worker's connection to its managers (ClusterConnectionState), a
# backpressure level (BackpressureLevel), a degradation level
# (DegradationLevel) and a workflow's status (WorkflowStatus) -- by the
# tone it reads as: expected, worth a look, or in trouble.
STATE_TONES: dict[str, StatusTone] = {
    "healthy": "ok",
    "connected": "ok",
    "none": "ok",
    "normal": "ok",
    "pending": "ok",
    "assigned": "ok",
    "running": "ok",
    "completed": "ok",
    "aggregated": "ok",
    "busy": "degraded",
    "degraded": "degraded",
    "draining": "degraded",
    "stressed": "degraded",
    "initializing": "degraded",
    "connecting": "degraded",
    "reconnecting": "degraded",
    "throttle": "degraded",
    "batch": "degraded",
    "light": "degraded",
    "moderate": "degraded",
    "cancelled": "degraded",
    "offline": "failing",
    "unhealthy": "failing",
    "overloaded": "failing",
    "reject": "failing",
    "heavy": "failing",
    "critical": "failing",
    "failed": "failing",
    "aggregation_failed": "failing",
}
# A state the table above does not know is worth a look.
UNKNOWN_STATE_TONE: StatusTone = "degraded"
# Tones from least to most severe: a badge built of several states reads
# as the worst of them.
TONE_SEVERITY: dict[StatusTone, int] = {"ok": 0, "degraded": 1, "failing": 2}
# Between the parts of a badge's label: ASCII, as every mode draws it.
LABEL_SEPARATOR = ", "


def state_tone(state: str) -> StatusTone:
    """The tone ``state`` reads as."""
    return STATE_TONES.get(state, UNKNOWN_STATE_TONE)


def state_badge(state: str) -> StatusBadgeReading:
    """``state`` as a badge in its own tone (a table's status cell)."""
    return StatusBadgeReading(state, state_tone(state))


def worst_tone(tones: list[StatusTone]) -> StatusTone:
    """The most severe of ``tones``."""
    return max(tones, key=TONE_SEVERITY.__getitem__)


def nonzero_counts(counts: tuple[tuple[int, str], ...]) -> list[str]:
    """``"<count> <label>"`` for each count above zero, in order."""
    return [f"{count} {label}" for count, label in counts if count > 0]


def ratio_tone(up_count: int, known_count: int) -> StatusTone:
    """All of ``known_count`` up reads as expected, fewer as worth a look."""
    return "ok" if up_count >= known_count else "degraded"


def known_ratio_badges(name: str, up_count: int, known_count: int) -> list[StatusBadgeReading]:
    """A ``"<name> <up>/<known>"`` badge where any are known; none where
    none are (a node with no such peers has nothing to show)."""
    if known_count < 1:
        return []

    return [StatusBadgeReading(f"{name} {up_count}/{known_count}", ratio_tone(up_count, known_count))]


def joined_label(parts: list[str]) -> str:
    """A badge's label of several parts."""
    return LABEL_SEPARATOR.join(parts)
