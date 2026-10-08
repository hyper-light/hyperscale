"""
SWIM probe budgeting (AD-52 section 8): edges with high phi get more
frequent probes, healthy edges fewer -- within SWIM's fixed budget of one
probe per protocol period, so the randomized round-robin, and the bound it
puts on detection time, stand.
"""

import math
from itertools import filterfalse

from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.health.phi_accrual_detector import PhiAccrualDetector

MemberAddress = tuple[str, int]

# The false extra probes a round-robin cycle can afford: one probe period's
# worth of the budget -- the unit the budget is spent in, not a tuning.
FALSE_EXTRA_PROBES_PER_CYCLE = 1.0



class SwimProbeBudget:
    """Which member, if any, takes this period's probe instead of the
    round-robin's next: one whose phi -- computed over every message this
    node receives from it -- reached a threshold learned from how its extra
    probes turned out. Each member gets at most one extra probe per
    round-robin cycle, so a cycle stretches by at most one period per
    member and the full round-robin still runs.

    The threshold spends ``FALSE_EXTRA_PROBES_PER_CYCLE`` (B) on false
    alarms -- extra probes a member answered directly. It starts where a
    calibrated phi would need it: a cycle of N periods checks each of N
    members every period, so 10^-T * N^2 = B, T = 2 log10 N - log10 B. Phi
    is calibrated only if arrivals on the edge are as normal as its model
    assumes, which SWIM's mix of probes, acks and gossip is not -- and how
    far off it is depends on the threshold itself (a normal tail falls away
    far faster than a heavy one), so no single correction recalibrates it.
    So the threshold is steered by the budget's own outcome: each cycle,
    T += (F - B) / (B ln 10) for the F false alarms it spent -- the log
    correction log10(F / B) taken to first order around F = B, so a cycle
    at budget leaves it, an overshoot raises it, and one with none relaxes
    it by 1 / ln 10 decades. Steady, the expected step is 0: the cycles
    spend B on average whatever their false alarms' distribution (steering
    a log of them instead settles off B by however their spread bends it
    -- Jensen). False alarms only fall as T rises, so it settles; at most
    one extra probe per member per cycle bounds how far a cycle can
    overshoot.
    """

    __slots__ = (
        "_protocol_period_seconds",
        "_max_sample_size",
        "_min_std_deviation_seconds",
        "_detectors",
        "_synced_members",
        "_extra_probed_this_cycle",
        "_threshold",
        "_false_alarms_this_cycle",
        "_extra_probes",
        "_false_alarms",
        "_catches",
        "_cycles",
    )

    def __init__(
        self,
        protocol_period_seconds: float,
        max_sample_size: int,
        min_std_deviation_seconds: float,
        member_count: int,
    ) -> None:
        """
        Args:
            protocol_period_seconds: SWIM's probe interval
            max_sample_size: Inter-arrival times each edge's phi is computed over
            min_std_deviation_seconds: The floor under each edge's deviation
            member_count: The probed members at the start: the first
                threshold's prior, 2 log10 N
        """
        self._protocol_period_seconds = protocol_period_seconds
        self._max_sample_size = max_sample_size
        self._min_std_deviation_seconds = min_std_deviation_seconds
        self._detectors: dict[MemberAddress, PhiAccrualDetector] = {}
        self._synced_members: tuple[MemberAddress, ...] = ()
        self._extra_probed_this_cycle: set[MemberAddress] = set()
        self._threshold = (
            2.0 * math.log10(member_count) if member_count > 1 else 0.0
        ) - math.log10(FALSE_EXTRA_PROBES_PER_CYCLE)
        self._false_alarms_this_cycle = 0
        self._extra_probes = 0
        self._false_alarms = 0
        self._catches = 0
        self._cycles = 0

    def record_message(self, member: MemberAddress, now: float) -> None:
        """A message from ``member`` arrived at ``now``: a heartbeat on the
        edge, if ``member`` is probed."""
        if (detector := self._detectors.get(member)) is not None:
            detector.heartbeat(now)

    def next_extra_target(self, members: tuple[MemberAddress, ...], now: float) -> MemberAddress | None:
        """The member this period's probe goes to instead of the
        round-robin's next, or None. Every member still eligible for an
        extra probe this cycle is checked, and the first whose phi reached
        the threshold takes the probe; the others are checked again next
        period."""
        if members is not self._synced_members:
            self._sync_members(members)
        chosen = self._first_due_member(members, now)
        if chosen is not None:
            self._extra_probed_this_cycle.add(chosen)
            self._extra_probes += 1
        return chosen

    def _first_due_member(self, members: tuple[MemberAddress, ...], now: float) -> MemberAddress | None:
        """The first member, in round-robin order, due an extra probe this period, or None."""
        for member in members:
            if self._is_due_for_extra_probe(member, now):
                return member
        return None

    def _is_due_for_extra_probe(self, member: MemberAddress, now: float) -> bool:
        """Whether ``member`` is still eligible this cycle and its phi reached the learned threshold."""
        return (
            member not in self._extra_probed_this_cycle
            and (detector := self._detectors.get(member)) is not None
            and detector.phi(now) >= self._threshold
        )

    def record_extra_probe_outcome(self, answered_directly: bool) -> None:
        """How the extra probe went: answered directly, a false alarm; not,
        a catch -- the member's suspicion starts a cycle early."""
        if answered_directly:
            self._false_alarms_this_cycle += 1
            self._false_alarms += 1
        else:
            self._catches += 1

    def complete_cycle(self) -> None:
        """A round-robin cycle ended: steer the threshold by the false
        alarms it spent, and open every member to one extra probe again."""
        self._threshold += (self._false_alarms_this_cycle - FALSE_EXTRA_PROBES_PER_CYCLE) / (
            FALSE_EXTRA_PROBES_PER_CYCLE * math.log(10)
        )
        self._false_alarms_this_cycle = 0
        self._extra_probed_this_cycle.clear()
        self._cycles += 1

    def _sync_members(self, members: tuple[MemberAddress, ...]) -> None:
        """Keep a detector per probed member and none for anyone else; a
        new member's first interval is estimated as N/2 periods -- it probes
        this node once per its cycle and answers this node's probe once per
        this node's, two messages per N periods."""
        current = set(members)
        for departed in list(filterfalse(current.__contains__, self._detectors)):
            del self._detectors[departed]
            self._extra_probed_this_cycle.discard(departed)
        estimate = self._protocol_period_seconds * max(1, len(members)) / 2.0
        self._add_member_detectors(members, estimate)
        self._synced_members = members

    def _add_member_detectors(self, members: tuple[MemberAddress, ...], estimate: float) -> None:
        """Give each newly probed member a phi detector whose first interval is ``estimate``."""
        for member in members:
            if member not in self._detectors:
                self._detectors[member] = PhiAccrualDetector(
                    PhiAccrualConfig(
                        # Compared with the learned threshold, never this.
                        threshold=math.inf,
                        max_sample_size=self._max_sample_size,
                        min_std_deviation_seconds=self._min_std_deviation_seconds,
                        # Early is the point: the threshold itself is the
                        # tolerance.
                        acceptable_heartbeat_pause_seconds=0.0,
                        first_heartbeat_estimate_seconds=estimate,
                    )
                )

    @property
    def threshold(self) -> float:
        return self._threshold

    def get_stats(self) -> dict[str, float | int]:
        return {
            "threshold": self._threshold,
            "extra_probes": self._extra_probes,
            "false_alarms": self._false_alarms,
            "catches": self._catches,
            "cycles": self._cycles,
            "tracked_edges": len(self._detectors),
        }
