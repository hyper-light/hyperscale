"""
SWIM probe budgeting (AD-52 section 8) -- the real ``ProbeScheduler`` and
``SwimProbeBudget``, one protocol period at a time on a seeded schedule.

Each edge carries what SWIM sends over it: the member's answers to this
node's probes, its own probes of this node (once per its cycle, at its own
phase), and gossip -- regular, or in heavy-tailed (Pareto) bursts nothing
like the normal arrivals phi's model assumes.

* The learned threshold spends the budget it is given -- one false extra
  probe (answered directly) per round-robin cycle -- on both kinds of edge.
  Frozen at its prior, what calibrated phi would need, a 30-member node
  overspends many times on heavy-tailed ones: the learning holds the
  budget, not the prior.
* A member that falls silent on a regular edge is probed far sooner than
  the round-robin would reach it; on a heavy-tailed edge, where silence is
  weak evidence, budgeting costs no more than the budget it spends.
"""

import random

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.swim.detection.probe_budget import FALSE_EXTRA_PROBES_PER_CYCLE, SwimProbeBudget
from hyperscale.distributed.swim.detection.probe_scheduler import ProbeScheduler
from tests.simulation.harness.sim import SeededRandom

SETTINGS = Env()
PROTOCOL_PERIOD_SECONDS = 1.0
# A LAN's round trip, far inside a period.
ANSWER_DELAY_SECONDS = 0.002
# Gossip burst gaps: Pareto with shape 1.5 -- finite mean, infinite
# variance -- scaled to a few periods.
GOSSIP_PARETO_SHAPE = 1.5
GOSSIP_GAP_SCALE_SECONDS = 2.0
SEEDS = (5, 13, 21, 34, 55)


def member_addresses(member_count: int) -> tuple[tuple[str, int], ...]:
    return tuple(("10.0.0.1", 9000 + slot) for slot in range(member_count))


def run_probe_budget(
    seed: int,
    member_count: int,
    cycles: int,
    learn: bool,
    failed_member_at: tuple[int, float] | None = None,
    budgeting: bool = True,
    heavy_tailed_gossip: bool = True,
) -> tuple[list[int], float | None, float | None]:
    """Probe ``member_count`` members for ``cycles`` round-robin cycles.
    Returns the false extra probes each cycle spent, and -- for a member
    failing at ``failed_member_at`` (slot, time) -- when it was first
    probed after failing, and when the round-robin alone would have."""
    draw = random.Random(seed)
    snapshot = snapshot_defaults()
    swap_defaults(random_source=SeededRandom(seed=seed))
    try:
        members = member_addresses(member_count)
        scheduler = ProbeScheduler()
        scheduler.update_members(list(members))
        budget = SwimProbeBudget(
            protocol_period_seconds=PROTOCOL_PERIOD_SECONDS,
            max_sample_size=SETTINGS.PHI_ACCRUAL_MAX_SAMPLE_SIZE,
            min_std_deviation_seconds=SETTINGS.PHI_ACCRUAL_MIN_STD_DEVIATION_SECONDS,
            member_count=member_count,
        )
        failed_member = members[failed_member_at[0]] if failed_member_at is not None else None
        failed_at = failed_member_at[1] if failed_member_at is not None else float("inf")

        # Each member's own traffic to this node, as (time, member) events:
        # its probes of this node once per its cycle, and gossip bursts.
        horizon = (cycles + 2) * member_count * PROTOCOL_PERIOD_SECONDS * 2
        pending: list[tuple[float, tuple[str, int]]] = []
        for member in members:
            phase = draw.uniform(0, member_count * PROTOCOL_PERIOD_SECONDS)
            sent_at = phase
            while sent_at < horizon:
                pending.append((sent_at, member))
                sent_at += member_count * PROTOCOL_PERIOD_SECONDS
            sent_at = draw.uniform(0, GOSSIP_GAP_SCALE_SECONDS)
            while sent_at < horizon:
                pending.append((sent_at, member))
                sent_at += GOSSIP_GAP_SCALE_SECONDS * (
                    draw.paretovariate(GOSSIP_PARETO_SHAPE) if heavy_tailed_gossip else draw.uniform(0.5, 1.5)
                )
        pending.sort()

        false_alarms_per_cycle: list[int] = []
        false_alarms_this_cycle = 0
        first_probe_after_failure: float | None = None
        round_robin_turn_after_failure: float | None = None
        now = 0.0
        event_index = 0
        while len(false_alarms_per_cycle) < cycles:
            # Deliver what arrived during the period.
            while event_index < len(pending) and pending[event_index][0] <= now:
                sent_at, member = pending[event_index]
                event_index += 1
                if member != failed_member or sent_at < failed_at:
                    budget.record_message(member, sent_at)

            extra_target = budget.next_extra_target(scheduler.members, now) if budgeting else None
            if extra_target is not None:
                target = extra_target
            else:
                cycles_before = scheduler.cycles_completed
                target = scheduler.get_next_target()
                if scheduler.cycles_completed != cycles_before:
                    false_alarms_per_cycle.append(false_alarms_this_cycle)
                    false_alarms_this_cycle = 0
                    if learn:
                        budget.complete_cycle()
                    else:
                        # Frozen at the prior: the cycle's evidence is
                        # dropped, the calibrated-phi threshold kept.
                        threshold = budget.threshold
                        budget.complete_cycle()
                        budget._threshold = threshold
                if (
                    target == failed_member
                    and now >= failed_at
                    and round_robin_turn_after_failure is None
                ):
                    round_robin_turn_after_failure = now

            answered = target != failed_member or now < failed_at
            if extra_target is not None:
                budget.record_extra_probe_outcome(answered)
                if answered:
                    false_alarms_this_cycle += 1
            if answered:
                budget.record_message(target, now + ANSWER_DELAY_SECONDS)
            elif first_probe_after_failure is None:
                first_probe_after_failure = now
            now += PROTOCOL_PERIOD_SECONDS
        return false_alarms_per_cycle, first_probe_after_failure, round_robin_turn_after_failure
    finally:
        restore_defaults(snapshot)


# The learned cycles are judged over their second half: 150 cycles, a mean
# whose standard error for Poisson(1) false alarms is sqrt(1/150) ~ 0.08.
# Four standard errors either side of the budget.
_SETTLED_CYCLES = 150
_BUDGET_BAND = (1.0 - 4 * (1 / _SETTLED_CYCLES) ** 0.5, 1.0 + 4 * (1 / _SETTLED_CYCLES) ** 0.5)


def settled_false_alarm_rate(seed: int, member_count: int, learn: bool, heavy_tailed_gossip: bool = True) -> float:
    per_cycle, _, _ = run_probe_budget(
        seed, member_count, 2 * _SETTLED_CYCLES, learn=learn, heavy_tailed_gossip=heavy_tailed_gossip
    )
    return sum(per_cycle[_SETTLED_CYCLES:]) / _SETTLED_CYCLES


@pytest.mark.parametrize("heavy_tailed_gossip", [True, False])
@pytest.mark.parametrize("member_count", [10, 30])
def test_the_learned_threshold_spends_one_false_extra_probe_per_cycle(
    member_count: int, heavy_tailed_gossip: bool
) -> None:
    learned_rates = [
        settled_false_alarm_rate(seed, member_count, learn=True, heavy_tailed_gossip=heavy_tailed_gossip)
        for seed in SEEDS
    ]
    assert all(_BUDGET_BAND[0] <= rate <= _BUDGET_BAND[1] for rate in learned_rates), (learned_rates, _BUDGET_BAND)


def test_the_prior_alone_overspends_on_heavy_tailed_edges() -> None:
    """With 30 members the threshold calibrated phi would need spends many
    times the budget on heavy-tailed edges -- the learning, not the prior,
    holds it. (With 10, the prior happens to land near the budget.)"""
    frozen_rates = [settled_false_alarm_rate(seed, 30, learn=False) for seed in SEEDS]
    assert all(rate > 4 * _BUDGET_BAND[1] for rate in frozen_rates), frozen_rates


# Failures placed across a cycle, past the threshold's settling.
_FAILURE_PHASES = 10
_CATCH_MEMBER_COUNT = 30


def first_probe_delays(heavy_tailed_gossip: bool, budgeting: bool) -> list[float]:
    """How long after failing a member was first probed, for failures at
    every tenth of a cycle across the seeds."""
    delays = []
    for seed in SEEDS:
        for phase in range(_FAILURE_PHASES):
            failed_at = (40 + phase / _FAILURE_PHASES) * _CATCH_MEMBER_COUNT + 0.5
            _, first_probe, _ = run_probe_budget(
                seed,
                _CATCH_MEMBER_COUNT,
                cycles=60,
                learn=True,
                failed_member_at=(7, failed_at),
                budgeting=budgeting,
                heavy_tailed_gossip=heavy_tailed_gossip,
            )
            assert first_probe is not None, (seed, phase)
            delays.append(first_probe - failed_at)
    return delays


def test_a_silent_member_on_a_regular_edge_is_probed_far_sooner() -> None:
    """Where an edge's traffic is regular, a member's silence stands out:
    its phi passes the threshold long before the round-robin reaches it
    (measured: a mean of 4.56 periods against the round-robin's 14.2)."""
    budgeted = first_probe_delays(heavy_tailed_gossip=False, budgeting=True)
    round_robin = first_probe_delays(heavy_tailed_gossip=False, budgeting=False)
    assert sum(budgeted) / len(budgeted) < (sum(round_robin) / len(round_robin)) / 2, (budgeted, round_robin)


def test_on_heavy_tailed_edges_budgeting_costs_at_most_its_budget() -> None:
    """Where an edge's traffic is heavy-tailed, silence is weak evidence:
    the threshold sits high and the round-robin usually gets there first.
    Budgeting then costs what it spends -- each false extra probe delays
    the round-robin by one period, B of them a cycle -- and no more
    (measured: 14.28 periods against 14.2)."""
    budgeted = first_probe_delays(heavy_tailed_gossip=True, budgeting=True)
    round_robin = first_probe_delays(heavy_tailed_gossip=True, budgeting=False)
    assert sum(budgeted) / len(budgeted) <= sum(round_robin) / len(round_robin) + (
        FALSE_EXTRA_PROBES_PER_CYCLE * PROTOCOL_PERIOD_SECONDS
    ), (budgeted, round_robin)
