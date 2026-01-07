from typing import Literal
from .components.terminal import action


StepStatsType = Literal["step", "total", "ok", "err"]

ExecutionStatsUpdate = dict[StepStatsType, str | int]


@action()
async def update_workflow_progress_seconds(
    workflow: str,
    elapsed: float,
):
    return (
        f"update_run_progress_seconds_{workflow}",
        int(elapsed),
    )


@action()
async def update_active_workflow_message(
    workflow: str,
    status: str,
):
    return (f"update_run_message_{workflow}", status)


@action()
async def update_workflow_run_timer(
    workflow: str,
    run_timer: bool,
):
    return (f"update_run_timer_{workflow}", run_timer)


@action()
async def update_workflow_executions_counter(
    workflow: str,
    count: int,
):
    return (f"update_total_executions_{workflow}", count)


@action()
async def update_workflow_executions_total_rate(
    workflow: str, count: int, run_timer: bool
):
    return (f"update_total_executions_rate_{workflow}", (count, run_timer))


@action()
async def update_workflow_executions_rates(
    workflow: str,
    execution_rates: list[tuple[float, int]],
):
    return (
        f"update_execution_rates_{workflow}",
        execution_rates,
    )


@action()
async def update_workflow_execution_stats(
    workflow: str, 
    execution_stats: dict[
        str, 
        dict[
            StepStatsType,
            int
        ]
    ],
) -> tuple[str, list[ExecutionStatsUpdate]]:
    
    return (
        f"update_execution_stats_{workflow}",
        [
            {
                "step": step,
                "ok": stats["ok"],
                "err": stats["err"],
                "total": stats["total"],
            }
            for step, stats in execution_stats.items()
        ],
    )
