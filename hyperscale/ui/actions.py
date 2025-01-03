from .components.terminal import action


@action()
async def update_active_workflow_message(workflow: str, status: str):
    return (
        f'update_run_message_{workflow}',
        status
    )