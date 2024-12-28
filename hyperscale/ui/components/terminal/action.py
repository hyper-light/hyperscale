from .terminal import Terminal


def action(alias: str | None = None):

    def wrap(action):
        return Terminal.wrap_action(
            action,
            alias=alias,
        )
    
    return wrap