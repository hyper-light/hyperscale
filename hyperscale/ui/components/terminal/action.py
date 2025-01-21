from hyperscale.ui.components.terminal.terminal import Terminal


def action(default_channel: str | None = None):
    def wrap(action):
        return Terminal.wrap_action(
            action,
            default_channel=default_channel,
        )

    return wrap
