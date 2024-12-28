from .terminal import Terminal
from hyperscale.ui.state import Action, ActionData
from typing import TypeVar


K = TypeVar('K')
T = TypeVar('T', bound=ActionData)


def action(action: Action[K, T]):
    return Terminal.wrap_action(action)