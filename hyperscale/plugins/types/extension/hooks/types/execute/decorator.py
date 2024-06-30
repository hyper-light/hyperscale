import functools

from hyperscale.plugins.types.common.plugin_hook import PluginHook
from hyperscale.plugins.types.common.registrar import plugin_registrar
from hyperscale.plugins.types.common.types import PluginHooks


@plugin_registrar(PluginHooks.ON_EXTENSION_EXECUTE)
def execute():
    def wrapper(func) -> PluginHook:
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper
