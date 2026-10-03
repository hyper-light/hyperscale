from .telegraf_config import TelegrafConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Telegraf":
        from .telegraf import Telegraf

        globals()["Telegraf"] = Telegraf
        return Telegraf

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
