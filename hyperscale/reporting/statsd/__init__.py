from .statsd_config import StatsDConfig as StatsDConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "StatsD":
        from .statsd import StatsD

        globals()["StatsD"] = StatsD
        return StatsD

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
