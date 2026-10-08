from .teleraf_statsd_config import TelegrafStatsDConfig as TelegrafStatsDConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "TelegrafStatsD":
        from .telegraf_statsd import TelegrafStatsD

        globals()["TelegrafStatsD"] = TelegrafStatsD
        return TelegrafStatsD

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
