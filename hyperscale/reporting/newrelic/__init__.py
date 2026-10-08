from .newrelic_config import NewRelicConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "NewRelic":
        from .newrelic import NewRelic

        globals()["NewRelic"] = NewRelic
        return NewRelic

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
