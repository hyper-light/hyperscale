from .datadog_config import DatadogConfig as DatadogConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Datadog":
        from .datadog import Datadog

        globals()["Datadog"] = Datadog
        return Datadog

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
