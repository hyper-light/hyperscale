from .graphite_config import GraphiteConfig as GraphiteConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Graphite":
        from .graphite import Graphite

        globals()["Graphite"] = Graphite
        return Graphite

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
