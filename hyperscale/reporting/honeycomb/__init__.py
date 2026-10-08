from .honeycomb_config import HoneycombConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Honeycomb":
        from .honeycomb import Honeycomb

        globals()["Honeycomb"] = Honeycomb
        return Honeycomb

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
