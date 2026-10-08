from .json_config import JSONConfig as JSONConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "JSON":
        from .json import JSON

        globals()["JSON"] = JSON
        return JSON

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
