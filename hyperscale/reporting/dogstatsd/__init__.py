from .dogstatsd_config import DogStatsDConfig as DogStatsDConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "DogStatsD":
        from .dogstatsd import DogStatsD

        globals()["DogStatsD"] = DogStatsD
        return DogStatsD

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
