from .timescaledb_config import TimescaleDBConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "TimescaleDB":
        from .timescaledb import TimescaleDB

        globals()["TimescaleDB"] = TimescaleDB
        return TimescaleDB

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
