from .postgres_config import PostgresConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Postgres":
        from .postgres import Postgres

        globals()["Postgres"] = Postgres
        return Postgres

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
