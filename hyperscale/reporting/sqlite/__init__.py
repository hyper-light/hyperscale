from .sqlite_config import SQLiteConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "SQLite":
        from .sqlite import SQLite

        globals()["SQLite"] = SQLite
        return SQLite

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
