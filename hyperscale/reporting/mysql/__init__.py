from .mysql_config import MySQLConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "MySQL":
        from .mysql import MySQL

        globals()["MySQL"] = MySQL
        return MySQL

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
