from .snowflake_config import SnowflakeConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Snowflake":
        from .snowflake import Snowflake

        globals()["Snowflake"] = Snowflake
        return Snowflake

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
