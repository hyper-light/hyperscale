from .bigtable_config import BigTableConfig as BigTableConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "BigTable":
        from .bigtable import BigTable

        globals()["BigTable"] = BigTable
        return BigTable

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
