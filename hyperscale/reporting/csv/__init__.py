from .csv_config import CSVConfig as CSVConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "CSV":
        from .csv import CSV

        globals()["CSV"] = CSV
        return CSV

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
