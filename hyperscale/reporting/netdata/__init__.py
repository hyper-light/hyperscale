from .netdata_config import NetdataConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Netdata":
        from .netdata import Netdata

        globals()["Netdata"] = Netdata
        return Netdata

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
