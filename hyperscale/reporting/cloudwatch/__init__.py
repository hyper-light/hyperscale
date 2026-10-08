from .cloudwatch_config import CloudwatchConfig as CloudwatchConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Cloudwatch":
        from .cloudwatch import Cloudwatch

        globals()["Cloudwatch"] = Cloudwatch
        return Cloudwatch

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
