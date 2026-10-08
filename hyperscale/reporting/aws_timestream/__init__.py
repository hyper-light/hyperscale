from .aws_timestream_config import AWSTimestreamConfig as AWSTimestreamConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "AWSTimestream":
        from .aws_timestream import AWSTimestream

        globals()["AWSTimestream"] = AWSTimestream
        return AWSTimestream

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
