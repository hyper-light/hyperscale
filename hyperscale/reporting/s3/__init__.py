from .s3_config import S3Config


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "S3":
        from .s3 import S3

        globals()["S3"] = S3
        return S3

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
