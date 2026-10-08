from .mongodb_config import MongoDBConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "MongoDB":
        from .mongodb import MongoDB

        globals()["MongoDB"] = MongoDB
        return MongoDB

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
