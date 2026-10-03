from .redis_config import RedisConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Redis":
        from .redis import Redis

        globals()["Redis"] = Redis
        return Redis

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
