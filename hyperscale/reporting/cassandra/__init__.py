from .cassandra_config import CassandraConfig as CassandraConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Cassandra":
        from .cassandra import Cassandra

        globals()["Cassandra"] = Cassandra
        return Cassandra

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
