from .kafka_config import KafkaConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Kafka":
        from .kafka import Kafka

        globals()["Kafka"] = Kafka
        return Kafka

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
