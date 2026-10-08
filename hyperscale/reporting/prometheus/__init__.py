from .prometheus_config import PrometheusConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "Prometheus":
        from .prometheus import Prometheus

        globals()["Prometheus"] = Prometheus
        return Prometheus

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
