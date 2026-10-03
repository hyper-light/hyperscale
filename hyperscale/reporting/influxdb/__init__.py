from .influxdb_config import InfluxDBConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "InfluxDB":
        from .influxdb import InfluxDB

        globals()["InfluxDB"] = InfluxDB
        return InfluxDB

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
