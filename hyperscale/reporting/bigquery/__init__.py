from .bigquery_config import BigQueryConfig as BigQueryConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "BigQuery":
        from .bigquery import BigQuery

        globals()["BigQuery"] = BigQuery
        return BigQuery

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
