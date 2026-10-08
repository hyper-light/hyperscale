from .cosmosdb_config import CosmosDBConfig as CosmosDBConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "CosmosDB":
        from .cosmosdb import CosmosDB

        globals()["CosmosDB"] = CosmosDB
        return CosmosDB

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
