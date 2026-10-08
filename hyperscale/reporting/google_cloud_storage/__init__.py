from .google_cloud_storage_config import GoogleCloudStorageConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "GoogleCloudStorage":
        from .google_cloud_storage import GoogleCloudStorage

        globals()["GoogleCloudStorage"] = GoogleCloudStorage
        return GoogleCloudStorage

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
