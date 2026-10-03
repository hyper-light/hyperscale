from .xml_config import XMLConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "XML":
        from .xml import XML

        globals()["XML"] = XML
        return XML

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
