import msgspec


class Error(msgspec.Struct):
    message: str
    traceback: str
    node: tuple[str, int]