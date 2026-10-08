import msgspec


class RaftStoreHeader(msgspec.Struct, frozen=True, tag="header", array_like=True):
    """The first record of a store file: the stamp of the identity that
    wrote it, and the record format it is written in."""

    stamp: bytes
    format_version: int
