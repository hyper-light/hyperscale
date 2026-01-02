import msgspec


class Message(msgspec.Struct):

    @classmethod
    def load(cls, data: bytes):
        """Deserialize bytes to this Message type."""
        return msgspec.json.decode(data, type=cls)
    
    def dump(self) -> bytes:
        """Serialize this Message to bytes."""
        return msgspec.json.encode(self)
