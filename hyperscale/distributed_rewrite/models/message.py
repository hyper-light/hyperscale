import orjson
import msgspec


class Message(msgspec.Struct):

    @classmethod
    def load(self, data: bytes):
        return type(self.__class__.__name__, self, orjson.loads(data))
    
    def dump(self):
        return orjson.dumps(
            msgspec.structs.asdict(self)
        )
    
    