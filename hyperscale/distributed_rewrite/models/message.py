import cloudpickle
import orjson
import msgspec
from typing import Literal, TypeVar, Any, Generic
from .error import Error


T = TypeVar("T")
M = TypeVar("M", bound=msgspec.Struct)


class Message(msgspec.Struct, Generic[M]):
    data: M | dict[str, Any]
    time: int
    call: str
    protocol: Literal['tcp', 'udp']
    sender: tuple[str, int]
    recipient: tuple[str, int]
    error: Error | None = None
    wraps: bool = False

    def load_json_data(self):
        return orjson.loads(self.data)
    
    def load_model_data(self, model: M):
        return model(**orjson.loads(self.data))
    
    def dump_json(self):
        return orjson.dumps(
            msgspec.structs.asdict(self)
        )
    
    @classmethod
    def load_json(
        cls,
        message: bytes
    ):
        return Message(**orjson.loads(message))
    
    
    @classmethod
    def load_json(
        cls,
        message: bytes
    ):
        return Message(**orjson.loads(message))