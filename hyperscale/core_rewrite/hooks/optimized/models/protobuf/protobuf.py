from typing import Generic, Optional, TypeVar

from google.protobuf.message import Message

from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .protobuf_validator import ProtobufValidator

T = TypeVar("T")


class Protobuf(OptimizedArg, Generic[T]):
    def __init__(self, protobuf: Message) -> None:
        super(
            Protobuf,
            self,
        ).__init__()

        validated_protobuf = ProtobufValidator(value=protobuf)
        self.data = validated_protobuf.value
        self.optimized: Optional[str] = None

    def __str__(self) -> str:
        return self.data.SerializeToString()

    def __repr__(self) -> str:
        return self.data.SerializeToString()

    def SerializeToString(self):
        return self.data.SerializeToString()
