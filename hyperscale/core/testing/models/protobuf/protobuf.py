import binascii
from typing import Generic, Optional, TypeVar

try:

    from google.protobuf.message import Message

except Exception:
    class Message:
        pass

from hyperscale.core.testing.models.base import OptimizedArg

from .protobuf_validator import ProtobufValidator

T = TypeVar("T")


class Protobuf(OptimizedArg, Generic[T]):
    def __init__(self, protobuf: Message) -> None:
        super(
            Protobuf,
            self,
        ).__init__()

        validated_protobuf = ProtobufValidator(value=protobuf)

        self.call_name: Optional[str] = None
        self.data = validated_protobuf.value
        self.optimized: Optional[bytes] = None

    async def optimize(self):
        if self.optimized is not None:
            return

        encoded_protobuf = str(
            binascii.b2a_hex(self.data.SerializeToString()),
            encoding="raw_unicode_escape",
        )
        encoded_message_length = (
            hex(int(len(encoded_protobuf) / 2)).lstrip("0x").zfill(8)
        )
        encoded_protobuf = f"00{encoded_message_length}{encoded_protobuf}"

        self.optimized = binascii.a2b_hex(encoded_protobuf)

    def __str__(self) -> str:
        return self.data.SerializeToString()

    def __repr__(self) -> str:
        return self.data.SerializeToString()

    def SerializeToString(self):
        return self.data.SerializeToString()
