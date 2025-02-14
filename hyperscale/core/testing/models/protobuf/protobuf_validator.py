
try:

    from google.protobuf.message import Message

except Exception:
    class Message:
        pass

from pydantic import BaseModel


class ProtobufValidator(BaseModel):
    value: Message

    class Config:
        arbitrary_types_allowed = True
