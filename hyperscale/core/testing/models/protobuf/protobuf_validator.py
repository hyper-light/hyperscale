
try:

    from google.protobuf.message import Message

except Exception:
    class Message:
        pass

from pydantic import BaseModel, ConfigDict


class ProtobufValidator(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: Message
