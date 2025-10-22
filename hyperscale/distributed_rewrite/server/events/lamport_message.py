from typing import Literal
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat


class LamportMessage(BaseModel):
    message_type: Literal['update', 'ack'] = 'update'
    timestamp: StrictInt
    sender: StrictStr
    receiver: StrictStr

    def __repr__(self):
        return "Message {} at {} from {} to {}".format(
        	self.message_type, self.timestamp, 
        	self.sender, self.receiver)
