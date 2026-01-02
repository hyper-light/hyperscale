import cloudpickle
from typing import Self


class Message:

    @classmethod
    def load(self, data: bytes) -> Self:
        return cloudpickle.loads(data)
    
    def dump(self):
        return cloudpickle.dumps(self)
    
    