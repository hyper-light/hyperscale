from typing import Any

import msgspec


class CallResult(msgspec.Struct):
    @classmethod
    def response_type(cls) -> Any:
        return None
