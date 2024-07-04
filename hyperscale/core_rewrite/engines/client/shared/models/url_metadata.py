from typing import Optional

import msgspec


class URLMetadata(msgspec.Struct):
    host: str
    path: str
    params: Optional[str] = None
    query: Optional[str] = None
