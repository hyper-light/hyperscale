from typing import Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlencode

import orjson
from pydantic import BaseModel, StrictBytes, StrictInt, StrictStr

from hyperscale.core.engines.client.shared.models import (
    URL,
    HTTPCookie,
    HTTPEncodableValue,
)
from hyperscale.core.engines.client.shared.protocols import NEW_LINE


class SMTPRequest(BaseModel):
    recipient: StrictStr
    sender: StrictStr
    server: StrictStr
    body: StrictStr


    class Config:
        arbitrary_types_allowed = True
