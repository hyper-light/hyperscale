from typing import List, Literal, Optional, Tuple

from pydantic import (
    BaseModel,
    StrictBytes,
    StrictInt,
    StrictStr,
)

from hyperscale.core_rewrite.engines.client.shared.models import URL


class OptimizedGraphQLRequest(BaseModel):
    call_id: StrictInt
    url: Optional[URL] = None
    method: Literal["GET", "POST"]
    encoded_params: Optional[StrictStr | StrictBytes] = None
    encoded_auth: Optional[StrictStr | StrictBytes] = None
    encoded_cookies: Optional[
        StrictStr
        | StrictBytes
        | Tuple[StrictStr, StrictStr]
        | Tuple[StrictBytes, StrictBytes]
    ] = None
    encoded_headers: Optional[
        StrictBytes
        | StrictStr
        | List[StrictStr]
        | List[StrictBytes]
        | List[Tuple[StrictStr, StrictStr]]
        | List[Tuple[StrictBytes, StrictBytes]]
    ] = None
    encoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True
