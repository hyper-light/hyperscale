from typing import Optional

from pydantic import BaseModel, StrictBytes, StrictInt

from hyperscale.core_rewrite.engines.client.shared.models import URL


class OptimizedUDPRequest(BaseModel):
    call_id: StrictInt
    url: Optional[URL] = None
    encoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True
