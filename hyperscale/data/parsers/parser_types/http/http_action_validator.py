from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat, AnyHttpUrl, Json


from typing import List, Dict, Optional, Union


class HTTPActionTag(BaseModel):
    name: StrictStr
    value: StrictStr


class HTTPActionValidator(BaseModel):
    engine: StrictStr
    name: StrictStr
    url: AnyHttpUrl
    method: StrictStr = "GET"
    headers: Dict[StrictStr, StrictStr] = {}
    params: Optional[Dict[StrictStr, Union[StrictInt, StrictStr, StrictFloat]]]
    data: Optional[Union[StrictStr, Json]]
    weight: Optional[Union[StrictInt, StrictFloat]]
    order: Optional[StrictInt]
    user: Optional[StrictStr]
    tags: List[HTTPActionTag] = []
