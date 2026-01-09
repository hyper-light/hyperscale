from typing import (
    Optional,
    Sequence,
)

try:

    from playwright.async_api import ElementHandle

except Exception:
    class ElementHandle:
        pass

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class SelectOptionCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    selector: StrictStr
    value: Optional[StrictStr | Sequence[StrictStr]] = None
    index: Optional[StrictInt | Sequence[StrictInt]] = None
    label: Optional[StrictStr | Sequence[StrictStr]] = None
    element: Optional[ElementHandle | Sequence[ElementHandle]] = None
    no_wait_after: Optional[StrictBool] = None
    force: Optional[StrictBool] = None
    strict: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat
