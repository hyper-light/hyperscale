from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from .tabulate import Colorizer


class HeaderOptions(BaseModel):
    precision_format: StrictStr | None = None
    header_color: Colorizer | None = None
    data_color: Colorizer | None = None
    fixed: StrictBool = False
    default: StrictInt | StrictFloat | StrictBool | StrictStr | None = None
