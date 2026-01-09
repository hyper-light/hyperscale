from pathlib import Path
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class AddScriptTagCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: Optional[StrictStr] = None
    path: Optional[StrictStr | Path] = None
    content: Optional[StrictStr] = None
    tag_type: Optional[StrictStr] = None
    timeout: StrictInt | StrictFloat
