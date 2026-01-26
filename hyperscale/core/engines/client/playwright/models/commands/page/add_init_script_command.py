from pathlib import Path
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class AddInitScriptCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    script: Optional[StrictStr] = None
    path: Optional[StrictStr | Path] = None
    timeout: StrictInt | StrictFloat
