from pathlib import Path
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class AddStyleTagCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: Optional[StrictStr] = None
    path: Optional[StrictStr | Path] = None
    content: Optional[StrictStr] = None
    timeout: StrictInt | StrictFloat
