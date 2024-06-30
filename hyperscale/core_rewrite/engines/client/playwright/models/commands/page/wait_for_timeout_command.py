from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class WaitForTimeoutCommand(BaseModel):
    timeout: StrictInt | StrictFloat
