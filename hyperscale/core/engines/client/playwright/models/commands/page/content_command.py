from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class ContentCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timeout: StrictInt | StrictFloat
