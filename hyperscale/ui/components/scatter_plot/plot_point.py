from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class PlotPoint(BaseModel):
    name: StrictStr
    value: StrictInt | StrictFloat
