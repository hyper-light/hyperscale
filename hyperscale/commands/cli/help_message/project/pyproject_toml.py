from pydantic import BaseModel, StrictStr


class Project(BaseModel):
    name: StrictStr
    version: StrictStr

class PyProjectToml(BaseModel):
    project: Project