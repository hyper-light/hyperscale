from pydantic import BaseModel


class _CloudwatchTarget(BaseModel):
    arn: str
    id: str
