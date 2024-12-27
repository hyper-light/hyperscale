from pydantic import AnyUrl, BaseModel


class LinkValidator(BaseModel):
    url: AnyUrl
