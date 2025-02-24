from pydantic import BaseModel, StrictStr


class EmailAttachment(BaseModel):
    path: StrictStr
    mime_type: StrictStr