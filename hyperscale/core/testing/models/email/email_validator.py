from pydantic import BaseModel, StrictStr
from typing import List
from .email_attachment import EmailAttachment

class EmailValidator(BaseModel):
    sender: StrictStr
    recipients: StrictStr | List[StrictStr]
    subject: StrictStr
    body: StrictStr
    attachments: EmailAttachment | List[EmailAttachment] | None = None