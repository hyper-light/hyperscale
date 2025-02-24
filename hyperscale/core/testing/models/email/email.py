
import asyncio
from base64 import encodebytes as _bencode
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from pathlib import Path
from typing import (
    Generic,
    TypeVar,
    Literal
)

from hyperscale.core.testing.models.base import OptimizedArg, FrozenDict
from .email_attachment import EmailAttachment
from .email_validator import EmailValidator


T = TypeVar("T")


COMMASPACE = ', '

EmailAttachmentConfig = dict[
    Literal[
        'path',
        'mime_type'
    ],
    str
]

EmailData = dict[
    Literal[
        'sender',
        'recipients',
        'subject',
        'body',
        'attachments'
    ],
    str | list[str] | EmailAttachmentConfig | list[EmailAttachmentConfig]
]

class Email(OptimizedArg, Generic[T]):

    def __init__(
        self,
        email: EmailData
    ):
        super(
            Email,
            self,
        ).__init__()

        attachments: list[EmailAttachment] | EmailAttachment | None = None
        attachment_data = email.get('attachments')
        if isinstance(attachment_data, list):
            attachments = [
                EmailAttachment(
                    path=attachment.get('path'),
                    mime_type=attachment.get('mime_type')
                ) for attachment in attachment_data
            ]

        elif attachment_data:
            attachments = EmailAttachment(
                path=attachment_data.get('path'),
                mime_type=attachment_data.get('mime_type')
            )

        validated_email = EmailValidator(
            sender=email.get('sender'),
            recipients=email.get('recipients'),
            subject=email.get('subject'),
            body=email.get('body'),
            attachments=attachments
        )

        self.call_name: str | None = None
        self.data: Email = FrozenDict(validated_email.model_dump(exclude_none=True))
        self.optimized: str | None = None

    async def optimize(
        self,
        sender: str,
        recipients: str | list[str],
        subject: str,
        body: str,
        attachments: EmailAttachment | list[EmailAttachment] | None = None,
    ):
        loop = asyncio.get_event_loop()

        if isinstance(recipients, str):
            recipients = [recipients]

        email = MIMEMultipart()
        email['From'] = sender
        email['To'] = COMMASPACE.join(recipients)
        email['Date'] = formatdate(localtime=True)
        email['Subject'] = subject

        email.attach(MIMEText(body))

        if isinstance(attachments, EmailAttachment):
            attachments = [attachments]

        if attachments:
            attachment_parts = await asyncio.gather(*[
                self._attach_file(
                    attachment.path,
                    loop,
                    mime_type=attachment.mime_type,
                ) for attachment in attachments
            ])

            for part in attachment_parts:
                email.attach(part)

        self.optimized =  email.as_string()

    async def _attach_file(
        self,
        filepath: str,
        loop: asyncio.AbstractEventLoop,
        mime_type: str = 'application/octet-stream'
    ):
        mime_base, mime_subtype = mime_type.split('/', maxsplit=1)
        part = MIMEBase(mime_base, mime_subtype)

        attachment_file = await loop.run_in_executor(
            None,
            open,
            filepath,
            'rb'
        )

        part.set_payload(
            await loop.run_in_executor(
                None,
                attachment_file.read
            )
        )

        encoders.encode_base64(part)

        orig = part.get_payload(decode=True)
        encdata = str(_bencode(orig), 'ascii')
        part.set_payload(encdata)
        part['Content-Transfer-Encoding'] = 'base64'

        path = await loop.run_in_executor(
            None,
            Path,
            filepath
        )

        part.add_header(
            'Content-Disposition',
            f'attachment; filename={path.name}'
        )

        return part