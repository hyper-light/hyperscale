import msgspec


class EmailAttachment(msgspec.Struct):
    path: str
    mime_type: str