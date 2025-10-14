import msgspec


class SMTPRequest(msgspec.Struct):
    recipient: str
    sender: str
    server: str
    body: str

