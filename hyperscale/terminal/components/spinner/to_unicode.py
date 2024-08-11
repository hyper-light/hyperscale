def to_unicode(text_type: str | bytes, encoding: str = "utf-8") -> str:
    if isinstance(text_type, bytes):
        return text_type.decode(encoding)
    return text_type
