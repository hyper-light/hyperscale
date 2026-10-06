"""``CertificateParseError`` -- pickled under the namespace
``hyperscale.distributed.discovery.security.role_validator`` (see that module)."""



class CertificateParseError(Exception):
    """Raised when certificate parsing fails in strict mode."""

    def __init__(self, message: str, parse_error: Exception | None = None):
        self.parse_error = parse_error
        super().__init__(message)
