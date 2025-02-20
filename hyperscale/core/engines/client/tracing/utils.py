from urllib.parse import urlparse, urlunparse

try:
    
    from opentelemetry.trace import StatusCode

except Exception:
    class StatusCode:
        pass

SUPPRESS_INSTRUMENTATION_KEY_PLAIN = (
    "suppress_instrumentation"  # Set for backward compatibility
)

def remove_url_credentials(url: str) -> str:
    """Given a string url, remove the username and password only if it is a valid url"""

    try:
        parsed = urlparse(url)
        if all([parsed.scheme, parsed.netloc]):  # checks for valid url
            parsed_url = urlparse(url)
            _, _, netloc = parsed.netloc.rpartition("@")
            return urlunparse(
                (
                    parsed_url.scheme,
                    netloc,
                    parsed_url.path,
                    parsed_url.params,
                    parsed_url.query,
                    parsed_url.fragment,
                )
            )
    except ValueError:  # an unparsable url was passed
        pass
    return url

def http_status_to_status_code(
    status: int,
) -> StatusCode:
    """Converts an HTTP status code to an OpenTelemetry canonical status code

    Args:
        status (int): HTTP status code
    """

    if status < 100:
        return StatusCode.ERROR
    if status <= 299:
        return StatusCode.UNSET
    if status <= 399:
        return StatusCode.UNSET
    
    return StatusCode.ERROR