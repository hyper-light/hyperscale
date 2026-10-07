import asyncio
import socket
import ssl


def get_peer_certificate_der(transport: asyncio.Transport) -> bytes | None:
    """
    Extract the peer's DER-encoded certificate from an SSL/TLS transport.

    Args:
        transport: The asyncio transport (must be SSL/TLS)

    Returns:
        DER-encoded certificate bytes, or None if not available
    """
    if not is_ssl(transport):
        return None

    return _peer_certificate_from_ssl_object(transport.get_extra_info("ssl_object"))


def _peer_certificate_from_ssl_object(ssl_object: ssl.SSLObject | None) -> bytes | None:
    """The peer's DER certificate from a TLS session object, or None when unavailable."""
    if ssl_object is None:
        return None

    try:
        # Get the peer certificate in DER format
        peer_cert_der = ssl_object.getpeercert(binary_form=True)
        return peer_cert_der
    except (AttributeError, ssl.SSLError):
        # Certificate not available (e.g., client didn't provide one)
        return None


def get_remote_addr(transport: asyncio.Transport) -> tuple[str, int] | None:
    socket_info = transport.get_extra_info("socket")
    if socket_info is not None:
        return _socket_peer_addr(socket_info)

    return _extra_info_addr(transport.get_extra_info("peername"))


def _socket_peer_addr(socket_info: socket.socket) -> tuple[str, int] | None:
    """The socket's (host, port) peer, or None for a non-IP peer or an unreadable one."""
    try:
        info = socket_info.getpeername()
        return (str(info[0]), int(info[1])) if isinstance(info, tuple) else None
    except OSError:  # pragma: no cover
        # This case appears to inconsistently occur with uvloop
        # bound to a unix domain socket.
        return None


def _extra_info_addr(info: object) -> tuple[str, int] | None:
    """A (host, port) from a transport's peername/sockname extra info, else None."""
    # isinstance rejects None, so no separate None check is needed.
    return (str(info[0]), int(info[1])) if isinstance(info, (list, tuple)) and len(info) == 2 else None


def get_local_addr(transport: asyncio.Transport) -> tuple[str, int] | None:
    socket_info = transport.get_extra_info("socket")
    if socket_info is not None:
        info = socket_info.getsockname()

        return (str(info[0]), int(info[1])) if isinstance(info, tuple) else None
    return _extra_info_addr(transport.get_extra_info("sockname"))


def is_ssl(transport: asyncio.Transport) -> bool:
    return bool(transport.get_extra_info("sslcontext"))