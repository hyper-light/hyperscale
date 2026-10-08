"""Definitions shared by the classes of
``hyperscale.distributed.nodes.client.handlers.tcp_leadership_transfer`` (see that module)."""



def _addr_str(addr: tuple[str, int] | None) -> str:
    """Format address as string, or 'unknown' if None."""
    return f"{addr}" if addr else "unknown"
