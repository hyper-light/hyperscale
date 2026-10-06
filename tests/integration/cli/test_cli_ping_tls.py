"""
`hyperscale ping` verifies the server it reaches; `--insecure` opts out.

The engine clients behind `hyperscale ping` used to skip certificate and
name checks for every TLS request. They now verify by default, as
curl does, and `--insecure` is the explicit opt-out for a self-signed
target.

Real `hyperscale` processes against a local HTTPS server whose certificate
is freshly self-signed:
* without `--insecure`, the handshake is refused: the server answers no
  request;
* with `--insecure`, the same command reaches it.
"""

import asyncio
import pathlib

import pytest

from tests.integration.cli.node_processes import HYPERSCALE, command_environment
from tests.unit.core.test_engine_tls_verification import HOST, start_https_target

COMMAND_TIMEOUT_SECONDS = 60


async def ping_https(port: int, *flags: str) -> int:
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE,
        "ping",
        "http",
        f"https://{HOST}:{port}/",
        "--quiet",
        *flags,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        env=command_environment(),
    )
    return await asyncio.wait_for(process.wait(), COMMAND_TIMEOUT_SECONDS)


@pytest.mark.asyncio
async def test_ping_refuses_a_self_signed_server_unless_told_insecure(tmp_path: pathlib.Path) -> None:
    server, target, port = await start_https_target(tmp_path)
    try:
        await ping_https(port)
        answered_when_verifying = target.requests_answered

        await ping_https(port, "--insecure")
        answered_when_insecure = target.requests_answered - answered_when_verifying
    finally:
        server.close()
        await server.wait_closed()

    assert answered_when_verifying == 0, "a self-signed server was reached without --insecure"
    assert answered_when_insecure == 1, "--insecure did not reach the self-signed server"
