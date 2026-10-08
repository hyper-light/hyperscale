"""A windowed-stats push is network input: it must be read through the
restricted unpickler like every other wire message.

The client handler (``nodes/client/handlers/tcp_windowed_stats.py:55``) and
the gate handler (``nodes/gate/server.py:2965``) read it with
``cloudpickle.loads`` at base commit 2e6d0532, which runs any callable a
payload names: a peer could execute code on the receiver. These tests send
a payload whose ``__reduce__`` calls ``os.system`` and assert it is refused
and never runs, while a legitimate ``WindowedStatsPush.dump()`` still loads.
"""

import os
import pickle
import shlex
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.models.security_error import SecurityError
from hyperscale.distributed.nodes.client.handlers import WindowedStatsPushHandler
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.logging import Logger


class ShellCommandPayload:
    """Unpickling this object runs ``os.system(command)``."""

    def __init__(self, command: str) -> None:
        self.command = command

    def __reduce__(self) -> tuple[object, tuple[str]]:
        return (os.system, (self.command,))


def malicious_payload_touching(marker_path: Path) -> bytes:
    """A pickle that, if unpickled unrestricted, creates ``marker_path``."""
    return pickle.dumps(ShellCommandPayload(f"touch {shlex.quote(str(marker_path))}"))


def client_handler_with_callback(job_id: str, received_pushes: list[WindowedStatsPush]) -> WindowedStatsPushHandler:
    """A client handler whose progress callback for ``job_id`` records each push."""
    state = ClientState()
    state._progress_callbacks[job_id] = received_pushes.append
    logger = Mock(spec=Logger)
    logger.log = AsyncMock()
    return WindowedStatsPushHandler(state, logger, None)


@pytest.mark.asyncio
async def test_client_refuses_code_executing_payload(tmp_path: Path) -> None:
    marker_path = tmp_path / "client_pwned"
    received_pushes: list[WindowedStatsPush] = []
    handler = client_handler_with_callback("job-1", received_pushes)

    result = await handler.handle(("manager", 9000), malicious_payload_touching(marker_path), 1)

    assert result == b"error"
    assert not marker_path.exists()
    assert received_pushes == []


@pytest.mark.asyncio
async def test_client_still_loads_legitimate_push() -> None:
    received_pushes: list[WindowedStatsPush] = []
    handler = client_handler_with_callback("job-1", received_pushes)
    push = WindowedStatsPush(job_id="job-1", workflow_id="workflow-1", completed_count=7)

    result = await handler.handle(("manager", 9000), push.dump(), 1)

    assert result == b"ok"
    assert [(received.job_id, received.completed_count) for received in received_pushes] == [("job-1", 7)]


@pytest.mark.asyncio
async def test_gate_refuses_code_executing_payload(tmp_path: Path) -> None:
    marker_path = tmp_path / "gate_pwned"
    job_manager = Mock()
    gate_stub = SimpleNamespace(_job_manager=job_manager)

    with pytest.raises(SecurityError):
        await GateServer._handle_windowed_stats_push(gate_stub, malicious_payload_touching(marker_path))

    assert not marker_path.exists()
    job_manager.has_job.assert_not_called()


@pytest.mark.asyncio
async def test_gate_still_loads_legitimate_push() -> None:
    job_manager = Mock()
    job_manager.has_job = Mock(return_value=False)
    udp_logger = Mock(spec=Logger)
    udp_logger.log = AsyncMock()
    gate_stub = SimpleNamespace(
        _job_manager=job_manager,
        _udp_logger=udp_logger,
        _host="127.0.0.1",
        _tcp_port=9100,
        _node_id=SimpleNamespace(short="gate-1"),
    )
    push = WindowedStatsPush(job_id="job-unknown", workflow_id="workflow-1", datacenter="dc-1")

    result = await GateServer._handle_windowed_stats_push(gate_stub, push.dump())

    assert result == b"discarded"
    job_manager.has_job.assert_called_once_with("job-unknown")
