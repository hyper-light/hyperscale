"""
Ratchet lint -- every TCP action a node sends names a handler some node
defines.

A request whose action no server receives is answered "no TCP handler
named ..." -- or, before that error path existed, silently dropped -- so
the sender waits out its timeout for an answer that can never come. The
client polled gates for job status with ``job_status`` while the gate's
handler was named ``receive_job_status_request``: every status poll to a
gate, and ``hyperscale job status --gates``, failed, and no test noticed
because the only end-to-end test asked managers.

Collected from the source: the action of every ``send_tcp(...)`` /
``_send_tcp(...)`` call whose action argument is a string literal, and
the name of every method decorated ``@tcp.receive()``. An action computed
at runtime is not seen here.

A client may address either tier -- ``query_job_status`` and the status
poll take whichever node they are given -- so an action the client sends
must be received by gates and managers alike, unless it is one tier's
alone (``TIER_SPECIFIC_CLIENT_ACTIONS``, each with its reason). The gate's
ping (``receive_gate_ping``) and status (``receive_job_status_request``)
handlers were named apart from the manager's, so a global check passed
while every client request of both to a gate failed.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.simulation.lints.expected_unreceived_action_violations import (
    EXPECTED_UNRECEIVED_ACTION_VIOLATIONS,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale"
SEND_FUNCTIONS = frozenset({"send_tcp", "_send_tcp"})
NODES_ROOT = PRODUCTION_ROOT / "distributed" / "nodes"
# Receivers every gate and manager inherits (the SWIM and transport bases).
SHARED_SERVER_ROOTS = (PRODUCTION_ROOT / "distributed" / "swim", PRODUCTION_ROOT / "distributed" / "server")
TIER_SPECIFIC_CLIENT_ACTIONS: dict[str, str] = {
    # Datacenters are a gate's to list; a manager is one of them.
    "datacenter_list": "gate",
}


def _is_tcp_receive(decorator: ast.expr) -> bool:
    return (
        isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "receive"
        and isinstance(decorator.func.value, ast.Name)
        and decorator.func.value.id == "tcp"
    )


def _sent_action(call: ast.Call) -> str | None:
    callee = call.func
    name = callee.attr if isinstance(callee, ast.Attribute) else callee.id if isinstance(callee, ast.Name) else None
    if name not in SEND_FUNCTIONS:
        return None
    action = call.args[1] if len(call.args) >= 2 else next(
        (keyword.value for keyword in call.keywords if keyword.arg == "action"), None
    )
    if isinstance(action, ast.Constant) and isinstance(action.value, str):
        return action.value
    return None


def _collect() -> tuple[dict[str, list[str]], set[str]]:
    sent: dict[str, list[str]] = {}
    received: set[str] = set()
    for path in PRODUCTION_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text())
        relative_path = path.relative_to(REPO_ROOT).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
                _is_tcp_receive(decorator) for decorator in node.decorator_list
            ):
                received.add(node.name)
            elif isinstance(node, ast.Call) and (action := _sent_action(node)) is not None:
                sent.setdefault(action, []).append(f"{relative_path}:{node.lineno}")
    return sent, received


def _receivers_under(roots: list[Path]) -> set[str]:
    return {
        node.name
        for root in roots
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(_is_tcp_receive(decorator) for decorator in node.decorator_list)
    }


def test_every_client_action_is_received_by_each_tier_it_can_address() -> None:
    tier_receivers = {
        tier: _receivers_under([NODES_ROOT / tier, *SHARED_SERVER_ROOTS]) for tier in ("gate", "manager")
    }
    client_actions: dict[str, list[str]] = {}
    for path in (NODES_ROOT / "client").rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and (action := _sent_action(node)) is not None:
                client_actions.setdefault(action, []).append(f"{path.relative_to(REPO_ROOT).as_posix()}:{node.lineno}")

    unanswered = sorted(
        f"{action} -> {tier} (sent at {', '.join(locations)})"
        for action, locations in client_actions.items()
        for tier, receivers in tier_receivers.items()
        if action not in receivers and TIER_SPECIFIC_CLIENT_ACTIONS.get(action, tier) == tier
    )
    assert not unanswered, (
        "Client action(s) a tier does not receive -- a client addressing that "
        "tier waits out its timeout:\n  " + "\n  ".join(unanswered)
    )


def test_every_sent_action_has_a_receiver() -> None:
    sent, received = _collect()
    discovered = {action for action in sent if action not in received}
    expected = set(EXPECTED_UNRECEIVED_ACTION_VIOLATIONS)

    newly_introduced = sorted(discovered - expected)
    assert not newly_introduced, (
        "TCP action(s) sent that no node receives -- each request waits out "
        "its timeout for an answer that cannot come:\n  "
        + "\n  ".join(f"{action} (sent at {', '.join(sent[action])})" for action in newly_introduced)
    )

    stale_entries = sorted(expected - discovered)
    assert not stale_entries, (
        "Snapshot lists actions that now have receivers (or are no longer "
        "sent) -- remove them from expected_unreceived_action_violations.py "
        "so the ratchet keeps its teeth:\n  " + "\n  ".join(stale_entries)
    )
