"""
Wire contract: every TCP action a node sends names a handler some node
serves.

A TCP request is dispatched by its action name to a ``@tcp.receive()``
method of the same name; an action no node defines is answered with an
error by the receiver (and ``send_tcp`` reports failures as values, not
exceptions), so a misspelled action fails on every send while looking
like an ordinary call. Found this way: all five AD-34 manager -> gate
timeout reports, the gate -> manager global timeout, and manager -> manager
state sync had never reached a handler.

Scans ``hyperscale/distributed`` statically for literal action names at
the send sites (``send_tcp`` and the wrappers that pass the action
positionally) and in action-carrying keyword arguments, and checks each
against the set of ``@tcp.receive()`` handler names.
"""

import ast
import pathlib
from collections import defaultdict

import hyperscale.distributed

DISTRIBUTED_ROOT = pathlib.Path(hyperscale.distributed.__file__).parent

# Callable name -> index of its positional action argument.
ACTION_ARGUMENT_POSITIONS = {
    "send_tcp": 1,
    "_send_tcp": 1,
    "_send_to_gate_peer": 1,
    "_send_to_gate": 2,
}
ACTION_KEYWORDS = frozenset({"forward_method", "handler_name"})
# Below this, the scan itself is broken (the tree has ~50 literal actions).
MINIMUM_EXPECTED_ACTIONS = 40


def _parsed_modules() -> list[tuple[pathlib.Path, ast.Module]]:
    return [(path, ast.parse(path.read_text())) for path in sorted(DISTRIBUTED_ROOT.rglob("*.py"))]


def _is_tcp_receive(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "receive"
        and isinstance(target.value, ast.Name)
        and target.value.id == "tcp"
    )


def _tcp_handler_names(modules: list[tuple[pathlib.Path, ast.Module]]) -> set[str]:
    return {
        node.name
        for _, module in modules
        for node in ast.walk(module)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and any(_is_tcp_receive(decorator) for decorator in node.decorator_list)
    }


def _called_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _literal_actions(call: ast.Call) -> list[str]:
    candidates: list[ast.expr] = [
        keyword.value for keyword in call.keywords if keyword.arg in ACTION_KEYWORDS
    ]
    if (position := ACTION_ARGUMENT_POSITIONS.get(_called_name(call))) is not None and len(call.args) > position:
        candidates.append(call.args[position])
    return [
        candidate.value
        for candidate in candidates
        if isinstance(candidate, ast.Constant) and isinstance(candidate.value, str)
    ]


def _sent_actions(modules: list[tuple[pathlib.Path, ast.Module]]) -> dict[str, list[str]]:
    sites: dict[str, list[str]] = defaultdict(list)
    for path, module in modules:
        for node in ast.walk(module):
            if isinstance(node, ast.Call):
                for action in _literal_actions(node):
                    sites[action].append(f"{path.relative_to(DISTRIBUTED_ROOT)}:{node.lineno}")
    return sites


def test_every_sent_tcp_action_has_a_handler() -> None:
    modules = _parsed_modules()
    handlers = _tcp_handler_names(modules)
    sent = _sent_actions(modules)

    assert len(sent) >= MINIMUM_EXPECTED_ACTIONS, sorted(sent)
    unserved = {action: sites for action, sites in sent.items() if action not in handlers}
    assert unserved == {}, unserved
