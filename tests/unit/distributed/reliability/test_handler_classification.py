"""
Every server-side handler has exactly one AD-37 class, and the classifier
names only handlers that exist.

Admission (AD-32), rate limiting (AD-24) and load shedding classify a
request by its handler's name; an unclassified handler silently falls to
the default class and is shed at the wrong load. The classifier drifted
that way before: job submission, workflow dispatch, cancellation and
leadership transfers were unclassified, while 35 of its names belonged to
no handler. Read from the source itself, so a handler added without a
class fails here.
"""

import ast
import pathlib

from hyperscale.distributed.reliability.message_class import (
    CONTROL_HANDLERS,
    DATA_HANDLERS,
    DISPATCH_HANDLERS,
    TELEMETRY_HANDLERS,
)

DISTRIBUTED_ROOT = pathlib.Path(__file__).resolve().parents[4] / "hyperscale" / "distributed"
HANDLER_CLASSES = {
    "CONTROL": CONTROL_HANDLERS,
    "DISPATCH": DISPATCH_HANDLERS,
    "DATA": DATA_HANDLERS,
    "TELEMETRY": TELEMETRY_HANDLERS,
}


def server_handler_names() -> set[str]:
    """Every ``@tcp.receive()`` handler, and every ``@udp.receive()`` one
    that does not declare its own priority (SWIM's does)."""
    handler_names: set[str] = set()
    for path in DISTRIBUTED_ROOT.rglob("*.py"):
        source = path.read_text()
        if ".receive(" not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call):
                    continue
                decorator_name = ast.unparse(decorator.func)
                declares_priority = any(keyword.arg == "priority" for keyword in decorator.keywords)
                if decorator_name == "tcp.receive" or (decorator_name == "udp.receive" and not declares_priority):
                    handler_names.add(node.name)
    return handler_names


def test_every_server_handler_has_exactly_one_class() -> None:
    handler_names = server_handler_names()
    assert handler_names, f"no handlers found under {DISTRIBUTED_ROOT}"

    unclassified = sorted(
        handler_name
        for handler_name in handler_names
        if not any(handler_name in handler_class for handler_class in HANDLER_CLASSES.values())
    )
    assert unclassified == [], f"handlers with no AD-37 class: {unclassified}"

    multiply_classified = sorted(
        handler_name
        for handler_name in handler_names
        if sum(handler_name in handler_class for handler_class in HANDLER_CLASSES.values()) > 1
    )
    assert multiply_classified == [], f"handlers in more than one class: {multiply_classified}"


def test_the_classifier_names_only_real_handlers() -> None:
    handler_names = server_handler_names()

    stale_names = {
        class_name: sorted(handler_class - handler_names)
        for class_name, handler_class in HANDLER_CLASSES.items()
        if handler_class - handler_names
    }
    assert stale_names == {}, f"classified names that are no handler: {stale_names}"
