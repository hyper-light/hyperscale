"""
Ratchet lint -- imports belong at the top of the module (Phase 5): an
import inside a function hides a dependency edge, runs a lookup each call,
and usually papers over an import cycle that should be broken instead.

Imports under ``if TYPE_CHECKING:`` are module-level and never counted.
Today's inline imports are snapshotted per function: none may be added,
and each one hoisted lowers its entry.

Two kinds of inline import are correct and exempt, because each defers an
optional dependency rather than hiding a cycle:

* an optional-dependency probe -- an import inside a ``try`` whose handler
  catches ``ImportError`` or ``ModuleNotFoundError``: the code runs with
  or without the library, so the import cannot sit unguarded at the top.
* a reporting backend's lazy loader -- the PEP 562 module ``__getattr__``
  of a ``hyperscale/reporting/<backend>/__init__.py`` importing its own
  reporter relatively: the reporter imports the backend's third-party
  client, an optional extra, which only a run that selects the backend
  may require.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator

from tests.simulation.lints.expected_inline_import_violations import EXPECTED_INLINE_IMPORT_VIOLATIONS
from tests.simulation.lints.ratchet import (
    FUNCTION_NODES,
    REPO_ROOT,
    functions_with_qualnames,
    own_nodes,
    production_modules,
    ratchet_failures,
    write_snapshot,
)

IMPORT_NODES = (ast.Import, ast.ImportFrom)
OPTIONAL_DEPENDENCY_ERRORS = frozenset({"ImportError", "ModuleNotFoundError"})
REPORTING_ROOT = "hyperscale/reporting/"


def handler_catches_import_error(handler: ast.ExceptHandler) -> bool:
    caught = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return any(isinstance(error, ast.Name) and error.id in OPTIONAL_DEPENDENCY_ERRORS for error in caught)


def statements_own_nodes(statements: list[ast.stmt]) -> Iterator[ast.AST]:
    """The nodes of ``statements``, stopping at nested functions, classes and lambdas."""
    pending: list[ast.AST] = list(statements)
    while pending:
        node = pending.pop()
        if isinstance(node, FUNCTION_NODES + (ast.ClassDef, ast.Lambda)):
            continue
        yield node
        pending.extend(ast.iter_child_nodes(node))


def optional_dependency_probes(function: ast.AST) -> set[int]:
    """The ids of ``function``'s imports guarded by an ``ImportError`` handler."""
    return {
        id(node)
        for guard in own_nodes(function)
        if isinstance(guard, ast.Try) and any(map(handler_catches_import_error, guard.handlers))
        for node in statements_own_nodes(guard.body)
        if isinstance(node, IMPORT_NODES)
    }


def is_reporting_lazy_loader(path: str, qualname: str, imports: list[ast.Import | ast.ImportFrom]) -> bool:
    return (
        path.startswith(REPORTING_ROOT)
        and path.endswith("/__init__.py")
        and qualname == "__getattr__"
        and all(isinstance(node, ast.ImportFrom) and node.level > 0 for node in imports)
    )


def counted_imports(path: str, qualname: str, function: ast.AST) -> int:
    probes = optional_dependency_probes(function)
    imports = [node for node in own_nodes(function) if isinstance(node, IMPORT_NODES) and id(node) not in probes]
    return 0 if is_reporting_lazy_loader(path, qualname, imports) else len(imports)


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        for qualname, function in functions_with_qualnames(module):
            if (count := counted_imports(path, qualname, function)) > 0:
                site = f"{path}::{qualname}"
                discovered[site] = discovered.get(site, 0) + count
    return discovered


def test_no_function_imports() -> None:
    failures = ratchet_failures(discover(), EXPECTED_INLINE_IMPORT_VIOLATIONS, __name__)
    assert not failures, "Imports inside functions:\n  " + "\n  ".join(failures)


def test_the_exemptions_cover_only_optional_dependencies() -> None:
    probe = ast.parse(
        "def guarded():\n"
        "    try:\n"
        "        import numpy\n"
        "    except ImportError:\n"
        "        pass\n"
        "    import json\n"
        "def unguarded():\n"
        "    try:\n"
        "        import numpy\n"
        "    except ValueError:\n"
        "        pass\n"
    )
    lazy_loader = ast.parse("def __getattr__(name):\n    from .kafka import Kafka\n").body[0]
    mixed_loader = ast.parse("def __getattr__(name):\n    from .kafka import Kafka\n    import os\n").body[0]
    counts = {
        qualname: counted_imports("hyperscale/core/probe.py", qualname, function)
        for qualname, function in functions_with_qualnames(probe)
    }
    assert counts == {"guarded": 1, "unguarded": 1}
    assert counted_imports("hyperscale/reporting/kafka/__init__.py", "__getattr__", lazy_loader) == 0
    assert counted_imports("hyperscale/reporting/kafka/__init__.py", "__getattr__", mixed_loader) == 2
    assert counted_imports("hyperscale/core/kafka/__init__.py", "__getattr__", lazy_loader) == 1


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_inline_import_violations.py",
        "EXPECTED_INLINE_IMPORT_VIOLATIONS",
        "Imports inside functions, per function, today (test_no_inline_imports).",
        discover(),
    )
