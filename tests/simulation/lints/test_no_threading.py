"""
Forbids the ``threading`` module in production code (CLAUDE.md: "Do not
use threading module items EVER. ALWAYS defer to the asyncio counterpart
of a threading item").

Every module under ``hyperscale/`` is checked for ``import threading``
and ``from threading import ...``, at any depth (an import inside a
function counts too).

The client engines under ``hyperscale/core/engines`` are not checked:
their executors and vendored SSH tunnelling use threads, and whether they
get a carve-out or a conversion is the owner's open decision (G-70).
Everywhere else the count is zero, and stays zero.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.ratchet import production_modules

UNDECIDED_PREFIX = "hyperscale/core/engines/"


def threading_imports(module: ast.Module) -> list[int]:
    """Line numbers of every ``threading`` import in ``module``."""
    lines: list[int] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Import) and any(
            alias.name == "threading" or alias.name.startswith("threading.") for alias in node.names
        ):
            lines.append(node.lineno)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None and (
            node.module == "threading" or node.module.startswith("threading.")
        ):
            lines.append(node.lineno)
    return lines


def test_no_production_module_imports_threading() -> None:
    violations = [
        f"{path}:{line}"
        for path, module in production_modules()
        if not path.startswith(UNDECIDED_PREFIX)
        for line in threading_imports(module)
    ]
    assert not violations, "threading imported outside the engines:\n    " + "\n    ".join(violations)
