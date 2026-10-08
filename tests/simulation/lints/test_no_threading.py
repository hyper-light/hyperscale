"""
Forbids the ``threading`` module in production code (CLAUDE.md: "Do not
use threading module items EVER. ALWAYS defer to the asyncio counterpart
of a threading item").

Every module under ``hyperscale/`` is checked for ``import threading``
and ``from threading import ...``, at any depth (an import inside a
function counts too). The count is zero, and stays zero, with one file
excepted: the vendored asyncssh tun/tap transport
(``hyperscale/core/engines/client/ssh/protocol/ssh/tuntap.py``), whose
macOS reader runs on a thread because kqueue cannot watch a tuntaposx
device. It is upstream vendored code that hyperscale's engines never call
(tun/tap forwarding), so it is left as vendored (decided 2026-10-06, G-70).
"""

from __future__ import annotations

import ast

from tests.simulation.lints.ratchet import production_modules

VENDORED_TUNTAP = "hyperscale/core/engines/client/ssh/protocol/ssh/tuntap.py"


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
        if path != VENDORED_TUNTAP
        for line in threading_imports(module)
    ]
    assert not violations, "threading imported:\n    " + "\n    ".join(violations)
