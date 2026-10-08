"""
Shared machinery for snapshot ratchet lints: a lint counts what it
forbids, per site, and the snapshot records today's counts. A site that
is new, or counts more than its snapshot, fails -- the rule holds for all
new code. A site that counts less, or is gone, fails too until the
snapshot is lowered: a count, once paid down, can never quietly rise
back.

Each lint regenerates its own snapshot (``python -m <lint module>``)
through ``write_snapshot``.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale"
FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


def production_modules() -> Iterator[tuple[str, ast.Module]]:
    """Every module under ``hyperscale/``, parsed, by repo-relative path."""
    for path in sorted(PRODUCTION_ROOT.rglob("*.py")):
        if "__pycache__" not in path.parts:
            yield path.relative_to(REPO_ROOT).as_posix(), ast.parse(path.read_text())


def functions_with_qualnames(module: ast.Module) -> Iterator[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]]:
    """Every function in ``module`` with its dotted qualified name -- the
    classes and functions enclosing it, outermost first."""
    pending: list[tuple[str, ast.AST]] = [("", module)]
    while pending:
        prefix, parent = pending.pop()
        for child in ast.iter_child_nodes(parent):
            if isinstance(child, FUNCTION_NODES + (ast.ClassDef,)):
                qualname = f"{prefix}{child.name}"
                if isinstance(child, FUNCTION_NODES):
                    yield qualname, child
                pending.append((f"{qualname}.", child))
            else:
                pending.append((prefix, child))


def own_nodes(function: ast.AST) -> Iterator[ast.AST]:
    """The nodes of ``function``'s own body: nested functions, classes and
    lambdas are sites of their own."""
    pending = list(ast.iter_child_nodes(function))
    while pending:
        node = pending.pop()
        if isinstance(node, FUNCTION_NODES + (ast.ClassDef, ast.Lambda)):
            continue
        yield node
        pending.extend(ast.iter_child_nodes(node))


def ratchet_failures(discovered: dict[str, int], expected: dict[str, int], snapshot_module: str) -> list[str]:
    """What breaks the ratchet: sites new or worse than the snapshot, and
    snapshot entries that are better now or gone (lower the snapshot)."""
    regenerate = f"regenerate with `uv run python -m {snapshot_module}`"
    return [
        *(
            f"{site}: {count} (snapshot {expected.get(site, 0)})"
            for site, count in sorted(discovered.items())
            if count > expected.get(site, 0)
        ),
        *(
            f"{site}: snapshot {count}, now {discovered.get(site, 0)} -- lower it ({regenerate})"
            for site, count in sorted(expected.items())
            if discovered.get(site, 0) < count
        ),
    ]


def write_snapshot(path: Path, variable: str, description: str, discovered: dict[str, int]) -> None:
    """Write ``discovered`` as the snapshot module at ``path``."""
    lines = [f'"""{description}"""', "", f"{variable}: dict[str, int] = {{"]
    lines.extend(f"    {site!r}: {count}," for site, count in sorted(discovered.items()))
    lines.extend(["}", ""])
    path.write_text("\n".join(lines))
