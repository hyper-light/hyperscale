"""
Ratchet lint — forbids reading ``self._x`` when ``_x`` is assigned
nowhere in the class's inheritance chain.

Why this is a crash lint
------------------------

A phantom attribute is a guaranteed ``AttributeError`` on the first
line that reads it, and Python gives no warning at import time. The
sibling duplicate-method lint made one half of this bug class extinct;
this is the other half, and it is the more common one:

``ManagerServer.state_sync_request`` referenced ``self._logger``,
``self._leadership_coordinator``, ``self._build_worker_snapshots`` and
``self._serialize_job_contexts`` — none of which exist in that class's
MRO. The handler raised on its FIRST statement, so peer state sync was
dead for every manager, and the three later phantoms were invisible
because execution never reached them. Fixing one attribute at a time
would have surfaced them one crash at a time.

``HealthAwareServer`` — base class of all three node types — carried
the same shape in the AD-35 confirmation callbacks: three
``self._logger`` sites plus a call to ``self._send_probe``, a method
that never existed (the real one is ``_send_probe_and_wait``). The
callback below it returned ``True`` unconditionally, so a working
version would have confirmed every peer it was asked about, including
dead ones.

Scope: private attributes only
------------------------------

The lint checks names matching ``_x`` — one leading underscore, not
dunder. That is deliberate, and it is what makes the analysis sound in
the presence of dynamic binding (below). Public attributes are the
ones frameworks bind at runtime; private ones are assigned in the
class body or they do not exist.

What counts as an assignment
----------------------------

``self._x = ...`` anywhere in the class body (including augmented,
annotated, tuple-unpacked, ``for`` and ``with`` targets), a
``__slots__`` entry, a class-level attribute or annotation, a method
or nested class of that name, ``setattr(self, "_x", ...)`` with a
literal name, or any of the same in an ancestor class.

Unprovable classes are skipped, not guessed
-------------------------------------------

A class is skipped when the lint cannot enumerate what it binds:

* a base class defined outside ``hyperscale/`` (a third-party or
  stdlib parent may bind anything), or
* a ``setattr(self, <non-literal>, ...)`` / ``self.__dict__`` in the
  class or an ancestor.

Skipping is the honest answer there — proving absence requires seeing
every binding site. The count is asserted below so the blind spot
cannot silently grow.

``PUBLIC_ONLY_DYNAMIC_BINDERS`` is the one exemption to the second
rule, and it needs a receipt per entry. ``MercurySyncBaseServer``
binds handlers with ``setattr(self, hook.name, hook)``, where the
hooks come from ``inspect.getmembers(self, predicate=is_hook)`` — they
are the class's own decorated methods, rebound to themselves under
their wire-handler names. That cannot introduce a private attribute,
so private-attribute analysis stays valid for it and for every node
class beneath it. Without this exemption the entire server hierarchy
— gate, manager, worker, ``HealthAwareServer`` — is unprovable, which
is exactly where the bugs were.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator

from tests.simulation.lints.expected_phantom_attribute_violations import (
    EXPECTED_PHANTOM_ATTRIBUTE_VIOLATIONS,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale"

# Bases that introduce no instance attributes of their own, so a class
# inheriting only these is still fully provable.
INERT_BASES: frozenset[str] = frozenset(
    {"object", "Generic", "Protocol", "ABC", "Struct"}
)

# Classes whose dynamic ``setattr`` provably binds only PUBLIC names.
# See the module docstring for the receipt behind each entry.
PUBLIC_ONLY_DYNAMIC_BINDERS: frozenset[str] = frozenset(
    {"MercurySyncBaseServer"}
)

# The lint's blind spot: classes with a base outside `hyperscale/` or
# an unenumerable dynamic bind. Asserted so it cannot grow unnoticed.
MAX_UNPROVABLE_CLASSES = 610


class ClassFacts:
    """What one class body binds, defines, and reads off ``self``."""

    __slots__ = ("name", "bases", "path", "assigned", "defined", "reads", "dynamic")

    def __init__(self, name: str, bases: list[str], path: Path) -> None:
        self.name = name
        self.bases = bases
        self.path = path
        self.assigned: set[str] = set()
        self.defined: set[str] = set()
        self.reads: list[tuple[str, int]] = []
        self.dynamic = False


def _iter_python_files(root: Path) -> Iterator[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def _base_name(node: ast.expr) -> str | None:
    """The trailing name of a base expression: ``a.b.C`` -> ``C``,
    ``Generic[T]`` -> ``Generic``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _base_name(node.value)
    return None


def _own_nodes(class_node: ast.ClassDef) -> Iterator[ast.AST]:
    """Every node in the class body EXCEPT nested class bodies, whose
    ``self`` refers to a different object entirely."""
    for statement in class_node.body:
        if isinstance(statement, ast.ClassDef):
            continue
        yield from ast.walk(statement)


def _record_assignment_target(target: ast.expr, facts: ClassFacts) -> None:
    if isinstance(target, ast.Attribute):
        if isinstance(target.value, ast.Name) and target.value.id == "self":
            facts.assigned.add(target.attr)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for element in target.elts:
            _record_assignment_target(element, facts)
    elif isinstance(target, ast.Starred):
        _record_assignment_target(target.value, facts)


def _binds_unenumerably(node: ast.AST, facts: ClassFacts) -> bool:
    """True when the node binds an attribute the lint cannot name.

    A string-literal ``setattr`` IS enumerable, so it is recorded as an
    ordinary assignment instead of blinding the class.
    """
    if isinstance(node, ast.Call):
        callee = node.func
        is_setattr = (isinstance(callee, ast.Name) and callee.id == "setattr") or (
            isinstance(callee, ast.Attribute) and callee.attr == "__setattr__"
        )
        if is_setattr and len(node.args) >= 2:
            target, name_node = node.args[0], node.args[1]
            if isinstance(target, ast.Name) and target.id == "self":
                if isinstance(name_node, ast.Constant) and isinstance(
                    name_node.value, str
                ):
                    facts.assigned.add(name_node.value)
                    return False
                return True

    if isinstance(node, ast.Attribute) and node.attr == "__dict__":
        return isinstance(node.value, ast.Name) and node.value.id == "self"

    return False


def _class_level_names(class_node: ast.ClassDef, facts: ClassFacts) -> None:
    """Methods, nested classes, class attributes, and ``__slots__``."""
    for statement in class_node.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            facts.defined.add(statement.name)
        elif isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            facts.defined.add(statement.target.id)
        elif isinstance(statement, ast.Assign):
            for target in statement.targets:
                if not isinstance(target, ast.Name):
                    continue
                facts.defined.add(target.id)
                if target.id != "__slots__":
                    continue
                for element in ast.walk(statement.value):
                    if isinstance(element, ast.Constant) and isinstance(
                        element.value, str
                    ):
                        facts.assigned.add(element.value)


def _analyze_class(class_node: ast.ClassDef, path: Path) -> ClassFacts:
    facts = ClassFacts(
        class_node.name,
        [name for name in map(_base_name, class_node.bases) if name],
        path,
    )
    _class_level_names(class_node, facts)

    exempt = class_node.name in PUBLIC_ONLY_DYNAMIC_BINDERS
    for node in _own_nodes(class_node):
        if _binds_unenumerably(node, facts) and not exempt:
            facts.dynamic = True

        if isinstance(node, ast.Assign):
            for target in node.targets:
                _record_assignment_target(target, facts)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            _record_assignment_target(node.target, facts)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            _record_assignment_target(node.target, facts)
        elif isinstance(node, ast.withitem) and node.optional_vars is not None:
            _record_assignment_target(node.optional_vars, facts)
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            is_self_read = (
                isinstance(node.value, ast.Name) and node.value.id == "self"
            )
            is_private = node.attr.startswith("_") and not node.attr.startswith("__")
            if is_self_read and is_private:
                facts.reads.append((node.attr, node.lineno))

    return facts


def _index_classes() -> dict[str, list[ClassFacts]]:
    classes: dict[str, list[ClassFacts]] = {}
    for path in _iter_python_files(PRODUCTION_ROOT):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                facts = _analyze_class(node, path)
                classes.setdefault(facts.name, []).append(facts)
    return classes


def _resolve(
    name: str, classes: dict[str, list[ClassFacts]], seen: set[str]
) -> tuple[set[str], bool]:
    """Every name bound by ``name`` and its ancestors, plus whether the
    chain was fully resolvable."""
    if name in INERT_BASES or name in seen:
        return set(), True

    seen.add(name)
    entries = classes.get(name)
    if entries is None:
        return set(), False

    names: set[str] = set()
    resolved = True
    for facts in entries:
        names |= facts.assigned | facts.defined
        if facts.dynamic:
            resolved = False
        for base in facts.bases:
            base_names, base_resolved = _resolve(base, classes, seen)
            names |= base_names
            resolved = resolved and base_resolved
    return names, resolved


def _discover_violations() -> tuple[set[str], int]:
    classes = _index_classes()
    violations: set[str] = set()
    unprovable = 0

    for name, entries in classes.items():
        known, resolved = _resolve(name, classes, set())
        if not resolved:
            unprovable += len(entries)
            continue
        for facts in entries:
            for attribute, lineno in facts.reads:
                if attribute in known:
                    continue
                relative_path = facts.path.relative_to(REPO_ROOT).as_posix()
                violations.add(f"{relative_path}::{name}.{attribute}:{lineno}")

    return violations, unprovable


def test_no_phantom_attributes() -> None:
    """Discovered phantom reads must equal the recorded snapshot."""
    discovered, unprovable = _discover_violations()
    expected = set(EXPECTED_PHANTOM_ATTRIBUTE_VIOLATIONS)

    newly_introduced = sorted(discovered - expected)
    assert not newly_introduced, (
        "Read(s) of an attribute assigned nowhere in the class's "
        "inheritance chain — each is a guaranteed AttributeError the "
        "first time that line executes:\n  " + "\n  ".join(newly_introduced)
    )

    stale_entries = sorted(expected - discovered)
    assert not stale_entries, (
        "Snapshot lists phantom attributes that no longer exist — remove "
        "them from expected_phantom_attribute_violations.py so the ratchet "
        "keeps its teeth:\n  " + "\n  ".join(stale_entries)
    )

    assert unprovable <= MAX_UNPROVABLE_CLASSES, (
        f"{unprovable} classes are unprovable (was {MAX_UNPROVABLE_CLASSES}) — "
        "the lint's blind spot grew. A class becomes unprovable through a "
        "base defined outside hyperscale/ or an unenumerable setattr; "
        "either shrink it back or raise the bound deliberately."
    )
