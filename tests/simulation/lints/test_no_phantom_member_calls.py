"""
Ratchet lint -- forbids calling ``self._x.member(...)`` when ``_x`` is
provably an instance of a ``hyperscale`` class whose inheritance chain binds
no ``member``.

The sibling phantom-attribute lint proves ``self._x`` exists; it cannot see
one dot further. ``HealthAwareServer._check_stale_unconfirmed_peers`` called
``self._metrics.record_counter(...)`` -- ``Metrics`` has ``increment``, never
``record_counter`` -- so every cleanup pass that found a stale peer raised
inside its error context, and the metric it meant to record never existed.

What makes ``_x``'s class provable
----------------------------------

Every binding of ``self._x`` in the class says the same class: each
``self._x = SomeClass(...)``, each ``self._x: SomeClass`` annotation, and
any class-level ``_x: SomeClass``. A binding the lint cannot type (a
parameter, a call to a function, a conditional expression) makes ``_x``
unknown, and unknown is skipped, never guessed. ``SomeClass`` must then be
inside ``hyperscale/`` all the way up, with no ``__getattr__`` -- except
one in a ``DECLARED_FIELD_BINDERS`` class (``Message``), which resolves
only the class's declared dataclass fields, names this lint already sees.

Unlike the sibling lint, a ``setattr(self, <name>, ...)`` does not blind the
class here: what is called is a method or a callable the class names, and
the dynamic binds in this tree rebind data fields that already exist
(``Metrics.increment``) or hooks under public wire names no internal caller
reaches through an attribute. A call the lint flags wrongly goes in the
snapshot with its receipt.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.simulation.lints.expected_phantom_member_call_violations import (
    EXPECTED_PHANTOM_MEMBER_CALL_VIOLATIONS,
)
from tests.simulation.lints.test_no_phantom_attributes import (
    DECLARED_FIELD_BINDERS,
    INERT_BASES,
    PRODUCTION_ROOT,
    REPO_ROOT,
    ClassFacts,
    _index_classes,
    _iter_python_files,
    _own_nodes,
)

# A value that types no binding: the attribute is unknown.
UNTYPED = ""


def _bound_class_name(value: ast.expr | None) -> str:
    """The class a binding's value is an instance of, or ``UNTYPED``."""
    if isinstance(value, ast.Call):
        callee = value.func
        if isinstance(callee, ast.Name):
            return callee.id
        if isinstance(callee, ast.Attribute) and ast.unparse(callee).split(".", maxsplit=1)[0] == "hyperscale":
            return callee.attr
    return UNTYPED


def _annotated_class_name(annotation: ast.expr | None) -> str:
    """The class an annotation names -- ``C``, ``"C"``, ``C | None`` --
    or ``UNTYPED``."""
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        annotation = ast.parse(annotation.value, mode="eval").body if annotation.value else None
    if isinstance(annotation, ast.Name):
        return annotation.id
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        named = [
            side
            for side in (annotation.left, annotation.right)
            if not (isinstance(side, ast.Constant) and side.value is None)
        ]
        return _annotated_class_name(named[0]) if len(named) == 1 else UNTYPED
    return UNTYPED


def _members(class_name: str, classes: dict[str, list[ClassFacts]]) -> set[str] | None:
    """Every name ``class_name``'s chain binds, or None when a base lies
    outside ``hyperscale/`` or the chain resolves names on demand (a
    ``DECLARED_FIELD_BINDERS`` class's ``__getattr__`` resolves only its
    declared fields, so it does not count)."""
    members: set[str] = set()
    resolves_on_demand = False
    pending = [class_name]
    seen: set[str] = set()
    while pending:
        name = pending.pop()
        if name in INERT_BASES or name in seen:
            continue
        seen.add(name)
        if (entries := classes.get(name)) is None:
            return None
        for facts in entries:
            members |= facts.assigned | facts.defined
            resolves_on_demand = resolves_on_demand or (
                name not in DECLARED_FIELD_BINDERS
                and bool({"__getattr__", "__getattribute__"} & facts.defined)
            )
            pending.extend(facts.bases)
    return None if resolves_on_demand else members


def _attribute_types_and_member_reads(
    class_node: ast.ClassDef,
) -> tuple[dict[str, set[str]], list[tuple[str, str, int]]]:
    """Each private attribute's bound class names, and every
    ``self._x.member(...)`` call as (attribute, member, line)."""
    bound_types: dict[str, set[str]] = {}
    member_reads: list[tuple[str, str, int]] = []
    # Constructor injection: ``self._x = x`` types ``_x`` by ``x``'s
    # annotation in ``__init__``.
    constructor_parameters: dict[str, str] = {
        argument.arg: _annotated_class_name(argument.annotation)
        for statement in class_node.body
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)) and statement.name == "__init__"
        for argument in (*statement.args.args, *statement.args.kwonlyargs)
    }
    for statement in class_node.body:
        if (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and statement.target.id.startswith("_")
        ):
            bound_types.setdefault(statement.target.id, set()).add(_annotated_class_name(statement.annotation))

    for node in _own_nodes(class_node):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                    bound_types.setdefault(target.attr, set()).add(
                        constructor_parameters.get(node.value.id, UNTYPED)
                        if isinstance(node.value, ast.Name)
                        else _bound_class_name(node.value)
                    )
                elif isinstance(target, (ast.Tuple, ast.List)):
                    for element in ast.walk(target):
                        if (
                            isinstance(element, ast.Attribute)
                            and isinstance(element.value, ast.Name)
                            and element.value.id == "self"
                        ):
                            bound_types.setdefault(element.attr, set()).add(UNTYPED)
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                annotated = _annotated_class_name(node.annotation)
                # An annotated binding is the annotation's class unless it
                # constructs one -- then both must agree.
                bound = _bound_class_name(node.value) if isinstance(node.value, ast.Call) else annotated
                bound_types.setdefault(target.attr, set()).update({annotated, bound})
        elif isinstance(node, (ast.AugAssign, ast.For, ast.AsyncFor, ast.withitem)):
            targets = (
                [node.optional_vars]
                if isinstance(node, ast.withitem)
                else [node.target]
            )
            for target in targets:
                for element in ast.walk(target) if target is not None else ():
                    if isinstance(element, ast.Attribute) and isinstance(element.value, ast.Name) and element.value.id == "self":
                        bound_types.setdefault(element.attr, set()).add(UNTYPED)
        elif isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == "setattr" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Name) and first.id == "self" and len(node.args) >= 2:
                    name_node = node.args[1]
                    if isinstance(name_node, ast.Constant) and isinstance(name_node.value, str):
                        bound_types.setdefault(name_node.value, set()).add(UNTYPED)
        if isinstance(node, ast.Call) and isinstance(called := node.func, ast.Attribute):
            owner = called.value
            if (
                isinstance(owner, ast.Attribute)
                and isinstance(owner.value, ast.Name)
                and owner.value.id == "self"
                and owner.attr.startswith("_")
                and not owner.attr.startswith("__")
                and not called.attr.startswith("__")
            ):
                member_reads.append((owner.attr, called.attr, called.lineno))
    return bound_types, member_reads


def _discover_violations() -> set[str]:
    classes = _index_classes()
    violations: set[str] = set()
    for path in _iter_python_files(PRODUCTION_ROOT):
        tree = ast.parse(path.read_text())
        # A name this file imports from outside ``hyperscale`` is that
        # module's class, even where a same-named fallback is defined here
        # for when the import fails.
        external_names = {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.level == 0
            and not (node.module or "").startswith("hyperscale")
            for alias in node.names
        }
        for class_node in ast.walk(tree):
            if not isinstance(class_node, ast.ClassDef):
                continue
            bound_types, member_reads = _attribute_types_and_member_reads(class_node)
            for attribute, member, lineno in member_reads:
                types = bound_types.get(attribute, {UNTYPED})
                if len(types) != 1 or UNTYPED in types:
                    continue
                (class_name,) = types
                if class_name not in classes or class_name in external_names:
                    continue
                if (members := _members(class_name, classes)) is None or member in members:
                    continue
                relative_path = Path(path).relative_to(REPO_ROOT).as_posix()
                violations.add(f"{relative_path}::{class_node.name}.{attribute}.{member}:{lineno}")
    return violations


def test_no_phantom_member_calls() -> None:
    """Discovered phantom member calls must equal the recorded snapshot."""
    discovered = _discover_violations()
    expected = set(EXPECTED_PHANTOM_MEMBER_CALL_VIOLATIONS)

    newly_introduced = sorted(discovered - expected)
    assert not newly_introduced, (
        "Read(s) of a member that the attribute's class binds nowhere in its "
        "inheritance chain -- each is a guaranteed AttributeError the first "
        "time that line executes:\n  " + "\n  ".join(newly_introduced)
    )

    stale_entries = sorted(expected - discovered)
    assert not stale_entries, (
        "Snapshot lists phantom member reads that no longer exist -- remove "
        "them from expected_phantom_member_call_violations.py so the ratchet "
        "keeps its teeth:\n  " + "\n  ".join(stale_entries)
    )
