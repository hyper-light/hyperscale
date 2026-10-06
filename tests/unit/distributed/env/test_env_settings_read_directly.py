"""
Every Env setting is read as the Env field it is.

``getattr(env, "NAME", default)`` returns its literal default whenever the
Env has no field NAME, so a setting read that way under a name Env never
declared can never be configured: an operator's override is ignored
without a word, and the literal quietly becomes the behavior. Found this
way: manager loops reading names Env never had while the documented
settings for the same loops went unread, gate AD-34 timeout-tracking and
client-update-history settings that could not be set, and fallbacks that
were wrong-typed (a slots dataclass's class attribute is its member
descriptor, not the field's default).

Reading ``env.NAME`` fails loudly on a name the Env lacks. Checks the
packages that read the distributed and core-jobs Envs.
"""

import ast
import pathlib

import hyperscale.core.jobs
import hyperscale.distributed

SETTINGS_READING_ROOTS = (
    pathlib.Path(hyperscale.distributed.__file__).parent,
    pathlib.Path(hyperscale.core.jobs.__file__).parent,
)


def _receiver_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id

    if isinstance(node, ast.Attribute):
        return node.attr

    return ""


def _fallback_reads(path: pathlib.Path) -> list[str]:
    """Each ``getattr(<env>, <name>, <default>)`` in ``path``, as file:line: call."""
    tree = ast.parse(path.read_text(), filename=str(path))
    return [
        f"{path}:{node.lineno}: {ast.unparse(node)}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) == 3
        and "env" in _receiver_name(node.args[0]).lower()
    ]


def test_no_setting_is_read_with_a_fallback_default() -> None:
    fallback_reads = [
        fallback_read
        for root in SETTINGS_READING_ROOTS
        for path in sorted(root.rglob("*.py"))
        for fallback_read in _fallback_reads(path)
    ]

    assert fallback_reads == []


def test_the_check_finds_a_fallback_read(tmp_path: pathlib.Path) -> None:
    # The check itself: each spelling of the pattern is found.
    module = tmp_path / "module.py"
    module.write_text(
        "value = getattr(env, 'NAME', 1.0)\n"
        "other = getattr(self._env, \"OTHER\", None)\n"
        "dynamic = getattr(node_env, name, 0)\n"
        "required = getattr(env, 'NAME')\n"
        "unrelated = getattr(config, 'NAME', 1.0)\n"
    )

    assert [fallback_read.split(": ", 1)[1] for fallback_read in _fallback_reads(module)] == [
        "getattr(env, 'NAME', 1.0)",
        "getattr(self._env, 'OTHER', None)",
        "getattr(node_env, name, 0)",
    ]
