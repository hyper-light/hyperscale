"""
Env.types_map parses every setting the way its declared type requires.

``load_env`` turns each environment variable's text into a value with the
parser ``types_map`` names. A boolean read with ``bool`` is True for any
non-empty text -- ``RESOURCE_GUARD_ENABLED=false`` enabled guards -- so
booleans must use ``parse_bool_envar``; a setting without an entry is
never read from the environment at all.
"""

import types
import typing

import pytest

from hyperscale.distributed.env.env import Env, parse_bool_envar

PARSER_FOR_TYPE = {bool: parse_bool_envar, int: int, float: float, str: str}


def _base_types(annotation: object) -> set[type]:
    """The plain types a setting may hold: ``Strict*`` wrappers removed,
    ``None`` dropped from optionals, a ``Literal`` taken as its values' type."""
    origin = typing.get_origin(annotation)
    if isinstance(annotation, types.UnionType) or origin is typing.Union:
        return {
            base_type
            for member in typing.get_args(annotation)
            if member is not type(None)
            for base_type in _base_types(member)
        }
    if origin is typing.Annotated:
        return _base_types(typing.get_args(annotation)[0])
    if origin is typing.Literal:
        return {type(value) for value in typing.get_args(annotation)}
    return {annotation}


@pytest.mark.parametrize("field_name", sorted(Env.model_fields))
def test_setting_is_parsed_as_its_declared_type(field_name: str) -> None:
    types_map = Env.types_map()
    allowed_parsers = [PARSER_FOR_TYPE[base_type] for base_type in _base_types(Env.model_fields[field_name].annotation)]

    assert field_name in types_map, f"{field_name} is never read from the environment"
    assert any(types_map[field_name] is parser for parser in allowed_parsers), (field_name, types_map[field_name])


@pytest.mark.parametrize("text", ["false", "False", "0", "no", "off", "N", " f "])
def test_false_text_disables_a_boolean(text: str) -> None:
    assert parse_bool_envar(text) is False


@pytest.mark.parametrize("text", ["true", "TRUE", "1", "yes", "on", "Y", " t "])
def test_true_text_enables_a_boolean(text: str) -> None:
    assert parse_bool_envar(text) is True


@pytest.mark.parametrize("text", ["", "ture", "enabled", "2", "-1"])
def test_text_that_is_not_a_boolean_is_refused(text: str) -> None:
    # Read as False before: "ture" silently disabled the setting.
    with pytest.raises(ValueError, match="invalid boolean"):
        parse_bool_envar(text)
