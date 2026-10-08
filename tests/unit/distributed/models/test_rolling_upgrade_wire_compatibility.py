"""
Rolling upgrades (AD-25): every wire message reads across one version step.

A cluster mid-upgrade runs two builds side by side, so every message a node
receives may come from a sender one version older or newer. For each of the
wire messages (every ``Message`` dataclass in the wire namespace
``hyperscale.distributed.models.distributed``), the sender's build is played
by a variant class registered under the message's exact wire name, and the
receiver loads the bytes through the real ``Message.load``:

* a NEWER receiver reading an OLDER sender (one defaulted field the sender
  did not have yet): the missing field reads as its default, every other
  field arrives intact;
* an OLDER receiver reading a NEWER sender (a field it does not know): the
  message loads and every field it knows arrives intact;
* a field added WITHOUT a default (a breaking change) is never silently
  invented: reading it raises, naming the field.

A message that defines its own ``__getstate__``/``__setstate__`` (its own
by-name state format) is sent by a variant that uses the same by-name format,
as its other builds do; its ``__setstate__`` decides its own policy for a
missing field, so it is excepted from the never-invented check.

Field values are synthesized from each field's annotation; a message whose
required fields cannot be synthesized is counted, and the coverage floor
below keeps that count from growing unnoticed.
"""

from __future__ import annotations

import dataclasses
import enum
import inspect
import types
import typing
from collections.abc import Iterator

import pytest

import hyperscale.distributed.models.distributed as wire_namespace
from hyperscale.distributed.models.message import Message

ADDED_FIELD_NAME = "field_added_by_a_newer_build"
ADDED_FIELD_VALUE = 7
# Every wire message but those with a required field of a type the
# synthesizer below does not build (nested messages, protocols, callables).
MINIMUM_COVERED_MESSAGES = 90
SYNTHESIZED_SCALARS: dict[type, object] = {
    str: "wire-value",
    int: 3,
    float: 2.5,
    bool: True,
    bytes: b"wire-bytes",
}
EMPTY_CONTAINERS: dict[object, object] = {
    list: [],
    dict: {},
    set: set(),
    frozenset: frozenset(),
    tuple: (),
}


class UnsynthesizableField(Exception):
    """A required field whose annotation the synthesizer does not build."""


def wire_messages() -> list[type[Message]]:
    return sorted(
        (
            candidate
            for candidate in vars(wire_namespace).values()
            if inspect.isclass(candidate)
            and issubclass(candidate, Message)
            and candidate is not Message
            and dataclasses.is_dataclass(candidate)
        ),
        key=lambda message_class: message_class.__qualname__,
    )


def has_default(message_field: dataclasses.Field) -> bool:
    return message_field.default is not dataclasses.MISSING or message_field.default_factory is not dataclasses.MISSING


def synthesize(annotation: object) -> object:
    """A value for a required field of type ``annotation``."""
    origin = typing.get_origin(annotation)
    arguments = typing.get_args(annotation)
    if annotation in SYNTHESIZED_SCALARS:
        return SYNTHESIZED_SCALARS[annotation]
    if origin in (typing.Union, types.UnionType):
        if type(None) in arguments:
            return None
        return synthesize(arguments[0])
    if origin is typing.Literal:
        return arguments[0]
    if (origin or annotation) in EMPTY_CONTAINERS:
        return type(EMPTY_CONTAINERS[origin or annotation])()
    if inspect.isclass(annotation) and issubclass(annotation, enum.Enum):
        return next(iter(annotation))
    raise UnsynthesizableField(repr(annotation))


def field_annotations(message_class: type[Message]) -> dict[str, object]:
    """Each field's annotation, resolved where its names are importable at
    runtime and left as written otherwise (names imported only for type
    checking)."""
    try:
        return typing.get_type_hints(message_class)
    except NameError:
        return {message_field.name: message_field.type for message_field in dataclasses.fields(message_class)}


def defines_own_state(message_class: type[Message]) -> bool:
    return "__setstate__" in vars(message_class)


def by_name_state(message: Message) -> dict[str, object]:
    """The by-name state format of messages that define their own state."""
    return {
        **{message_field.name: getattr(message, message_field.name) for message_field in dataclasses.fields(message)},
        "message_id": message._message_id,
        "sender_incarnation": message._sender_incarnation,
    }


def build_instance(message_class: type[Message]) -> Message:
    hints = field_annotations(message_class)
    arguments = {
        message_field.name: synthesize(hints[message_field.name])
        for message_field in dataclasses.fields(message_class)
        if not has_default(message_field)
    }
    return message_class(**arguments)


def variant_of(
    message_class: type[Message],
    kept_fields: list[dataclasses.Field],
    extra_field: tuple[str, type, dataclasses.Field] | None,
) -> type[Message]:
    """The message as another build defines it: ``kept_fields`` (and
    ``extra_field``), registered under the message's exact wire name."""
    hints = field_annotations(message_class)
    specification = [
        (message_field.name, hints[message_field.name], dataclasses.field(
            default=message_field.default,
            default_factory=message_field.default_factory,
        ))
        if has_default(message_field)
        else (message_field.name, hints[message_field.name])
        for message_field in kept_fields
    ]
    if extra_field is not None:
        specification.append(extra_field)
    variant = dataclasses.make_dataclass(
        message_class.__name__,
        specification,
        bases=(Message,),
        slots=True,
        kw_only=message_class.__dataclass_params__.kw_only,
    )
    variant.__module__ = message_class.__module__
    variant.__qualname__ = message_class.__qualname__
    if defines_own_state(message_class):
        variant.__getstate__ = by_name_state
    return variant


def sent_by(variant: type[Message], message_class: type[Message], values: dict[str, object]) -> bytes:
    """``values`` serialized by a sender whose build defines the message as
    ``variant``, then the receiver's own definition restored."""
    setattr(wire_namespace, message_class.__name__, variant)
    try:
        return variant(**values).dump()
    finally:
        setattr(wire_namespace, message_class.__name__, message_class)


def covered_messages() -> Iterator[tuple[type[Message], Message]]:
    for message_class in wire_messages():
        try:
            yield message_class, build_instance(message_class)
        except UnsynthesizableField:
            continue


def field_values(message: Message, names: list[str]) -> dict[str, object]:
    return {name: getattr(message, name) for name in names}


def test_enough_wire_messages_are_covered() -> None:
    covered = sum(1 for _ in covered_messages())
    assert covered >= MINIMUM_COVERED_MESSAGES, f"{covered} of {len(wire_messages())} wire messages covered"


@pytest.mark.parametrize("message_class", wire_messages(), ids=lambda message_class: message_class.__name__)
def test_a_newer_receiver_reads_an_older_senders_message(message_class: type[Message]) -> None:
    try:
        instance = build_instance(message_class)
    except UnsynthesizableField as unsynthesizable:
        pytest.skip(f"required field not synthesized: {unsynthesizable}")
    fields = dataclasses.fields(message_class)
    defaulted = [message_field for message_field in fields if has_default(message_field)]
    if not defaulted:
        pytest.skip("no defaulted field: no field could have been added compatibly")
    added_later = defaulted[-1]
    older_fields = [message_field for message_field in fields if message_field is not added_later]
    older_values = field_values(instance, [message_field.name for message_field in older_fields])
    wire_bytes = sent_by(variant_of(message_class, older_fields, None), message_class, older_values)

    received = message_class.load(wire_bytes)

    assert type(received) is message_class
    assert field_values(received, list(older_values)) == older_values
    expected_default = (
        added_later.default_factory()
        if added_later.default_factory is not dataclasses.MISSING
        else added_later.default
    )
    assert getattr(received, added_later.name) == expected_default


@pytest.mark.parametrize("message_class", wire_messages(), ids=lambda message_class: message_class.__name__)
def test_an_older_receiver_reads_a_newer_senders_message(message_class: type[Message]) -> None:
    try:
        instance = build_instance(message_class)
    except UnsynthesizableField as unsynthesizable:
        pytest.skip(f"required field not synthesized: {unsynthesizable}")
    fields = dataclasses.fields(message_class)
    known_values = field_values(instance, [message_field.name for message_field in fields])
    newer = variant_of(
        message_class,
        list(fields),
        (ADDED_FIELD_NAME, int, dataclasses.field(default=ADDED_FIELD_VALUE)),
    )
    wire_bytes = sent_by(newer, message_class, {**known_values, ADDED_FIELD_NAME: ADDED_FIELD_VALUE})

    received = message_class.load(wire_bytes)

    assert type(received) is message_class
    assert field_values(received, list(known_values)) == known_values


@pytest.mark.parametrize("message_class", wire_messages(), ids=lambda message_class: message_class.__name__)
def test_a_required_field_the_sender_lacked_is_never_invented(message_class: type[Message]) -> None:
    try:
        instance = build_instance(message_class)
    except UnsynthesizableField as unsynthesizable:
        pytest.skip(f"required field not synthesized: {unsynthesizable}")
    if defines_own_state(message_class):
        pytest.skip("its own __setstate__ decides how a missing field reads")
    fields = dataclasses.fields(message_class)
    required = [message_field for message_field in fields if not has_default(message_field)]
    if not required:
        pytest.skip("no required field")
    missing = required[-1]
    older_fields = [message_field for message_field in fields if message_field is not missing]
    older_values = field_values(instance, [message_field.name for message_field in older_fields])
    wire_bytes = sent_by(variant_of(message_class, older_fields, None), message_class, older_values)

    received = message_class.load(wire_bytes)

    with pytest.raises(AttributeError, match=missing.name):
        getattr(received, missing.name)
