"""Adversarial VOPR over ``RestrictedUnpickler`` — the deserialization
gate every cross-node message (and every submitted workflow / context)
passes through before its bytes become live Python objects.

The gate's one job is total: a payload may only ever reconstruct classes
and functions from the allowlist (``hyperscale.*`` plus a vetted stdlib
and dependency set), and must refuse everything else by raising
``SecurityError`` -- *before* the pickle machine runs any ``REDUCE`` /
``INST`` / ``BUILD`` that could execute code. These tests drive the real
``find_class`` through every opcode that reaches it and assert that:

* INV1 -- a blocked module or builtin is refused no matter which opcode
  names it (protocol-0 ``GLOBAL``, protocol-4 ``STACK_GLOBAL``, and the
  ``INST`` instantiation path);
* INV2 -- a dotted ``name`` cannot walk attributes out of a vetted module
  into a sibling it merely imported (``logging`` -> ``os.system``); this
  is the real escape the allowlist's string-pair check missed, since
  ``pickle`` protocol >= 4 resolves dotted names by attribute traversal;
* INV3 -- feeding seeded byte mutations (and truncations, oversize/zero
  prefixes, trailing garbage) of *valid* message pickles never yields an
  object drawn from a blocked module, never executes, never hangs, and is
  deterministic per seed.

Determinism is asserted the strong way: a seed produces identical mutated
bytes and the identical outcome on every run; INV3 mixes known-adversarial
payloads into the fuzz batch so the "no blocked global ever resolves"
assertion has teeth against a weakened allowlist.
"""

from __future__ import annotations

import pickle
import time
from typing import List, Tuple

import cloudpickle
import pytest

from hyperscale.distributed.models.restricted_unpickler import (
    BLOCKED_MODULES,
    RestrictedUnpickler,
    restricted_loads,
)
from hyperscale.distributed.models.security_error import SecurityError
from tests.simulation.harness.sim import SeededRandom


PICKLE_PROTOCOL_4_HEADER = b"\x80\x04"
PICKLE_STOP = pickle.STOP

# Blocked targets that MUST be refused however an opcode names them. Each is
# either a blocked module, a blocked (module, class) builtin, or a module that
# is simply absent from the allowlist. All must raise SecurityError at
# find_class time, before any instantiation runs.
BLOCKED_GLOBAL_TARGETS: Tuple[Tuple[str, str], ...] = (
    ("os", "system"),
    ("os", "popen"),
    ("subprocess", "Popen"),
    ("subprocess", "run"),
    ("builtins", "eval"),
    ("builtins", "exec"),
    ("builtins", "compile"),
    ("builtins", "open"),
    ("builtins", "__import__"),
    ("builtins", "breakpoint"),
    ("io", "open"),
    ("_io", "FileIO"),
    ("importlib", "import_module"),
    ("pathlib", "Path"),
    ("ctypes", "CDLL"),
    ("threading", "Thread"),
    ("multiprocessing", "Process"),
    ("inspect", "currentframe"),
    ("sys", "exit"),
    ("posix", "system"),
    ("nt", "system"),
    ("webbrowser", "open"),
    ("_frozen_importlib", "__import__"),
)

# Dotted names that attempt to walk out of a vetted module into a blocked one
# via attribute traversal. ``module`` is on the allowlist; ``name`` steps
# through an imported sibling module. Every one must raise SecurityError.
DOTTED_ESCAPE_TARGETS: Tuple[Tuple[str, str], ...] = (
    ("logging", "os.system"),
    ("logging", "os.popen"),
    ("logging", "os.environ"),
    ("logging", "os.remove"),
    ("logging", "sys.modules"),
    ("json", "codecs.open"),
    ("collections", "_sys.modules"),
    ("asyncio", "subprocess.Process"),
)


def _global_opcode(module: str, name: str) -> bytes:
    """Protocol-0 ``GLOBAL`` opcode: ``c<module>\\n<name>\\n``."""
    return b"c" + module.encode() + b"\n" + name.encode() + b"\n"


def _stack_global_opcode(module: str, name: str) -> bytes:
    """Protocol-4 ``STACK_GLOBAL``: push two unicode strings, then resolve.

    This is the opcode whose dotted-name attribute traversal is the escape
    INV2 guards against.
    """
    module_bytes = module.encode()
    name_bytes = name.encode()
    return (
        pickle.SHORT_BINUNICODE + bytes([len(module_bytes)]) + module_bytes
        + pickle.SHORT_BINUNICODE + bytes([len(name_bytes)]) + name_bytes
        + pickle.STACK_GLOBAL
    )


def _inst_opcode(module: str, name: str) -> bytes:
    """Protocol-0 ``INST``: ``(i<module>\\n<name>\\n`` resolves the global via
    find_class and then instantiates it -- a second code path into the gate."""
    return pickle.MARK + b"i" + module.encode() + b"\n" + name.encode() + b"\n"


def _resolved_module_is_blocked(resolved_object: object) -> bool:
    """True if a find_class result belongs to a module the gate forbids."""
    home_module = getattr(resolved_object, "__module__", None)
    if home_module is None:
        return False
    if home_module in BLOCKED_MODULES:
        return True
    return any(home_module.startswith(blocked + ".") for blocked in BLOCKED_MODULES)


class _RecordingUnpickler(RestrictedUnpickler):
    """Wraps the real gate to record every global it hands to the machine,
    so a successful load can be proven to have resolved only safe objects."""

    def find_class(self, module: str, name: str) -> object:
        resolved_object = super().find_class(module, name)
        self.resolved_globals.append((module, name, resolved_object))
        return resolved_object


def _recording_loads(data: bytes) -> Tuple[object, List[Tuple[str, str, object]]]:
    import io

    unpickler = _RecordingUnpickler(io.BytesIO(data))
    unpickler.resolved_globals = []
    loaded = unpickler.load()
    return loaded, unpickler.resolved_globals


@pytest.mark.parametrize("module,name", BLOCKED_GLOBAL_TARGETS)
def test_blocked_target_refused_through_global_opcode(module: str, name: str):
    """INV1 (GLOBAL): a blocked target named by the protocol-0 opcode is
    refused at the gate rather than resolved into a callable."""
    payload = PICKLE_PROTOCOL_4_HEADER + _global_opcode(module, name) + PICKLE_STOP
    with pytest.raises(SecurityError):
        restricted_loads(payload)


@pytest.mark.parametrize("module,name", BLOCKED_GLOBAL_TARGETS)
def test_blocked_target_refused_through_stack_global_opcode(module: str, name: str):
    """INV1 (STACK_GLOBAL): the same targets are refused through the
    protocol-4 opcode, including a trailing REDUCE that would execute them
    if find_class had returned the callable."""
    payload = (
        PICKLE_PROTOCOL_4_HEADER
        + _stack_global_opcode(module, name)
        + pickle.MARK
        + pickle.TUPLE
        + pickle.REDUCE
        + PICKLE_STOP
    )
    with pytest.raises(SecurityError):
        restricted_loads(payload)


@pytest.mark.parametrize("module,name", BLOCKED_GLOBAL_TARGETS)
def test_blocked_target_refused_through_inst_opcode(module: str, name: str):
    """INV1 (INST): the instantiation opcode routes through the same gate
    and is refused before any object is constructed."""
    payload = _inst_opcode(module, name) + PICKLE_STOP
    with pytest.raises(SecurityError):
        restricted_loads(payload)


@pytest.mark.parametrize("module,name", DOTTED_ESCAPE_TARGETS)
def test_dotted_name_cannot_escape_vetted_module(module: str, name: str):
    """INV2 (the real bug): a dotted ``name`` must not walk attributes out of
    a vetted module into a blocked sibling. ``pickle`` protocol >= 4 resolves
    dotted names by attribute traversal, so ``logging`` + ``os.system``
    resolves ``os.system`` unless the gate rejects the module crossing."""
    payload = PICKLE_PROTOCOL_4_HEADER + _stack_global_opcode(module, name) + PICKLE_STOP
    with pytest.raises(SecurityError):
        restricted_loads(payload)


def test_dotted_escape_would_resolve_dangerous_object_without_the_guard():
    """Proof the INV2 payload is a genuine escape and not a no-op: resolving
    the same dotted name directly on the stdlib ``logging`` module yields
    ``os.system`` -- exactly what the gate must prevent handing to a REDUCE."""
    import logging
    import os

    resolved_without_guard = logging
    for attribute_name in "os.system".split("."):
        resolved_without_guard = getattr(resolved_without_guard, attribute_name)
    # The traversal lands squarely on the real os.system callable -- the exact
    # object a REDUCE opcode would then invoke with attacker-supplied args.
    assert resolved_without_guard is os.system


def _valid_pickle_corpus() -> List[bytes]:
    """Valid payloads the gate must accept: a real submitted workflow pickled
    by value (cloudpickle internals + hyperscale globals) and a nested
    container of allowed stdlib types."""
    import collections
    import sys

    from hyperscale.distributed.testing.workflows import SimpleWorkflow

    cloudpickle.register_pickle_by_value(sys.modules[SimpleWorkflow.__module__])
    workflow_bytes = cloudpickle.dumps(SimpleWorkflow())

    container = {
        "numbers": [1, 2, 3, 4],
        "ordered": collections.OrderedDict(first=1, second=2),
        "counter": collections.Counter("abracadabra"),
        "deque": collections.deque([1, 2, 3]),
    }
    container_bytes = cloudpickle.dumps(container)
    return [workflow_bytes, container_bytes]


def _mutate_bytes(seeded_random: SeededRandom, original: bytes) -> bytes:
    """Deterministically flip/replace a seeded number of byte positions."""
    mutable = bytearray(original)
    mutation_count = seeded_random.randrange(1, max(2, len(mutable) // 4))
    for _ in range(mutation_count):
        position = seeded_random.randrange(0, len(mutable))
        mutable[position] = seeded_random.randrange(0, 256)
    return bytes(mutable)


def test_valid_corpus_loads_and_resolves_only_allowed_globals():
    """The gate must ACCEPT legitimate payloads and, in doing so, resolve
    only allowlisted globals -- confirming the fix did not over-block."""
    for original in _valid_pickle_corpus():
        loaded, resolved_globals = _recording_loads(original)
        assert loaded is not None
        assert resolved_globals, "a real pickle resolves at least one global"
        for module, name, resolved_object in resolved_globals:
            assert not _resolved_module_is_blocked(resolved_object), (
                f"accepted pickle resolved a blocked global: {module}.{name}"
            )


_FUZZ_SEEDS = range(200)


def test_random_mutation_never_resolves_blocked_global_and_is_deterministic():
    """INV3: seeded byte mutations of valid pickles (mixed with known escape
    payloads) either raise a bounded exception or load successfully having
    resolved ONLY allowlisted globals -- never a blocked one, never executing
    anything, never hanging. Identical seeds produce identical bytes and the
    identical outcome."""
    corpus = _valid_pickle_corpus()
    # Known-adversarial payloads folded into the fuzz loop: each must be
    # refused with SecurityError on every iteration, so weakening either the
    # allowlist or the dotted-name guard makes this test fail even though the
    # payloads otherwise look like ordinary bytes in the stream.
    adversarial = [
        PICKLE_PROTOCOL_4_HEADER + _global_opcode("os", "system") + PICKLE_STOP,
        PICKLE_PROTOCOL_4_HEADER + _stack_global_opcode("logging", "os.system") + PICKLE_STOP,
        PICKLE_PROTOCOL_4_HEADER + _stack_global_opcode("subprocess", "Popen") + PICKLE_STOP,
    ]
    for adversarial_payload in adversarial:
        with pytest.raises(SecurityError):
            restricted_loads(adversarial_payload)

    for seed in _FUZZ_SEEDS:
        base = corpus[seed % len(corpus)]
        mutated_first = _mutate_bytes(SeededRandom(seed), base)
        mutated_second = _mutate_bytes(SeededRandom(seed), base)
        assert mutated_first == mutated_second, "mutation must be deterministic per seed"

        truncation_point = SeededRandom(seed + 10_000).randrange(0, len(base) + 1)
        candidates = [
            mutated_first,
            base[:truncation_point],                       # truncation at a seeded boundary
            base + b"\x00" * 64,                            # trailing zero/garbage after a valid frame
            base + mutated_first,                          # interleaved: valid frame then junk
            PICKLE_PROTOCOL_4_HEADER + b"\x95" + (1 << 60).to_bytes(8, "little"),  # oversize FRAME length
            PICKLE_PROTOCOL_4_HEADER + b"\x95" + (0).to_bytes(8, "little") + PICKLE_STOP,  # zero FRAME length
        ]

        for candidate in candidates:
            started_at = time.perf_counter()
            try:
                loaded, resolved_globals = _recording_loads(candidate)
            except (Exception, RecursionError):
                # A clean, bounded refusal/parse failure is the acceptable
                # outcome for malformed input. The gate must never let the
                # payload execute, which it cannot do without first resolving
                # a blocked global -- and that path raises SecurityError.
                elapsed = time.perf_counter() - started_at
                assert elapsed < 5.0, "deserialization must not hang"
                continue
            elapsed = time.perf_counter() - started_at
            assert elapsed < 5.0, "deserialization must not hang"
            for module, name, resolved_object in resolved_globals:
                assert not _resolved_module_is_blocked(resolved_object), (
                    f"mutated payload resolved a blocked global: {module}.{name}"
                )


def test_fuzz_outcome_is_identical_across_two_runs():
    """INV3 (determinism, end to end): the whole fuzz batch yields byte-for-byte
    identical outcomes (same exception type or same success) on two runs of the
    same seeds -- the property a replay harness depends on."""
    corpus = _valid_pickle_corpus()

    def outcomes() -> List[str]:
        results: List[str] = []
        for seed in range(50):
            base = corpus[seed % len(corpus)]
            candidate = _mutate_bytes(SeededRandom(seed), base)
            try:
                restricted_loads(candidate)
                results.append("ok")
            except Exception as error:  # noqa: BLE001 - outcome identity, not handling
                results.append(type(error).__name__)
        return results

    assert outcomes() == outcomes()
