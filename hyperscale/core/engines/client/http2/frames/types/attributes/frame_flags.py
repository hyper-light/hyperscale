# -*- coding: utf-8 -*-
"""
hyperframe/flags
~~~~~~~~~~~~~~~~

Defines basic Flag and Flags data structures.
"""

from typing import NamedTuple, Iterable


class Flag(NamedTuple):
    name: str
    bit: int


class Flags(set):
    __slots__ = ()

    """
    The names of the flags set on a frame. A plain set, so testing a flag is
    a C-level lookup: frames are built and tested for every request.

    ``defined_flags`` is accepted for compatibility; flags are not validated
    against it.
    """

    def __init__(self, defined_flags: Iterable[Flag] = ()):
        super().__init__()

    def __repr__(self) -> str:
        return repr(sorted(self))
