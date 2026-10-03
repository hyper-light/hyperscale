class AddressRotation:
    """
    The starting offsets of new connections into a host's addresses: 0, 1,
    2, ... Unlike ``itertools.count``, it pickles (Python 3.14 removed pickle
    support from itertools), so an engine holding one still ships with its
    workflow.
    """

    __slots__ = ("_next_offset",)

    def __init__(self) -> None:
        self._next_offset = 0

    def __iter__(self):
        return self

    def __next__(self) -> int:
        offset = self._next_offset
        self._next_offset += 1
        return offset
