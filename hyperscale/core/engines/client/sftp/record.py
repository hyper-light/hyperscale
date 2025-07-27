from typing import Mapping, Type, cast


class _RecordMeta(type):
    """Metaclass for general-purpose record type"""

    __slots__: dict[str, object] = {}

    def __new__(mcs: Type['_RecordMeta'], name: str, bases: tuple[type, ...],
                ns: dict[str, object]) -> '_RecordMeta':
        cls = cast(_RecordMeta, super().__new__(mcs, name, bases, ns))

        if name != 'Record':
            fields = cast(Mapping[str, str], cls.__annotations__.keys())
            defaults = {k: ns.get(k) for k in fields}
            cls.__slots__ = defaults

        return cls


class Record(metaclass=_RecordMeta):
    """Generic Record class"""

    __slots__: Mapping[str, object] = {}

    def __init__(self, *args: object, **kwargs: object):
        for k, v in self.__slots__.items():
            setattr(self, k, v)

        for k, v in zip(self.__slots__, args):
            setattr(self, k, v)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self) -> str:
        values = ', '.join(f'{k}={getattr(self, k)!r}' for k in self.__slots__)

        return f'{type(self).__name__}({values})'

    def __str__(self) -> str:
        values = ((k, self._format(k, getattr(self, k)))
                  for k in self.__slots__)

        return ', '.join(f'{k}: {v}' for k, v in values if v is not None)

    def _format(self, k: str, v: object) -> str | None:
        """Format a field as a string"""

        # pylint: disable=no-self-use,unused-argument

        return str(v)