from hyperscale.commands.cli.arg_types.data_types import (
    AssertSet,
    Context,
    Env,
    ImportFile,
    JsonData,
    JsonFile,
    Paths,
    Pattern,
    RawFile,
)
from hyperscale.commands.cli.arg_types.operators import Operator
from typing import (
    Literal,
    Generic,
    TypeVar,
    Any,
    Callable,
    get_args,
    get_origin,
)


T = TypeVar("T")


class PositionalArg(Generic[T]):
    def __init__(
        self,
        name: str,
        index: int,
        data_type: type[T],
        description: str | None = None,
        is_context_arg: bool = False,
    ):
        self.name = name
        self.index = index
        self.is_context_arg = is_context_arg

        self.is_envar: bool = False

        args = get_args(data_type)
        self._complex_types: dict[
            AssertSet
            | Context
            | Env
            | ImportFile
            | JsonData
            | JsonData
            | Operator
            | Paths
            | Pattern
            | RawFile,
            Callable[
                [str, type[Any]],
                AssertSet
                | Context
                | Env
                | ImportFile
                | JsonData
                | JsonData
                | Operator
                | Paths
                | Pattern
                | RawFile,
            ],
        ] = {
            AssertSet: lambda name, subtype: AssertSet(name, subtype),
            Context: lambda _, __: Context(),
            Env: lambda envar, subtype: Env(envar, subtype),
            ImportFile: lambda _, subtype: ImportFile(subtype),
            JsonFile: lambda _, subtype: JsonFile(subtype),
            JsonData: lambda _, subtype: JsonData(subtype),
            Operator: lambda name, subtype: Operator(name, subtype),
            Paths: lambda _, subtype: Paths(subtype),
            Pattern: lambda _, subtype: Pattern(subtype),
            RawFile: lambda _, subtype: RawFile(subtype),
        }

        self._is_complex_type = (
            get_origin(
                data_type,
            )
            in self._complex_types.keys()
        )

        if len(args) > 0 and self._is_complex_type is False:
            self._value_type = args

        elif self._is_complex_type:
            self._value_type = tuple([data_type])

        else:
            self._value_type = tuple([data_type])

        self.loads_from_envar = (
            len([subtype for subtype in self.value_type if get_origin(subtype) == Env])
            > 0
        )

        self._data_type = [subtype_type.__name__ for subtype_type in self.value_type]

        self.description = description

    @property
    def data_type(self):
        return ", ".join(self._data_type)

    @property
    def value_type(self):
        return (
            self._value_type
            if self._is_complex_type is False
            else tuple([get_origin(data_type) for data_type in self._value_type])
        )

    def to_help_string(self):
        help_string = f"{self.name}: [{self.data_type}]"

        if self.description:
            help_string = f"{help_string} {self.description}"

        return help_string

    async def parse(self, value: str | None = None):
        parse_error: Exception | None = None

        for subtype in self._value_type:
            try:
                if complex_type_factory := self._complex_types.get(get_origin(subtype)):
                    complex_type = complex_type_factory(self.name, subtype)

                    return await complex_type.parse(value)

                elif subtype == bytes:
                    return bytes(value, encoding="utf-8")

                return subtype(value)

            except Exception as e:
                parse_error = e

        return parse_error
