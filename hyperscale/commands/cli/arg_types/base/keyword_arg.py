import inspect
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
from hyperscale.commands.cli.arg_types.data_types.reduce_pattern_type import reduce_pattern_type
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


KeywordArgType = Literal["keyword", "flag"]


T = TypeVar("T")


class KeywordArg(Generic[T]):
    def __init__(
        self,
        name: str,
        data_type: type[T],
        short_name: str | None = None,
        default: T | None = None,
        group: str | None = None,
        required: bool = True,
        arg_type: KeywordArgType = "keyword",
        description: str | None = None,
        is_context_arg: bool = False,
    ):
        if short_name is None:
            short_name = name[:1]

        self.name = name
        self.short_name = short_name
        self.is_context_arg = is_context_arg

        full_flag = "-".join(name.split("_"))

        self.full_flag = f"--{full_flag}"
        self.short_flag = f"-{short_name}"

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

        else:
            self._value_type = tuple([data_type])

        self.loads_from_envar = (
            len([subtype for subtype in self._value_type if get_origin(subtype) == Env])
            > 0
        )

        self.required = required

        self.default_is_awaitable = False
        if inspect.isawaitable(default) or inspect.iscoroutinefunction(default):
            self.default_is_awaitable = True

        self.default_is_callable = False
        if inspect.isfunction(default) or inspect.ismethod(default):
            self.default_is_callable = True

        base_type = [data_type]
        if self._is_complex_type:
            base_type = reduce_pattern_type(data_type)

        self.default = default
        self.group = group
        self.arg_type: KeywordArgType = arg_type
        self._data_type = [
            subtype_type.__name__ 
            if hasattr(subtype_type, '__name__') 
            else subtype_type 
            for subtype_type in base_type
        ]

        self.description = description

        self.consumed_next_positions_count = 1 if arg_type == "keyword" else 0

    @property
    def data_type(self):
        return ", ".join(self._data_type)

    @property
    def data_type_list(self):
        return self._data_type

    @property
    def value_type(self):
        return (
            self._value_type
            if self._is_complex_type is False
            else tuple([get_origin(data_type) for data_type in self._value_type])
        )

    async def to_default(self):
        if self.default_is_awaitable:
            default_value = await self.default()

        elif self.default_is_callable:
            default_value = self.default()

        else:
            default_value = self.default

        for subtype in self._value_type:
            if complex_type_factory := self._complex_types.get(get_origin(subtype)):
                complex_type = complex_type_factory(self.name, subtype)

                complex_type.data = default_value

                return complex_type
            
        return default_value

    def to_flag(self):
        return f"--{self.name}"

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


def is_required_missing_keyword_arg(
    flag: str, keyword_arg: KeywordArg, keyword_args: dict[str, KeywordArg]
):
    if keyword_args.get(keyword_arg.name):
        return False

    if flag != keyword_arg.full_flag:
        return False

    if keyword_arg.arg_type == "flag":
        return False

    return keyword_arg.required


def is_defaultable(
    flag: str, keyword_arg: KeywordArg, keyword_args: dict[str, KeywordArg]
):
    if keyword_args.get(keyword_arg.name):
        return False

    if flag == keyword_arg.short_flag:
        return False

    if keyword_arg.arg_type == "flag":
        return False

    return keyword_arg.required is False and keyword_arg.loads_from_envar is False


def is_env_defaultable(
    flag: str, keyword_arg: KeywordArg, keyword_args: dict[str, KeywordArg]
):
    if keyword_args.get(keyword_arg.name):
        return False

    if flag == keyword_arg.short_flag:
        return False

    if keyword_arg.arg_type == "flag":
        return False

    return keyword_arg.loads_from_envar


def is_unsupported_keyword_arg(arg: str, keyword_args: dict[str, KeywordArg]):
    return (arg.startswith("--") or arg.startswith("-")) and arg.replace(
        "-", ""
    ) not in keyword_args
