import inspect
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    TypeVar,
    get_args,
    get_origin,
)

from hyperscale.commands.cli.arg_types.data_types import (
    AssertPath,
    AssertSet,
    Context,
    Env,
    ImportInstance,
    ImportType,
    JsonData,
    JsonFile,
    Paths,
    Pattern,
    RawFile,
)
from hyperscale.commands.cli.arg_types.data_types.reduce_pattern_type import reduce_pattern_type
from hyperscale.commands.cli.arg_types.operators import Operator

KeywordArgType = Literal["keyword", "flag"]


T = TypeVar("T")


class KeywordArg(Generic[T]):
    complex_types: dict[
        AssertPath
        | AssertSet
        | Context
        | Env
        | ImportInstance
        | ImportType
        | JsonData
        | JsonData
        | Operator
        | Paths
        | Pattern
        | RawFile,
        Callable[
            [str, type[Any]],
            AssertPath
            | AssertSet
            | Context
            | Env
            | ImportInstance
            | ImportType
            | JsonData
            | JsonData
            | Operator
            | Paths
            | Pattern
            | RawFile,
        ],
    ] = {
        AssertPath: lambda _, __: AssertPath(),
        AssertSet: lambda name, subtype: AssertSet(name, subtype),
        Context: lambda _, __: Context(),
        Env: lambda envar, subtype: Env(envar, subtype),
        ImportInstance: lambda _, subtype: ImportInstance(subtype),
        ImportType: lambda _, subtype: ImportType(subtype),
        JsonFile: lambda _, subtype: JsonFile(subtype),
        JsonData: lambda _, subtype: JsonData(subtype),
        Operator: lambda name, subtype: Operator(name, subtype),
        Paths: lambda _, subtype: Paths(subtype),
        Pattern: lambda _, subtype: Pattern(subtype),
        RawFile: lambda _, subtype: RawFile(subtype),
    }


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
        is_multiarg: bool = False,
        is_context_arg: bool = False,
    ):
        if short_name is None:
            short_name = name[:1]

        self.name = name
        self.short_name = short_name
        self.is_context_arg = is_context_arg
        self.is_multiarg = is_multiarg

        full_flag = "-".join(name.split("_"))

        self.full_flag = f"--{full_flag}"
        self.short_flag = f"-{short_name}"

        args = get_args(data_type)

        self._is_complex_type = (
            get_origin(
                data_type,
            )
            in self.complex_types.keys()
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

        elif len(args) > 0:
            base_type = args

        self.default = default
        self.group = group
        self.arg_type: KeywordArgType = arg_type
        self._data_type = [
            subtype_type.__name__ if hasattr(subtype_type, "__name__") else subtype_type
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
            if (
                complex_type_factory := self.complex_types.get(get_origin(subtype))
            ) or (
                complex_type_factory := self.complex_types.get(subtype)
            ):
                complex_type = complex_type_factory(self.name, subtype)

                complex_type.data = default_value

                return complex_type

        return default_value

    def to_flag(self):
        return self.full_flag

    async def parse(self, value: str | None = None):
        parse_error: Exception | None = None

        if self.is_multiarg:
            value_types = []
            for subtype in self._value_type:
                if subtype in [
                    list,
                    set,
                ]:
                    value_types.extend(subtype)

                elif get_origin(subtype) in [
                    list,
                    set,
                ]:
                    value_types.extend(
                        get_args(subtype)
                    )

                elif subtype:
                    value_types.append(subtype)

            return await self._parse(value, value_types)

        return await self._parse(value, self._value_type)

    async def _parse(self, value: str | None, value_types: list[Any]):

        for subtype in value_types:
            try:
                if (
                    complex_type_factory := self.complex_types.get(get_origin(subtype))
                ) or (
                    complex_type_factory := self.complex_types.get(subtype)
                ):
                    complex_type = complex_type_factory(self.name, subtype)

                    return await complex_type.parse(value)

                elif subtype is bytes:
                    return bytes(value, encoding="utf-8")
                
                elif callable(subtype):
                    return subtype(value)
                
                else:
                    return value

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
