import inspect
from .context import Context
from typing import Literal, Generic, TypeVar, get_args, get_origin
from .env import Env
from .json_file import JsonFile
from .pattern import Pattern
from .raw_file import RawFile


KeywordArgType = Literal[
    "keyword",
    "flag"
]


T = TypeVar('T')


class KeywordArg(Generic[T]):

    def __init__(
        self,
        name: str,
        data_type: type[T],
        short_name: str | None = None,
        default: T | None = None,
        group: str | None = None,
        required: bool = True,
        arg_type: KeywordArgType = 'keyword',
        description: str | None = None,
        is_context_arg: bool = False
    ):
        if short_name is None:
            short_name = name[:1]

        self.name = name
        self.short_name = short_name
        self.is_context_arg = is_context_arg

        full_flag = '-'.join(name.split('_'))

        self.full_flag = f'--{full_flag}'
        self.short_flag = f'-{short_name}'

        args = get_args(data_type)
        complex_types = [
            Pattern,
            Env,
            Context,
            RawFile,
            JsonFile,
        ]

        self._is_complex_type = get_origin(
            data_type,
        ) in complex_types

        if len(args) > 0 and self._is_complex_type is False:
            self._value_type = args

        elif self._is_complex_type:
            self._value_type = tuple([
                data_type
            ])

        else:
            self._value_type = tuple([data_type])

        
        self.loads_from_envar = len([
            subtype for subtype in self._value_type if get_origin(subtype) == Env
        ]) > 0

        self.required = required

        self.default_is_awaitable = False
        if inspect.isawaitable(default) or inspect.iscoroutinefunction(default):
            self.default_is_awaitable = True

        self.default_is_callable = False
        if inspect.isfunction(default) or inspect.ismethod(default):
            self.default_is_callable = True

        self.default = default
        self.group = group
        self.arg_type: KeywordArgType = arg_type
        self._data_type = [
            subtype_type.__name__ for subtype_type in self.value_type
        ]

        self.description = description

        self.consumed_next_positions_count = 1 if arg_type == 'keyword' else 0

    @property
    def data_type(self):
        return ', '.join(self._data_type)
    
    @property
    def value_type(self):
        return self._value_type if self._is_complex_type is False else tuple([
            get_origin(data_type) for data_type in self._value_type
        ])
    
    async def to_default(self):
        if self.default_is_awaitable:
            return await self.default()
        
        elif self.default_is_callable:
            return self.default()
        
        else:
            return self.default


    def to_help_string(
        self,
        descriptor: str | None = None
    ):
        
        if descriptor is None:
            descriptor = self.description

        arg_type = 'flag' if self.arg_type == 'flag' else self.data_type

        help_string = f'{self.full_flag}/{self.short_flag}: [{arg_type}]'

        if descriptor:
            help_string = f'{help_string} {descriptor}'

        return help_string
        
    def to_flag(self):
        return f'--{self.name}'
    
    async def parse(self, value: str | None = None):
        parse_error: Exception | None = None

        for subtype in self._value_type:
            try:
                if subtype == bytes:
                    return bytes(value, encoding='utf-8')
                
                elif get_origin(subtype) == Env:
                    environmental_variable = Env(
                        self.name,
                        subtype
                    )

                    return await environmental_variable.parse()
                
                elif get_origin(subtype) == Pattern:
                    pattern = Pattern(subtype)

                    return await pattern.parse(value)
                
                elif get_origin(subtype) == RawFile:
                    rawfile = RawFile(subtype)
                    return await rawfile.parse(value)
                
                elif get_origin(subtype) == JsonFile:
                    json_file = JsonFile(subtype)
                    return await json_file.parse(value)
                
                else:
                    return subtype(value)
            
            except Exception as e:
                parse_error = e

        return parse_error


def is_required_missing_keyword_arg(
    flag: str, 
    keyword_arg: KeywordArg,
    keyword_args: dict[str, KeywordArg]
):
    if keyword_args.get(keyword_arg.name):
        return False
    
    if flag != keyword_arg.full_flag:
        return False

    if keyword_arg.arg_type == 'flag':
        return False
    
    return keyword_arg.required
    
def is_defaultable(
    flag: str, 
    keyword_arg: KeywordArg,
    keyword_args: dict[str, KeywordArg]
):
    if keyword_args.get(keyword_arg.name):
        return False
    
    if flag != keyword_arg.full_flag:
        return False

    if keyword_arg.arg_type == 'flag':
        return False
    
    return keyword_arg.required is False
