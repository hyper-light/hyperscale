from typing import Generic, TypeVar, get_args, get_origin
from.context import Context
from .env import Env
from .pattern import Pattern


T = TypeVar('T')


class PositionalArg(Generic[T]):

    def __init__(
        self,
        name: str,
        index: int,
        data_type: type[T],
        description: str | None = None,
        is_context_arg: bool = False
    ):
        self.name = name
        self.index = index
        self.is_context_arg = is_context_arg

        self.is_envar: bool = False

        args = get_args(data_type)
        self._is_complex_type = get_origin(data_type) in [Pattern, Env, Context]

        if len(args) > 0 and self._is_complex_type is False:
            self._value_type = args

        elif self._is_complex_type:
            self._value_type = tuple([
                data_type
            ])

        else:
            self._value_type = tuple([data_type])

        self.loads_from_envar = len([
            subtype for subtype in self.value_type if get_origin(subtype) == Env
        ]) > 0

        self._data_type = [
            subtype_type.__name__ for subtype_type in self.value_type
        ]

        self.description = description

    @property
    def data_type(self):
        return ', '.join(self._data_type)
    
    @property
    def value_type(self):
        return self._value_type if self._is_complex_type is False else tuple([
            get_origin(data_type) for data_type in self._value_type
        ])

    def to_help_string(self):

        help_string = f'{self.name}: [{self.data_type}]'

        if self.description:
            help_string = f'{help_string} {self.description}'

        return help_string
    
    async def parse(self, value: str | None = None):

        parse_error: Exception | None = None

        for subtype in self.value_type:
            try:
                
                if get_origin(subtype) == Env:
                    environmental_variable = Env(
                        self.name,
                        get_origin(subtype)
                    )

                    return await environmental_variable.parse()
                
                elif get_origin(subtype) == Pattern:
                    pattern = Pattern(subtype)

                    return await pattern.parse(value)
                

                elif subtype == bytes:
                    return bytes(value, encoding='utf-8')
                
                else:
                    return subtype(value)
            
            except Exception as e:
                parse_error = e

        return parse_error