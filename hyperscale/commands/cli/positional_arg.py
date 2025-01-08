from typing import Generic, TypeVar, get_args


T = TypeVar('T')


class PositionalArg(Generic[T]):

    def __init__(
        self,
        name: str,
        index: int,
        data_type: type[T],
        description: str | None = None
    ):
        self.name = name
        self.index = index

        args = get_args(data_type)
        if len(args) > 0:
            self.value_type = args

        else:
            self.value_type = tuple([data_type])

        self._data_type = [
            subtype_type.__name__ for subtype_type in self.value_type
        ]

        self.description = description

    @property
    def data_type(self):
        return ', '.join(self._data_type)

    def to_help_string(self):

        help_string = f'{self.name}: [{self.data_type}]'

        if self.description:
            help_string = f'{help_string} {self.description}'

        return help_string
    
    def parse(self, value: str):

        parse_error: Exception | None = None

        for subtype in self.value_type:
            try:
                if subtype == bytes:
                    return bytes(value, encoding='utf-8')
                
                else:
                    return subtype(value)
            
            except Exception as e:
                parse_error = e

        return parse_error