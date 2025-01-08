from typing import Literal, Generic, TypeVar, Union, get_args


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
        description: str | None = None
    ):
        if short_name is None:
            short_name = name[:1]

        self.name = name
        self.short_name = short_name

        self.full_flag = f'--{name}'
        self.short_flag = f'-{short_name}'

        args = get_args(data_type)
        if len(args) > 0:
            self.value_type = args

        else:
            self.value_type = tuple([data_type])

        self.required = required

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
