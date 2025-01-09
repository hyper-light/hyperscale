from typing import List

from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.commands.cli.arg_types import KeywordArg, Context

from .cli_style import CLIStyle
from .line_parsing import is_arg_descriptor


class OptionsHelpMessage(BaseModel):
    options: List[KeywordArg]
    help_string: StrictStr
    indentation: StrictInt = 0
    header: StrictStr = 'options'
    styling: CLIStyle | None = None

    class Config:
        arbitrary_types_allowed=True

    def _map_doc_string_param_descriptors(self):
        param_lines = [
            line.strip()
            for line in self.help_string.split('\n') 
            if is_arg_descriptor(line)
        ]

        param_descriptors: dict[str, str] = {}

        for line in param_lines:
            if line.startswith('@param'):
                cleaned_line = line.strip('@param').strip()
                name, descriptor = cleaned_line.split(' ', maxsplit=1)

                param_descriptors[name] = descriptor

            elif line.startswith(':param'):
                cleaned_line = line.strip(':param').strip()
                _, name, descriptor = cleaned_line.split(' ', maxsplit=2)

                param_descriptors[name] = descriptor

        return param_descriptors
    
    async def to_message(self):
        param_descriptors = self._map_doc_string_param_descriptors()

        tabs = ' ' * self.indentation
        join_char = f'\n{tabs}'
        
        arg_string = join_char.join([
            arg.to_help_string(
                descriptor=param_descriptors.get(arg.name)
            ) for arg in self.options if Context not in arg.value_type
        ])

        header_indentation = max(self.indentation - 1, 0)
        header_indentation_tabs = f' ' * header_indentation
        header_join_char = f'\n{header_indentation_tabs}'

        return f'{header_join_char}{self.header}:{join_char}{arg_string}'
