import textwrap

from pydantic import BaseModel, StrictStr, StrictInt

from .cli_style import CLIStyle
from .line_parsing import is_arg_descriptor


class DescriptionHelpMessage(BaseModel):
    help_string: StrictStr
    indentation: StrictInt = 0
    styling: CLIStyle | None = None

    async def to_message(self):
        tabs = ' ' * self.indentation
        join_char = f'\n{tabs}'

        help_message_lines = join_char.join([
            line
            for line in textwrap.dedent(
                self.help_string
            ).split('\n') 
            if not is_arg_descriptor(line) and len(line.strip()) > 0
        ])
        
        return f'{join_char}{help_message_lines}'
