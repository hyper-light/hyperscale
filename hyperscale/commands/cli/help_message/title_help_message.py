from typing import List

from pydantic import BaseModel, StrictStr
from hyperscale.commands.cli.arg_types import KeywordArg

from .cli_style import CLIStyle


def is_arg_descriptor(line: str):
    stripped_line = line.strip()
    return stripped_line.startswith('@param') or stripped_line.startswith(':param')


class TitleHelpMessage(BaseModel):
    command: StrictStr
    options: List[KeywordArg] | None = None
    styling: CLIStyle | None = None
    
    class Config:
        arbitrary_types_allowed=True

    async def to_message(self):

        command_name = self.command
        
        options_string: str | None = None
        if self.options:
            options_string = ','.join([
                arg.to_flag() for arg in self.options
            ])

        return f'\n{command_name} [{options_string}]'