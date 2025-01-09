from pydantic import BaseModel, StrictInt
from hyperscale.commands.cli.arg_types import KeywordArg


from .cli_style import CLIStyle
from .description_help_message import DescriptionHelpMessage
from .options_help_message import OptionsHelpMessage
from .title_help_message import TitleHelpMessage


class HelpMessage(BaseModel):
    title: TitleHelpMessage
    options: OptionsHelpMessage | None = None
    description: DescriptionHelpMessage
    indentation: StrictInt = 0
    styling: CLIStyle | None = None

    async def to_lines(
        self,
        subcommands: list[str] | None = None,
        error: str | None = None,
        global_styles: CLIStyle | None = None
    ):
        
        styles = self.styling
        if styles is None:
            styles = global_styles
        
        lines: list[str] = []

        if error:
            lines.append(error)

        lines.extend([
            await self.title.to_message(),
            await self.description.to_message()
        ])
        

        if self.options:
            lines.append(
                await self.options.to_message()
            )

        if subcommands:
            lines.append(
                await self._create_subcommands_description(subcommands)
            )

        return '\n'.join(lines)

    async def _create_subcommands_description(
        self, 
        subcommands: list[str],
    ):
        tabs = ' ' * self.indentation
        join_char = f'\n{tabs}'

        subcommands_string = join_char.join(subcommands)

        header_indentation = max(self.indentation - 1, 0)
        header_indentation_tabs = f' ' * header_indentation
        header_join_char = f'\n{header_indentation_tabs}'

        header = 'commands'
        
        return f'{header_join_char}{header}:{join_char}{subcommands_string}'


def create_help_string(
    command_name: str,
    help_string: str,
    indentation: int | None = None,
    keyword_args_map: dict[str, KeywordArg] | None = None,
    styling: CLIStyle | None = None,
):
    
    if indentation is None:
        indentation = 0

    if help_string is None:
        help_string = "No description found..."

    sorted_arg_keys = sorted(keyword_args_map.keys())
    options = [keyword_args_map[arg_name] for arg_name in sorted_arg_keys if arg_name == keyword_args_map[arg_name].full_flag]

    return HelpMessage(
        title=TitleHelpMessage(
            command=command_name,
            options=options,
            styling=styling,
        ),
        description=DescriptionHelpMessage(
            help_string=help_string,
            indentation=indentation,
            styling=styling,
        ),
        options=OptionsHelpMessage(
            options=options,
            help_string=help_string,
            indentation=indentation,
            styling=styling,
        ),
        styling=styling,
    )
