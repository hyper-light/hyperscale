import asyncio
import textwrap

from pydantic import BaseModel, StrictInt

from hyperscale.commands.cli.arg_types import KeywordArg
from hyperscale.ui.styling import get_style, stylize

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
        global_styles: CLIStyle | None = None,
    ):
        styles = self.styling
        if styles is None:
            styles = global_styles

        indentation = self.indentation
        if global_styles:
            indentation = global_styles.indentation

        lines: list[str] = []

        if styles and styles.header:
            header_indentation = max(indentation - 1, 0)
            header_text = textwrap.indent(
                await styles.header(), " " * header_indentation
            )

            lines.append(f"{header_text}\n")

        error_header = "error"

        if error and styles and styles.has_error_styles():
            error_header = await stylize(
                error_header,
                color=get_style(styles.header_color),
                highlight=get_style(styles.header_highlight),
                attrs=[get_style(attribute) for attribute in styles.header_attributes]
                if styles.header_highlight
                else None,
            )

            error = await stylize(
                error,
                color=get_style(styles.error_color),
                highlight=get_style(styles.error_highlight),
                attrs=[get_style(attribute) for attribute in styles.error_attributes]
                if styles.error_attributes
                else None,
                mode=styles.to_mode(),
            )

        if error:
            error_indentation = " " * max(indentation - 1, 0)
            lines.append(f"{error_indentation}{error_header}: {error}\n")

        lines.extend(
            [
                await self.title.to_message(
                    global_styles=styles,
                ),
                await self.description.to_message(
                    global_styles=styles,
                ),
            ]
        )

        if self.options:
            lines.append(
                await self.options.to_message(
                    global_styles=styles,
                )
            )

        if subcommands and len(subcommands) > 0:
            lines.append(
                await self._create_subcommands_description(
                    subcommands, indentation, styles=styles
                )
            )

        message_lines = "\n".join(lines)

        return f"\033[2J\033[H\n{message_lines}\n\n"

    async def _create_subcommands_description(
        self,
        subcommands: list[str],
        indentation: int,
        styles: CLIStyle | None = None,
    ):
        tabs = " " * indentation
        join_char = f"\n{tabs}"

        styled_subcommands: list[str] = []
        if styles and styles.has_subcommand_styles():
            for subcommand in styled_subcommands:
                styled_subcommands.append(
                    await stylize(
                        subcommand,
                        color=get_style(styles.subcommand_color),
                        highlight=get_style(styles.subcommand_highlight),
                        attrs=[
                            get_style(attribute)
                            for attribute in styles.subcommand_attributes
                        ]
                        if styles.subcommand_attributes
                        else None,
                    )
                )

        else:
            styled_subcommands = subcommands

        header_indentation = max(indentation - 1, 0)
        header_indentation_tabs = " " * header_indentation

        header = "commands"
        if styles and styles.has_subcommand_styles():
            subcommands = await asyncio.gather(
                *[
                    stylize(
                        subcommand,
                        color=get_style(styles.subcommand_color),
                        highlight=get_style(styles.subcommand_highlight),
                        attrs=[
                            get_style(attribute)
                            for attribute in styles.subcommand_attributes
                        ]
                        if styles.subcommand_attributes
                        else None,
                        mode=styles.to_mode(),
                    )
                    for subcommand in subcommands
                ]
            )

        if styles and styles.has_header_styles():
            header = await stylize(
                header,
                color=get_style(styles.header_color),
                highlight=get_style(styles.header_highlight),
                attrs=[get_style(attribute) for attribute in styles.header_attributes]
                if styles.header_attributes
                else None,
                mode=styles.to_mode(),
            )

        subcommands_string = join_char.join(subcommands)

        return f"\n{header_indentation_tabs}{header}:{join_char}{subcommands_string}"


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
    options = [
        keyword_args_map[arg_name]
        for arg_name in sorted_arg_keys
        if arg_name == keyword_args_map[arg_name].full_flag
    ]

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
        indentation=indentation,
        styling=styling,
    )
