import asyncio
from typing import List

from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.commands.cli.arg_types import KeywordArg
from hyperscale.ui.styling import stylize, get_style

from .cli_style import CLIStyle


def is_arg_descriptor(line: str):
    stripped_line = line.strip()
    return stripped_line.startswith("@param") or stripped_line.startswith(":param")


class TitleHelpMessage(BaseModel):
    command: StrictStr
    indentation: StrictInt = 0
    options: List[KeywordArg] | None = None
    styling: CLIStyle | None = None

    class Config:
        arbitrary_types_allowed = True

    async def to_message(
        self,
        global_styles: CLIStyle | None = None,
    ):
        indentation = self.indentation
        if global_styles.indentation:
            indentation = global_styles.indentation

        styles = self.styling
        if styles is None:
            styles = global_styles

        command_name = self.command

        if styles and styles.has_header_styles():
            command_name = await stylize(
                command_name,
                color=get_style(styles.header_color),
                highlight=get_style(styles.header_highlight),
                attrs=get_style(styles.header_attributes),
                mode=styles.to_mode(),
            )

        options_string: str | None = str(None)

        left_char = await self._style_text("[", styles)

        right_char = await self._style_text(
            "]",
            styles,
        )

        options = self.options
        if options:
            options = [arg.to_flag() for arg in self.options]

        else:
            options = ["None"]

        styled_options = await self._style_flags(
            [arg.to_flag() for arg in self.options],
            styles,
        )

        styled_options = left_char + styled_options + right_char

        indentation = " " * max(indentation - 1, 0)

        return f"{indentation}{command_name} {styled_options}"

    async def _style_flags(
        self,
        flags: list[str],
        styles: CLIStyle | None,
    ):
        options_join_char = ", "

        if styles and styles.has_text_styles():
            options_join_char = await stylize(
                options_join_char,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=[get_style(attribute) for attribute in styles.text_attributes]
                if styles.text_attributes
                else None,
                mode=styles.to_mode(),
            )

        if styles is None or styles.has_flag_styles() is False:
            return options_join_char.join(flags)

        styled_flags = await asyncio.gather(
            *[
                stylize(
                    flag,
                    color=get_style(styles.flag_color),
                    highlight=get_style(styles.flag_highlight),
                    attrs=[get_style(attribute) for attribute in styles.flag_attributes]
                    if styles.flag_attributes
                    else None,
                    mode=styles.to_mode(),
                )
                for flag in flags
            ]
        )

        return options_join_char.join(styled_flags)

    async def _style_text(
        self,
        text: str,
        styles: CLIStyle | None,
    ):
        if styles is None or styles.has_text_styles() is False:
            return text

        return await stylize(
            text,
            color=get_style(styles.text_color),
            highlight=get_style(styles.text_highlight),
            attrs=[get_style(attribute) for attribute in styles.text_attributes]
            if styles.text_attributes
            else None,
            mode=styles.to_mode(),
        )
