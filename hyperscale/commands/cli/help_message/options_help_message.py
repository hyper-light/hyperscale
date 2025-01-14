import asyncio
from typing import List

from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.commands.cli.arg_types import KeywordArg, Context
from hyperscale.ui.styling import stylize, get_style
from hyperscale.ui.styling.attributes import Attributizer

from .cli_style import CLIStyle
from .line_parsing import is_arg_descriptor


class OptionsHelpMessage(BaseModel):
    options: List[KeywordArg]
    help_string: StrictStr
    indentation: StrictInt = 0
    header: StrictStr = "options"
    styling: CLIStyle | None = None

    class Config:
        arbitrary_types_allowed = True

    def _map_doc_string_param_descriptors(self, styles: CLIStyle | None = None):
        param_lines = [
            line.strip()
            for line in self.help_string.split("\n")
            if is_arg_descriptor(line)
        ]

        param_descriptors: dict[str, str] = {}

        for line in param_lines:
            if line.startswith("@param"):
                cleaned_line = line.strip("@param").strip()
                name, descriptor = cleaned_line.split(" ", maxsplit=1)

                param_descriptors[name] = descriptor

            elif line.startswith(":param"):
                cleaned_line = line.strip(":param").strip()
                _, name, descriptor = cleaned_line.split(" ", maxsplit=2)

                param_descriptors[name] = descriptor

        return param_descriptors

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

        param_descriptors = self._map_doc_string_param_descriptors(styles=styles)

        tabs = " " * indentation
        join_char = f"\n{tabs}"

        arg_string = join_char.join(
            [
                await self._to_help_string(
                    arg, descriptor=param_descriptors.get(arg.name), styles=styles
                )
                for arg in self.options
                if Context not in arg.value_type
            ]
        )

        header_indentation = max(indentation - 1, 0)
        header_indentation_tabs = f" " * header_indentation
        header_join_char = f"\n{header_indentation_tabs}"

        header = self.header
        if styles and styles.has_header_styles():
            header = await stylize(
                header,
                color=get_style(styles.header_color),
                highlight=get_style(styles.header_highlight),
                attrs=get_style(styles.header_attributes),
                mode=styles.to_mode(),
            )

        return f"{header_join_char}{header}:{join_char}{arg_string}"

    async def _to_help_string(
        self,
        arg: KeywordArg,
        descriptor: str | None = None,
        styles: CLIStyle | None = None,
    ):
        if descriptor is None:
            descriptor = arg.description

        if styles and styles.has_flag_description_styles():
            descriptor = await stylize(
                descriptor,
                color=get_style(styles.flag_description_color),
                highlight=get_style(styles.flag_description_highlight),
                attrs=[
                    get_style(attribute)
                    for attribute in styles.flag_description_attributes
                ]
                if styles.flag_description_attributes
                else None,
                mode=styles.to_mode(),
            )

        elif styles and styles.has_description_styles():
            descriptor = await stylize(
                descriptor,
                color=get_style(styles.description_color),
                highlight=get_style(styles.description_highlight),
                attrs=[
                    get_style(attribute) for attribute in styles.description_attributes
                ]
                if styles.description_attributes
                else None,
                mode=styles.to_mode(),
            )

        elif styles and styles.has_text_styles():
            descriptor = await stylize(
                descriptor,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=[get_style(attribute) for attribute in styles.text_attributes]
                if styles.text_attributes
                else None,
                mode=styles.to_mode(),
            )

        separator_char = "/"
        colon_char = ":"
        left_brace = "["
        right_brace = "]"

        if styles and styles.has_text_styles():
            text_attributes = (
                [get_style(attribute) for attribute in styles.text_attributes]
                if styles.text_attributes
                else None
            )

            separator_char = await stylize(
                separator_char,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=text_attributes,
                mode=styles.to_mode(),
            )

            colon_char = await stylize(
                colon_char,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=text_attributes,
                mode=styles.to_mode(),
            )

            left_brace = await stylize(
                left_brace,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=text_attributes,
                mode=styles.to_mode(),
            )

            right_brace = await stylize(
                right_brace,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=text_attributes,
                mode=styles.to_mode(),
            )

        full_flag = arg.full_flag
        short_flag = arg.short_flag
        arg_types = ["flag"] if arg.arg_type == "flag" else arg.data_type_list
        arg_types_join_char = ", "

        if len(arg_types) < 1:
            arg_types = ["None"]

        if styles and styles.has_flag_styles():
            flag_attributes = (
                [get_style(attribute) for attribute in styles.flag_attributes]
                if styles.flag_attributes
                else None
            )

            full_flag = await stylize(
                full_flag,
                color=get_style(styles.flag_color),
                highlight=get_style(styles.flag_highlight),
                attrs=flag_attributes,
                mode=styles.to_mode(),
            )

            short_flag = await stylize(
                short_flag,
                color=get_style(styles.flag_color),
                highlight=get_style(styles.flag_highlight),
                attrs=flag_attributes,
                mode=styles.to_mode(),
            )

            arg_types = await asyncio.gather(
                *[
                    stylize(
                        arg_type,
                        color=get_style(styles.flag_color),
                        highlight=get_style(styles.flag_highlight),
                        attrs=flag_attributes,
                        mode=styles.to_mode(),
                    )
                    for arg_type in arg_types
                ]
            )

            arg_types_join_char = await stylize(
                arg_types_join_char,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=[get_style(attribute) for attribute in styles.text_attributes]
                if styles.text_attributes
                else None,
                mode=styles.to_mode(),
            )

        arg_types_string = arg_types_join_char.join(arg_types)

        help_string = f"{full_flag}{separator_char}{short_flag}{colon_char} {left_brace}{arg_types_string}{right_brace}"

        if descriptor:
            help_string = f"{help_string} {descriptor}"

        return help_string
