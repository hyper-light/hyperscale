import textwrap

from pydantic import BaseModel, StrictStr, StrictInt

from .cli_style import CLIStyle
from .line_parsing import is_arg_descriptor
from hyperscale.ui.styling import stylize, get_style


class DescriptionHelpMessage(BaseModel):
    help_string: StrictStr
    indentation: StrictInt = 0
    styling: CLIStyle | None = None

    async def to_message(
        self,
        global_styles: CLIStyle | None = None,
    ):
        indentation = self.indentation
        if global_styles.indentation:
            indentation = global_styles.indentation

        tabs = " " * indentation
        join_char = f"\n{tabs}"

        help_message_lines = join_char.join(
            [
                line
                for line in textwrap.dedent(self.help_string).split("\n")
                if not is_arg_descriptor(line) and len(line.strip()) > 0
            ]
        )

        styles = self.styling
        if styles is None:
            styles = global_styles

        if styles and styles.has_description_styles():
            help_message_lines = await stylize(
                help_message_lines,
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
            help_message_lines = await stylize(
                help_message_lines,
                color=get_style(styles.text_color),
                highlight=get_style(styles.text_highlight),
                attrs=[get_style(attribute) for attribute in styles.text_attributes]
                if styles.text_attributes
                else None,
                mode=styles.to_mode(),
            )

        header_indentation = " " * max(indentation - 1, 0)
        header_join_char = f"\n{header_indentation}"

        header = "description"
        if styles and styles.has_header_styles():
            header = await stylize(
                header,
                color=get_style(styles.header_color),
                highlight=get_style(styles.header_highlight),
                attrs=[get_style(attribute) for attribute in styles.header_attributes]
                if styles.header_attributes
                else None,
            )

        return f"{header_join_char}{header}:{join_char}{help_message_lines}"
