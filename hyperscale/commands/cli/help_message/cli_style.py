from pydantic import BaseModel, StrictInt
from hyperscale.ui.config.mode import TerminalDisplayMode, TerminalMode
from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from typing import List, Callable, Awaitable, Any


class CLIStyle(BaseModel):
    header: Callable[..., Awaitable[List[str]]] | None = None
    description_color: Colorizer | None = None
    description_highlight: HighlightColorizer | None = None
    description_attributes: List[Attributizer] | None = None
    flag_description_color: Colorizer | None = None
    flag_description_highlight: HighlightColorizer | None = None
    flag_description_attributes: List[Attributizer] | None = None
    error_color: Colorizer | None = None
    error_highlight: HighlightColorizer | None = None
    error_attributes: List[Attributizer] | None = None
    flag_color: Colorizer | None = None
    flag_highlight: HighlightColorizer | None = None
    flag_attributes: List[Attributizer] | None = None
    header_color: Colorizer | None = None
    header_highlight: HighlightColorizer | None = None
    header_attributes: List[Attributizer] | None = None
    link_color: Colorizer | None = None
    link_highlight: HighlightColorizer | None = None
    link_attributes: List[Attributizer] | None = None
    subcommand_color: Colorizer | None = None
    subcommand_highlight: HighlightColorizer | None = None
    subcommand_attributes: List[Attributizer] | None = None
    text_color: Colorizer | None = None
    text_highlight: HighlightColorizer | None = None
    text_attributes: List[Attributizer] | None = None
    indentation: StrictInt = 0
    terminal_mode: TerminalDisplayMode = "compatability"

    class Config:
        allow_arbitrary_types = True

    def to_mode(self):
        return TerminalMode.to_mode(self.terminal_mode)

    def has_flag_description_styles(self):
        return (
            (self.flag_description_color is not None)
            or (self.flag_description_highlight is not None)
            or (self.flag_description_attributes is not None)
        )

    def has_subcommand_styles(self):
        return (
            (self.subcommand_color is not None)
            or (self.subcommand_highlight is not None)
            or (self.subcommand_attributes is not None)
        )

    def has_text_styles(self):
        return (
            (self.text_color is not None)
            or (self.text_highlight is not None)
            or (self.text_attributes is not None)
        )

    def has_description_styles(self):
        return (
            (self.description_color is not None)
            or (self.description_highlight is not None)
            or (self.description_attributes is not None)
        )

    def has_header_styles(self):
        return (
            (self.header_color is not None)
            or (self.header_highlight is not None)
            or (self.header_attributes is not None)
        )

    def has_flag_styles(self):
        return (
            (self.flag_color is not None)
            or (self.flag_highlight is not None)
            or (self.flag_attributes is not None)
        )

    def has_link_styles(self):
        return (
            (self.link_color is not None)
            or (self.link_highlight is not None)
            or (self.link_attributes is not None)
        )

    def has_error_styles(self):
        return (
            (self.error_color is not None)
            or (self.error_highlight is not None)
            or (self.error_attributes is not None)
        )
