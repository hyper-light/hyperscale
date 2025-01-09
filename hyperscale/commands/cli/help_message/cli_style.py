from pydantic import BaseModel
from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer


class CLIStyle(BaseModel):
    description_color: Colorizer | None = None
    description_highlight: HighlightColorizer | None = None
    description_attributes: Attributizer | None = None
    header_color: Colorizer | None = None
    header_highlight: HighlightColorizer | None = None
    header_attributes: Attributizer | None = None
    title_color: Colorizer | None = None
    title_highlight: HighlightColorizer | None = None
    title_attributes: Attributizer | None = None
    link_color: Colorizer | None = None
    link_highlight: HighlightColorizer | None = None
    link_attributes: Attributizer | None = None
    error_color: Colorizer | None = None
    error_highlight: HighlightColorizer | None = None
    error_attributes: Attributizer | None = None

    def has_description_styles(self):
        return (
            self.description_color is not None
        ) or (
            self.description_highlight is not None
        ) or (
            self.description_attributes is not None
        )
    
    def has_header_styles(self):
        return (
            self.header_color is not None
        ) or (
            self.header_highlight is not None
        ) or (
            self.header_attributes is not None
        )
    
    def has_title_styles(self):
        return (
            self.title_color is not None
        ) or (
            self.title_highlight is not None
        ) or (
            self.title_attributes is not None
        )
    
    def has_link_styles(self):
        return (
            self.link_color is not None
        ) or (
            self.link_highlight is not None
        ) or (
            self.link_attributes is not None
        )
    
    def has_error_style(self):
        return (
            self.error_color is not None
        ) or (
            self.error_highlight is not None
        ) or (
            self.error_attributes is not None
        )