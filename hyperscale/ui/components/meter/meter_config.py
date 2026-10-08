from pydantic import BaseModel, StrictBool

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import ColorName, ExtendedColorName


class MeterConfig(BaseModel):
    """A meter's look: the color its fill is drawn in when a reading
    gives no tone, and whether it draws its reading's label."""

    fill_color: ColorName | ExtendedColorName = "aquamarine_2"
    show_label: StrictBool = True
    terminal_mode: TerminalDisplayMode = "compatability"
