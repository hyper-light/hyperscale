from pydantic import BaseModel, StrictStr

from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.styling.colors import ColorName, ExtendedColorName


class StatTileConfig(BaseModel):
    """A stat tile's label (drawn dim, as given: ``WORKERS``), the color
    its meter fills in when a reading gives no tone, and its mode."""

    label: StrictStr
    meter_fill_color: ColorName | ExtendedColorName = "aquamarine_2"
    terminal_mode: TerminalDisplayMode = "compatability"
