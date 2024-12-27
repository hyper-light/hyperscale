from enum import Enum
from typing import Literal

WidgetFitDimensionsType = Literal["x_axis", "y_axis", "x_y_axis"]


class WidgetFitDimensions(Enum):
    X_AXIS = "x_axis"
    Y_AXIS = "y_axis"
    X_Y_AXIS = "x_y_axis"

    @classmethod
    def to_fit_type(self, fit_type: WidgetFitDimensionsType):
        match fit_type:
            case "x_axis":
                return WidgetFitDimensions.X_AXIS

            case "y_axis":
                return WidgetFitDimensions.Y_AXIS

            case "x_y_axis":
                return WidgetFitDimensions.X_Y_AXIS

            case _:
                return WidgetFitDimensions.X_AXIS
