from enum import Enum


class FillCharType(Enum):
    EMPTY = " "
    BLOCK = "█"
    EQUALS = "="
    DOT = "●"
    PERIOD = "."
    LEFT_ARROW_EMPTY = "▹"
    LEFT_ARROW_FULL = "▸"
    NOISE_LIGHT = "░"
    NOISE_MEDIUM = "▒"
    NOISE_HEAVY = "▓"
    EMPTY_DOT = "o"
    SQUARE = "▄"
    DASH = "-"
    DOT_BLOCK = "⣿"
    SLANT_RECT = "▰"
    CENTER_PERIOD = "∙"
    TRIPLE_EQUALS = "≡"
    TOGGLE = "⊶"
    CIRCLE_TOGGLE = "◉"
