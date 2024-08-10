from dataclasses import dataclass


@dataclass
class Spinner:
    frames: str
    interval: int


default_spinner = Spinner("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏", 80)
