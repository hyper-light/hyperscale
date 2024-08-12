from hyperscale.terminal.components.spinner import Spinner

from .segment_type import SegmentType


class Segment:
    def __init__(
        self,
        segment_char: Spinner | str,
        segment_type: SegmentType,
        segment_default_char: str | None = None,
    ) -> None:
        self.segment_char = segment_char
        self.segment_type = segment_type
        self.segment_default_char = segment_default_char
