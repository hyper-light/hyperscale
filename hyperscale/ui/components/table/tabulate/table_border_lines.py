from typing import List, Literal

from pydantic import BaseModel, StrictInt, StrictStr

BorderCharsetType = Literal[
    "line_above",
    "line_below_header",
    "line_between_rows",
    "line_below",
    "header_row",
    "data_row",
]


class TableBorderCharset(BaseModel):
    charset_type: BorderCharsetType
    begin_char: StrictStr | None = None
    fill_char: StrictStr | None = None
    separator_char: StrictStr | None = None
    end_char: StrictStr | None = None

    def size(self):
        chars = [self.begin_char, self.fill_char, self.separator_char, self.end_char]

        return sum([len(char) for char in chars if char is not None])


class TableBorderLines(BaseModel):
    line_above: TableBorderCharset | None = None
    line_below_header: TableBorderCharset | None = None
    line_between_rows: TableBorderCharset | None = None
    line_below: TableBorderCharset | None = None
    header_row: TableBorderCharset | None = None
    data_row: TableBorderCharset | None = None
    padding: StrictInt = 0
    hide_lines: List[
        Literal[
            "line_above",
            "line_below_header",
            "line_between_rows",
            "line_below",
            "header_row",
            "data_row",
        ],
    ] = []
