from pydantic import BaseModel, StrictStr, StrictInt
from typing import List


class FormattedWord(BaseModel):
    plaintext_word: StrictStr
    ascii: StrictStr
    ascii_lines: List[StrictStr]
    height: StrictInt
    width: StrictInt
