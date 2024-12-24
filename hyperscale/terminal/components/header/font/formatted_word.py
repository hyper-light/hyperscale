from pydantic import BaseModel, StrictStr, StrictInt


class FormattedWord(BaseModel):
    plaintext_word: StrictStr
    ascii: StrictStr
    height: StrictInt
    width: StrictInt
