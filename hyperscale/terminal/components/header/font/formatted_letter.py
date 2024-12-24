from pydantic import BaseModel, StrictStr, StrictInt


class FormattedLetter(BaseModel):
    plaintext_letter: StrictStr
    ascii: StrictStr
    height: StrictInt
    width: StrictInt
