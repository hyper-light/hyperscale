from typing import List

from pydantic import BaseModel

from .cookies_types import HTTPCookie


class CookiesValidator(BaseModel):
    value: List[HTTPCookie]
