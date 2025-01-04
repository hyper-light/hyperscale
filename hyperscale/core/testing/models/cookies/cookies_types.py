from typing import Tuple

from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

HTTPCookie = Tuple[str, HTTPEncodableValue]
