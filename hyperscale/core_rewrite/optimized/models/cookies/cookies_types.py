from typing import Tuple

from hyperscale.core_rewrite.optimized.models.base.base_types import (
    HTTPEncodableValue,
)

HTTPCookie = Tuple[str, HTTPEncodableValue]
