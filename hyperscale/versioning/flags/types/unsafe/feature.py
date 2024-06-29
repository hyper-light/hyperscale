from typing import Any

from hyperscale.versioning.flags.exceptions.unsafe_not_enabled import (
    UnsafeNotEnabledException,
)
from hyperscale.versioning.flags.types.base.feature import Flag
from hyperscale.versioning.flags.types.base.flag_type import FlagTypes


class UnsafeFeature(Flag):
    def __init__(self, feature_name: str, feature: Any) -> None:
        super().__init__(
            feature_name, feature, FlagTypes.UNSAFE_FEATURE, UnsafeNotEnabledException
        )
