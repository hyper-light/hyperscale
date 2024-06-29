from typing import Any

from hyperscale.versioning.flags.exceptions.latest_not_enabled import (
    LatestNotEnabledException,
)
from hyperscale.versioning.flags.types.base.feature import Flag
from hyperscale.versioning.flags.types.base.flag_type import FlagTypes


class UnstableFeature(Flag):
    def __init__(
        self,
        feature_name: str,
        feature: Any,
    ) -> None:
        super().__init__(
            feature_name,
            feature,
            FlagTypes.UNSTABLE_FEATURE,
            LatestNotEnabledException,
        )
