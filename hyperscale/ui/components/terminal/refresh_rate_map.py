from typing import Dict, Literal

from .refresh_rate_enum import RefreshRate

RefreshRateProfile = Literal["low", "medium", "high", "ultra"]


class RefreshRateMap:
    rates: Dict[RefreshRateProfile, RefreshRate] = {
        "low": RefreshRate.LOW,
        "medium": RefreshRate.MEDIUM,
        "high": RefreshRate.HIGH,
        "ultra": RefreshRate.ULTRA,
    }

    @classmethod
    def to_refresh_rate(cls, refresh_rate_profile: RefreshRateProfile):
        return cls.rates.get(refresh_rate_profile, RefreshRate.MEDIUM)
