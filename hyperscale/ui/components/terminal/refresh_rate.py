from enum import Enum
from typing import Literal, Dict

RefreshRateProfile = Literal["low", "medium", "high", "ultra"]


class RefreshRate(Enum):
    LOW = 15
    MEDIUM = 30
    HIGH = 60
    ULTRA = 120


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
