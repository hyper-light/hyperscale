from typing import (
    Generic,
    Optional,
    Tuple,
    TypeVar,
)

from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .auth_validator import AuthValidator

T = TypeVar("T")


class Auth(OptimizedArg, Generic[T]):
    def __init__(self, auth: Tuple[str, str]) -> None:
        super(
            Auth,
            self,
        ).__init__()

        validated_auth = AuthValidator(value=auth)

        self.data = validated_auth.value
        self.optimized: Optional[str] = None

    @property
    def user(self):
        if len(self.data) > 1:
            return self.data[0]

    @property
    def password(self):
        if len(self.data) > 1:
            return self.data[1]

    @property
    def token(self):
        if len(self.data) < 2:
            return self.data[0]

    def __str__(self) -> str:
        if len(self.data) > 1:
            return f"{self.data[0]}:{self.data[1]}"

        return self.data[0]

    def __repr__(self) -> str:
        if len(self.data) > 1:
            return f"{self.data[0]}:{self.data[1]}"

        return self.data[0]
