import base64
from typing import (
    Generic,
    Optional,
    TypeVar,
)

from hyperscale.core.engines.client.shared.protocols import NEW_LINE
from hyperscale.core.testing.models.base import OptimizedArg

from .auth_validator import AuthValidator

T = TypeVar("T")


class Auth(OptimizedArg, Generic[T]):
    def __init__(
        self,
        auth: tuple[str, str] | tuple[str],
    ) -> None:
        super(
            Auth,
            self,
        ).__init__()

        validated_auth = AuthValidator(value=auth)

        self.call_name: Optional[str] = None
        self.data = validated_auth.value
        self.optimized: Optional[str] = None

    async def optimize(self):
        if self.optimized is not None:
            return

        if len(self.data) > 1:
            credentials_string = f"{self.data[0]}:{self.data[1]}"
            encoded_credentials = base64.b64encode(
                credentials_string.encode(),
            ).decode()

        else:

            encoded_credentials = base64.b64encode(
                self.data[0].encode()
            ).decode()

        self.optimized = f'Authorization: Basic {encoded_credentials}{NEW_LINE}'

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
