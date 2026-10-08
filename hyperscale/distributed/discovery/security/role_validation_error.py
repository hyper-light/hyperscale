"""``RoleValidationError`` -- pickled under the namespace
``hyperscale.distributed.discovery.security.role_validator`` (see that module)."""

from hyperscale.distributed.models.distributed import NodeRole


class RoleValidationError(Exception):
    """Raised when role validation fails."""

    def __init__(
        self,
        source_role: NodeRole,
        target_role: NodeRole,
        message: str,
    ):
        self.source_role = source_role
        self.target_role = target_role
        super().__init__(
            f"Role validation failed: {source_role.value} -> {target_role.value}: {message}"
        )
