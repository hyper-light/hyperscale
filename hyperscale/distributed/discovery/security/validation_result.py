"""``ValidationResult`` -- pickled under the namespace
``hyperscale.distributed.discovery.security.role_validator`` (see that module)."""

from dataclasses import dataclass

from .certificate_claims import CertificateClaims


@dataclass(slots=True)
class ValidationResult:
    """Result of role validation."""

    allowed: bool
    """Whether the connection is allowed."""

    reason: str
    """Explanation of the decision."""

    source_claims: CertificateClaims | None = None
    """Claims of the source node."""

    target_claims: CertificateClaims | None = None
    """Claims of the target node."""
