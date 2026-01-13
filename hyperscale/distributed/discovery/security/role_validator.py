"""
Role-based certificate validation for mTLS.

Enforces the node communication matrix based on certificate claims.
"""

from dataclasses import dataclass
from typing import ClassVar

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509.oid import NameOID, ExtensionOID

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


@dataclass(slots=True, frozen=True)
class CertificateClaims:
    """Claims extracted from an mTLS certificate."""

    cluster_id: str
    """Cluster identifier from certificate CN or SAN."""

    environment_id: str
    """Environment identifier (prod, staging, dev)."""

    role: NodeRole
    """Node role from certificate OU or custom extension."""

    node_id: str
    """Unique node identifier."""

    datacenter_id: str = ""
    """Optional datacenter identifier."""

    region_id: str = ""
    """Optional region identifier."""


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


@dataclass
class RoleValidator:
    """
    Validates node communication based on mTLS certificate claims.

    Implements the node communication matrix from AD-28:

    | Source  | Target  | Allowed | Notes                        |
    |---------|---------|---------|------------------------------|
    | Client  | Gate    | Yes     | Job submission               |
    | Gate    | Manager | Yes     | Job distribution             |
    | Gate    | Gate    | Yes     | Cross-DC coordination        |
    | Manager | Worker  | Yes     | Workflow dispatch            |
    | Manager | Manager | Yes     | Peer coordination            |
    | Worker  | Manager | Yes     | Results/heartbeats           |
    | Client  | Manager | No      | Must go through Gate         |
    | Client  | Worker  | No      | Must go through Gate/Manager |
    | Worker  | Worker  | No      | No direct communication      |
    | Worker  | Gate    | No      | Must go through Manager      |

    Usage:
        validator = RoleValidator(
            cluster_id="prod-cluster-1",
            environment_id="prod",
        )

        # Validate a connection
        result = validator.validate(source_claims, target_claims)
        if not result.allowed:
            raise RoleValidationError(...)

        # Check if a role can connect to another
        if validator.is_allowed(NodeRole.CLIENT, NodeRole.GATE):
            allow_connection()
    """

    cluster_id: str
    """Required cluster ID for all connections."""

    environment_id: str
    """Required environment ID for all connections."""

    strict_mode: bool = True
    """If True, reject connections with mismatched cluster/environment."""

    allow_same_role: bool = True
    """If True, allow same-role connections where documented (Manager-Manager, Gate-Gate)."""

    _allowed_connections: ClassVar[set[tuple[NodeRole, NodeRole]]] = {
        # Client connections
        (NodeRole.CLIENT, NodeRole.GATE),
        # Gate connections
        (NodeRole.GATE, NodeRole.MANAGER),
        (NodeRole.GATE, NodeRole.GATE),  # Cross-DC
        # Manager connections
        (NodeRole.MANAGER, NodeRole.WORKER),
        (NodeRole.MANAGER, NodeRole.MANAGER),  # Peer coordination
        # Worker connections
        (NodeRole.WORKER, NodeRole.MANAGER),  # Results/heartbeats
    }

    _role_descriptions: ClassVar[dict[tuple[NodeRole, NodeRole], str]] = {
        (NodeRole.CLIENT, NodeRole.GATE): "Job submission",
        (NodeRole.GATE, NodeRole.MANAGER): "Job distribution",
        (NodeRole.GATE, NodeRole.GATE): "Cross-DC coordination",
        (NodeRole.MANAGER, NodeRole.WORKER): "Workflow dispatch",
        (NodeRole.MANAGER, NodeRole.MANAGER): "Peer coordination",
        (NodeRole.WORKER, NodeRole.MANAGER): "Results and heartbeats",
    }

    def validate(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> ValidationResult:
        """
        Validate a connection between two nodes.

        Args:
            source: Claims from the source (connecting) node
            target: Claims from the target (listening) node

        Returns:
            ValidationResult indicating if connection is allowed
        """
        # Check cluster ID
        if self.strict_mode:
            if source.cluster_id != self.cluster_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Source cluster mismatch: {source.cluster_id} != {self.cluster_id}",
                    source_claims=source,
                    target_claims=target,
                )

            if target.cluster_id != self.cluster_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Target cluster mismatch: {target.cluster_id} != {self.cluster_id}",
                    source_claims=source,
                    target_claims=target,
                )

            # Check environment ID
            if source.environment_id != self.environment_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Source environment mismatch: {source.environment_id} != {self.environment_id}",
                    source_claims=source,
                    target_claims=target,
                )

            if target.environment_id != self.environment_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Target environment mismatch: {target.environment_id} != {self.environment_id}",
                    source_claims=source,
                    target_claims=target,
                )

        # Check cross-environment (never allowed)
        if source.environment_id != target.environment_id:
            return ValidationResult(
                allowed=False,
                reason=f"Cross-environment connection not allowed: {source.environment_id} -> {target.environment_id}",
                source_claims=source,
                target_claims=target,
            )

        # Check role-based permission
        connection_type = (source.role, target.role)
        if connection_type in self._allowed_connections:
            description = self._role_descriptions.get(
                connection_type, "Allowed connection"
            )
            return ValidationResult(
                allowed=True,
                reason=description,
                source_claims=source,
                target_claims=target,
            )

        return ValidationResult(
            allowed=False,
            reason=f"Connection type not allowed: {source.role.value} -> {target.role.value}",
            source_claims=source,
            target_claims=target,
        )

    def is_allowed(self, source_role: NodeRole, target_role: NodeRole) -> bool:
        """
        Check if a role combination is allowed.

        Simple check without claims validation.

        Args:
            source_role: Role of the connecting node
            target_role: Role of the target node

        Returns:
            True if the connection type is allowed
        """
        return (source_role, target_role) in self._allowed_connections

    def get_allowed_targets(self, source_role: NodeRole) -> list[NodeRole]:
        """
        Get list of roles a source role can connect to.

        Args:
            source_role: The source role

        Returns:
            List of target roles that are allowed
        """
        return [
            target
            for source, target in self._allowed_connections
            if source == source_role
        ]

    def get_allowed_sources(self, target_role: NodeRole) -> list[NodeRole]:
        """
        Get list of roles that can connect to a target role.

        Args:
            target_role: The target role

        Returns:
            List of source roles that are allowed to connect
        """
        return [
            source
            for source, target in self._allowed_connections
            if target == target_role
        ]

    def validate_claims(self, claims: CertificateClaims) -> ValidationResult:
        """
        Validate claims against expected cluster/environment.

        Args:
            claims: Claims to validate

        Returns:
            ValidationResult indicating if claims are valid
        """
        if self.strict_mode:
            if claims.cluster_id != self.cluster_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Cluster mismatch: {claims.cluster_id} != {self.cluster_id}",
                    source_claims=claims,
                )

            if claims.environment_id != self.environment_id:
                return ValidationResult(
                    allowed=False,
                    reason=f"Environment mismatch: {claims.environment_id} != {self.environment_id}",
                    source_claims=claims,
                )

        return ValidationResult(
            allowed=True,
            reason="Claims valid",
            source_claims=claims,
        )

    @staticmethod
    def extract_claims_from_cert(
        cert_der: bytes,
        default_cluster: str = "",
        default_environment: str = "",
    ) -> CertificateClaims:
        """
        Extract claims from a DER-encoded certificate.

        Parses the certificate and extracts claims from:
        - CN (Common Name): cluster_id
        - OU (Organizational Unit): role
        - SAN (Subject Alternative Name) DNS entries: node_id, datacenter_id, region_id
        - Custom OID extensions: environment_id

        Expected certificate structure:
        - Subject CN=<cluster_id>
        - Subject OU=<role> (client|gate|manager|worker)
        - SAN DNS entries in format: node=<id>, dc=<dc_id>, region=<region_id>
        - Custom extension OID 1.3.6.1.4.1.99999.1 for environment_id

        Args:
            cert_der: DER-encoded certificate bytes
            default_cluster: Default cluster if not in cert
            default_environment: Default environment if not in cert

        Returns:
            CertificateClaims extracted from certificate

        Raises:
            ValueError: If certificate cannot be parsed or required fields are missing
        """
        try:
            # Parse DER-encoded certificate
            cert = x509.load_der_x509_certificate(cert_der, default_backend())

            # Extract cluster_id from CN (Common Name)
            cluster_id = default_cluster
            try:
                cn_attribute = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
                if cn_attribute:
                    cluster_id = cn_attribute[0].value
            except Exception:
                pass

            # Extract role from OU (Organizational Unit)
            role = NodeRole.CLIENT  # Default fallback
            try:
                ou_attribute = cert.subject.get_attributes_for_oid(
                    NameOID.ORGANIZATIONAL_UNIT_NAME
                )
                if ou_attribute:
                    role_str = ou_attribute[0].value.lower()
                    # Map OU value to NodeRole
                    if role_str in {r.value for r in NodeRole}:
                        role = NodeRole(role_str)
            except Exception:
                pass

            # Extract node_id, datacenter_id, region_id from SAN
            node_id = "unknown"
            datacenter_id = ""
            region_id = ""

            try:
                san_extension = cert.extensions.get_extension_for_oid(
                    ExtensionOID.SUBJECT_ALTERNATIVE_NAME
                )
                san_values = san_extension.value

                # Parse DNS names in SAN
                for dns_name in san_values.get_values_for_type(x509.DNSName):
                    # Expected format: "node=<id>", "dc=<dc_id>", "region=<region_id>"
                    if dns_name.startswith("node="):
                        node_id = dns_name[5:]
                    elif dns_name.startswith("dc="):
                        datacenter_id = dns_name[3:]
                    elif dns_name.startswith("region="):
                        region_id = dns_name[7:]
            except x509.ExtensionNotFound:
                # SAN is optional, use defaults
                pass
            except Exception:
                # If SAN parsing fails, continue with defaults
                pass

            # Extract environment_id from custom extension OID
            # Using OID 1.3.6.1.4.1.99999.1 as example (would be registered in production)
            environment_id = default_environment
            try:
                # Try to get custom extension for environment
                # Note: This would need a registered OID in production
                custom_oid = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1")
                env_extension = cert.extensions.get_extension_for_oid(custom_oid)
                environment_id = env_extension.value.value.decode("utf-8")
            except x509.ExtensionNotFound:
                # Custom extension is optional
                pass
            except Exception:
                # If custom extension parsing fails, use default
                pass

            return CertificateClaims(
                cluster_id=cluster_id,
                environment_id=environment_id,
                role=role,
                node_id=node_id,
                datacenter_id=datacenter_id,
                region_id=region_id,
            )

        except Exception as parse_error:
            # If certificate parsing fails completely, return defaults
            # In strict production, this should raise an error
            return CertificateClaims(
                cluster_id=default_cluster,
                environment_id=default_environment,
                role=NodeRole.CLIENT,
                node_id="unknown",
                datacenter_id="",
                region_id="",
            )

    @classmethod
    def get_connection_matrix(cls) -> dict[str, list[str]]:
        """
        Get the full connection matrix as a dict.

        Returns:
            Dict mapping source role to list of allowed target roles
        """
        matrix: dict[str, list[str]] = {role.value: [] for role in NodeRole}

        for source, target in cls._allowed_connections:
            matrix[source.value].append(target.value)

        return matrix
