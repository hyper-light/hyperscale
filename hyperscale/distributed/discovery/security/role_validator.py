"""
Role-based certificate validation for mTLS.

Enforces the node communication matrix based on certificate claims.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass
from typing import ClassVar
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509.oid import NameOID, ExtensionOID
from hyperscale.distributed.models.distributed import NodeRole

from .certificate_claims import CertificateClaims
from .certificate_parse_error import CertificateParseError
from .role_validation_error import RoleValidationError
from .validation_result import ValidationResult


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
        (NodeRole.GATE, NodeRole.CLIENT),  # Status/result push
        # Manager connections
        (NodeRole.MANAGER, NodeRole.WORKER),
        (NodeRole.MANAGER, NodeRole.MANAGER),  # Peer coordination
        (NodeRole.MANAGER, NodeRole.GATE),  # Registration/heartbeats/results
        (NodeRole.MANAGER, NodeRole.CLIENT),  # Status/result push
        # Worker connections
        (NodeRole.WORKER, NodeRole.MANAGER),  # Results/heartbeats
    }

    # Mirrors the AD-28 connection matrix (docs/architecture/AD_28.md).
    _role_descriptions: ClassVar[dict[tuple[NodeRole, NodeRole], str]] = {
        (NodeRole.CLIENT, NodeRole.GATE): "Job submission",
        (NodeRole.GATE, NodeRole.MANAGER): "Job distribution",
        (NodeRole.GATE, NodeRole.GATE): "Cross-DC coordination",
        (NodeRole.GATE, NodeRole.CLIENT): "Status and result push",
        (NodeRole.MANAGER, NodeRole.WORKER): "Workflow dispatch",
        (NodeRole.MANAGER, NodeRole.MANAGER): "Peer coordination",
        (NodeRole.MANAGER, NodeRole.GATE): "Registration, heartbeats and results",
        (NodeRole.MANAGER, NodeRole.CLIENT): "Status and result push",
        (NodeRole.WORKER, NodeRole.MANAGER): "Results and heartbeats",
    }

    # The role names an OU may carry (role-claim parsing below).
    _node_role_values: ClassVar[frozenset[str]] = frozenset(role.value for role in NodeRole)

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
        if (rejection_reason := self._connection_rejection_reason(source, target)) is not None:
            return ValidationResult(
                allowed=False,
                reason=rejection_reason,
                source_claims=source,
                target_claims=target,
            )

        return self._role_permission_result(source, target)

    def _connection_rejection_reason(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> str | None:
        """Why the identity checks reject this connection, in check order:
        strict cluster/environment match, then cross-environment (AD-28)."""
        if (strict_reason := self._strict_mismatch_reason(source, target)) is not None:
            return strict_reason

        # Check cross-environment (never allowed)
        if source.environment_id != target.environment_id:
            return f"Cross-environment connection not allowed: {source.environment_id} -> {target.environment_id}"

        return None

    def _strict_mismatch_reason(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> str | None:
        """In strict mode, the first cluster then environment mismatch of
        either side against this validator's identity."""
        if not self.strict_mode:
            return None
        return self._cluster_mismatch_reason(source, target) or self._environment_mismatch_reason(source, target)

    def _cluster_mismatch_reason(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> str | None:
        """The source-then-target cluster ID mismatch, if any."""
        # Check cluster ID
        if source.cluster_id != self.cluster_id:
            return f"Source cluster mismatch: {source.cluster_id} != {self.cluster_id}"

        if target.cluster_id != self.cluster_id:
            return f"Target cluster mismatch: {target.cluster_id} != {self.cluster_id}"

        return None

    def _environment_mismatch_reason(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> str | None:
        """The source-then-target environment ID mismatch, if any."""
        # Check environment ID
        if source.environment_id != self.environment_id:
            return f"Source environment mismatch: {source.environment_id} != {self.environment_id}"

        if target.environment_id != self.environment_id:
            return f"Target environment mismatch: {target.environment_id} != {self.environment_id}"

        return None

    def _role_permission_result(
        self,
        source: CertificateClaims,
        target: CertificateClaims,
    ) -> ValidationResult:
        """The AD-28 connection-matrix verdict for the two claims' roles."""
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
        if self.strict_mode and (mismatch_reason := self._claims_mismatch_reason(claims)) is not None:
            return ValidationResult(
                allowed=False,
                reason=mismatch_reason,
                source_claims=claims,
            )

        return ValidationResult(
            allowed=True,
            reason="Claims valid",
            source_claims=claims,
        )

    def _claims_mismatch_reason(self, claims: CertificateClaims) -> str | None:
        """The cluster-then-environment mismatch of ``claims``, if any."""
        if claims.cluster_id != self.cluster_id:
            return f"Cluster mismatch: {claims.cluster_id} != {self.cluster_id}"

        if claims.environment_id != self.environment_id:
            return f"Environment mismatch: {claims.environment_id} != {self.environment_id}"

        return None

    def extract_peer_claims(self, cert_der: bytes) -> CertificateClaims:
        """Parse a peer certificate under THIS validator's configured
        strictness and identity.

        The static parser below is configuration-blind: its permissive
        ``strict=False`` default meant every production call site had
        to remember to thread the node's strict flag through — and none
        did, so with ``mtls_strict_mode`` enabled a garbage certificate
        still fell back to defaults. The defaults are this node's OWN
        cluster and environment ids, so the defaulted claims passed
        ``validate_claims`` — an unparseable certificate authenticated
        as a well-configured CLIENT (FIX.md 1.1, scenario 41.23).

        Strictness and the default identity are init-state
        configuration; both nodes already construct their validator
        with ``strict_mode`` wired from config. Routing the parse
        through the instance makes it impossible for a call site to
        disagree with that configuration.

        Raises:
            CertificateParseError: In strict mode, when the certificate
                cannot be parsed or required claims are missing. Callers
                treat this as a validation failure and reject the peer.
        """
        return self.extract_claims_from_cert(
            cert_der,
            default_cluster=self.cluster_id,
            default_environment=self.environment_id,
            strict=self.strict_mode,
        )

    @staticmethod
    def extract_claims_from_cert(
        cert_der: bytes,
        default_cluster: str = "",
        default_environment: str = "",
        strict: bool = False,
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
            strict: If True, raise CertificateParseError on parse failures instead of returning defaults

        Returns:
            CertificateClaims extracted from certificate

        Raises:
            CertificateParseError: If strict=True and certificate cannot be parsed or required fields missing
        """
        parse_errors: list[str] = []

        try:
            cert = x509.load_der_x509_certificate(cert_der, default_backend())

            cluster_id = RoleValidator._read_cluster_id(cert, default_cluster, strict, parse_errors)

            role = RoleValidator._read_role(cert, strict, parse_errors)

            node_id = "unknown"
            datacenter_id = ""
            region_id = ""

            try:
                node_id, datacenter_id, region_id = RoleValidator._identifiers_from_subject_alternative_names(cert)
            except x509.ExtensionNotFound:
                pass
            except Exception as san_error:
                parse_errors.append(f"Failed to parse SAN: {san_error}")

            environment_id = default_environment
            try:
                environment_id = RoleValidator._environment_id_from_extension(cert)
            except x509.ExtensionNotFound:
                pass
            except Exception as env_error:
                parse_errors.append(
                    f"Failed to parse environment extension: {env_error}"
                )

            RoleValidator._raise_on_strict_parse_errors(strict, parse_errors)

            return CertificateClaims(
                cluster_id=cluster_id,
                environment_id=environment_id,
                role=role,
                node_id=node_id,
                datacenter_id=datacenter_id,
                region_id=region_id,
            )

        except CertificateParseError:
            raise
        except Exception as parse_error:
            return RoleValidator._unparseable_certificate_claims(
                parse_error,
                default_cluster,
                default_environment,
                strict,
            )

    @staticmethod
    def _read_cluster_id(
        cert: x509.Certificate,
        default_cluster: str,
        strict: bool,
        parse_errors: list[str],
    ) -> str:
        """The cluster ID from the subject CN, else ``default_cluster``; a
        failed read is recorded in ``parse_errors``."""
        try:
            cn_attribute = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            return RoleValidator._cluster_id_from_common_name(cn_attribute, default_cluster, strict, parse_errors)
        except Exception as cn_error:
            parse_errors.append(f"Failed to extract CN: {cn_error}")
            return default_cluster

    @staticmethod
    def _cluster_id_from_common_name(
        cn_attribute: list[x509.NameAttribute],
        default_cluster: str,
        strict: bool,
        parse_errors: list[str],
    ) -> str:
        """The first CN value; when absent, ``default_cluster`` (recorded as an error when strict)."""
        if cn_attribute:
            return str(cn_attribute[0].value)
        if strict:
            parse_errors.append("CN (cluster_id) not found in certificate")
        return default_cluster

    @staticmethod
    def _read_role(
        cert: x509.Certificate,
        strict: bool,
        parse_errors: list[str],
    ) -> NodeRole:
        """The role from the subject OU, else ``NodeRole.CLIENT``; a failed
        read is recorded in ``parse_errors``."""
        role: NodeRole | None = None
        try:
            ou_attribute = cert.subject.get_attributes_for_oid(
                NameOID.ORGANIZATIONAL_UNIT_NAME
            )
            role = RoleValidator._role_from_organizational_unit(ou_attribute, strict, parse_errors)
        except Exception as ou_error:
            parse_errors.append(f"Failed to extract OU: {ou_error}")

        return NodeRole.CLIENT if role is None else role

    @staticmethod
    def _role_from_organizational_unit(
        ou_attribute: list[x509.NameAttribute],
        strict: bool,
        parse_errors: list[str],
    ) -> NodeRole | None:
        """The role named by the first OU value; None when absent or not a
        role (each recorded as an error when strict)."""
        if not ou_attribute:
            RoleValidator._record_strict_parse_error(strict, parse_errors, "OU (role) not found in certificate")
            return None

        role_str = str(ou_attribute[0].value).lower()
        if role_str in RoleValidator._node_role_values:
            return NodeRole(role_str)

        RoleValidator._record_strict_parse_error(strict, parse_errors, f"Invalid role in OU: {role_str}")
        return None

    @staticmethod
    def _record_strict_parse_error(strict: bool, parse_errors: list[str], message: str) -> None:
        """Record a missing or invalid claim -- an error only under strict parsing."""
        if strict:
            parse_errors.append(message)

    @staticmethod
    def _identifiers_from_subject_alternative_names(cert: x509.Certificate) -> tuple[str, str, str]:
        """``(node_id, datacenter_id, region_id)`` from the SAN DNS entries.

        Raises:
            x509.ExtensionNotFound: the certificate has no SAN extension.
        """
        san_extension = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        )
        san_values = san_extension.value

        return RoleValidator._identifiers_from_dns_names(san_values.get_values_for_type(x509.DNSName))

    @staticmethod
    def _identifiers_from_dns_names(dns_names: list[str]) -> tuple[str, str, str]:
        """``(node_id, datacenter_id, region_id)`` from ``node=``/``dc=``/``region=``
        SAN entries; the last entry of each prefix wins."""
        identifiers = {"node=": "unknown", "dc=": "", "region=": ""}

        for dns_name in dns_names:
            # The prefix through the first "=" ("" when there is none); the
            # three prefixes are disjoint, so at most one ever matches.
            prefix = dns_name[: dns_name.find("=") + 1]
            if prefix in identifiers:
                identifiers[prefix] = dns_name[len(prefix):]

        return (identifiers["node="], identifiers["dc="], identifiers["region="])

    @staticmethod
    def _environment_id_from_extension(cert: x509.Certificate) -> str:
        """The environment ID carried in the custom OID 1.3.6.1.4.1.99999.1 extension.

        Raises:
            x509.ExtensionNotFound: the certificate has no such extension.
        """
        custom_oid = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1")
        env_extension = cert.extensions.get_extension_for_oid(custom_oid)
        return env_extension.value.value.decode("utf-8")

    @staticmethod
    def _raise_on_strict_parse_errors(strict: bool, parse_errors: list[str]) -> None:
        """Under strict parsing, refuse a certificate any claim failed to parse from.

        Raises:
            CertificateParseError: ``strict`` and ``parse_errors`` is non-empty.
        """
        if strict and parse_errors:
            raise CertificateParseError(
                f"Certificate parse errors: {'; '.join(parse_errors)}"
            )

    @staticmethod
    def _unparseable_certificate_claims(
        parse_error: Exception,
        default_cluster: str,
        default_environment: str,
        strict: bool,
    ) -> CertificateClaims:
        """Default CLIENT claims for an unparseable certificate (FIX.md 1.1).

        Raises:
            CertificateParseError: ``strict`` -- an unparseable certificate is rejected.
        """
        if strict:
            raise CertificateParseError(
                f"Failed to parse certificate: {parse_error}",
                parse_error=parse_error,
            )
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

_REHOMED = (
    RoleValidationError,
    CertificateParseError,
    CertificateClaims,
    ValidationResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
