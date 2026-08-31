"""
mTLS strict mode actually rejects unparseable certificates (FIX.md
1.1, AD-28, scenario 41.23).

``RoleValidator.extract_claims_from_cert`` is a configuration-blind
staticmethod with a permissive ``strict=False`` default, so every
production call site had to remember to thread the node's
``mtls_strict_mode`` flag through — and none did. The failure
composes viciously: on any parse failure the claims fall back to
``default_cluster``/``default_environment``, which the call sites set
to the node's OWN identity, and ``validate_claims``' strict check is
cluster/environment equality. A garbage certificate therefore parsed
to a perfectly-configured CLIENT and passed validation on the very
node that had strict mode enabled.

The fix moves the parse onto the validator instance —
``extract_peer_claims`` — which reads strictness and default identity
from the configuration the node was constructed with, so a call site
can no longer disagree with it. These tests pin the trap (as
documentation of why the static default is never enough), the strict
rejection, the preserved non-strict behavior, and the identity
threading.
"""

from __future__ import annotations

import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID

from hyperscale.distributed.discovery.security.role_validator import (
    CertificateParseError,
    NodeRole,
    RoleValidator,
)

CLUSTER_ID = "prod-cluster-1"
ENVIRONMENT_ID = "prod"

GARBAGE_DER = b"this is not a certificate"


def _build_cert_der(
    common_name: str | None,
    organizational_unit: str | None,
    san_entries: tuple[str, ...] = (),
) -> bytes:
    """Self-signed DER cert with exactly the claims fields the
    validator reads: CN=cluster, OU=role, SAN DNS ``node=``/``dc=``/
    ``region=`` entries."""
    private_key = ec.generate_private_key(ec.SECP256R1())

    name_attributes = []
    if common_name is not None:
        name_attributes.append(x509.NameAttribute(NameOID.COMMON_NAME, common_name))
    if organizational_unit is not None:
        name_attributes.append(
            x509.NameAttribute(
                NameOID.ORGANIZATIONAL_UNIT_NAME, organizational_unit
            )
        )
    subject = x509.Name(name_attributes)

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2026, 1, 1))
        .not_valid_after(datetime.datetime(2036, 1, 1))
    )
    if san_entries:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(entry) for entry in san_entries]
            ),
            critical=False,
        )

    return builder.sign(private_key, hashes.SHA256()).public_bytes(Encoding.DER)


def _validator(strict_mode: bool) -> RoleValidator:
    return RoleValidator(
        cluster_id=CLUSTER_ID,
        environment_id=ENVIRONMENT_ID,
        strict_mode=strict_mode,
    )


def test_the_trap_the_fix_exists_for() -> None:
    """Documents the pre-fix mechanism end to end: the static parser
    without ``strict=`` turns garbage into claims that carry this
    node's own cluster/environment — which is exactly what strict-mode
    ``validate_claims`` checks, so the garbage cert was ALLOWED.

    This is the behavior every call site got by forgetting one kwarg.
    If this test ever fails, the static default changed and the
    instance method's rationale should be revisited.
    """
    strict_validator = _validator(strict_mode=True)

    defaulted_claims = RoleValidator.extract_claims_from_cert(
        GARBAGE_DER,
        default_cluster=CLUSTER_ID,
        default_environment=ENVIRONMENT_ID,
    )

    assert defaulted_claims.cluster_id == CLUSTER_ID
    assert defaulted_claims.environment_id == ENVIRONMENT_ID
    assert defaulted_claims.role == NodeRole.CLIENT
    assert strict_validator.validate_claims(defaulted_claims).allowed is True, (
        "the trap closed some other way — the defaulted claims no longer "
        "pass strict validation"
    )


def test_strict_validator_rejects_garbage() -> None:
    """The fix: the same garbage against the instance method on a
    strict validator raises instead of authenticating."""
    strict_validator = _validator(strict_mode=True)

    with pytest.raises(CertificateParseError):
        strict_validator.extract_peer_claims(GARBAGE_DER)


def test_strict_validator_rejects_a_cert_missing_required_claims() -> None:
    """Strictness is not only about undecodable bytes: a REAL x509
    certificate that lacks the role claim (OU) must also be refused —
    otherwise any valid-but-unrelated certificate authenticates as
    CLIENT."""
    strict_validator = _validator(strict_mode=True)
    cert_missing_role = _build_cert_der(
        common_name=CLUSTER_ID, organizational_unit=None
    )

    with pytest.raises(CertificateParseError) as raised:
        strict_validator.extract_peer_claims(cert_missing_role)

    assert "role" in str(raised.value)


def test_strict_validator_parses_a_well_formed_cert() -> None:
    """The other direction: strict mode must not reject the
    certificates the deployment guide prescribes."""
    strict_validator = _validator(strict_mode=True)
    cert_der = _build_cert_der(
        common_name=CLUSTER_ID,
        organizational_unit="worker",
        san_entries=("node=worker-1", "dc=dc-east", "region=us-east"),
    )

    claims = strict_validator.extract_peer_claims(cert_der)

    assert claims.cluster_id == CLUSTER_ID
    assert claims.environment_id == ENVIRONMENT_ID
    assert claims.role == NodeRole.WORKER
    assert claims.node_id == "worker-1"
    assert claims.datacenter_id == "dc-east"
    assert claims.region_id == "us-east"
    assert strict_validator.validate_claims(claims).allowed is True


def test_non_strict_validator_keeps_the_permissive_fallback() -> None:
    """Backward compatibility for deployments that run without strict
    mode (the shipped default): garbage still parses to defaulted
    claims rather than raising."""
    permissive_validator = _validator(strict_mode=False)

    claims = permissive_validator.extract_peer_claims(GARBAGE_DER)

    assert claims.cluster_id == CLUSTER_ID
    assert claims.environment_id == ENVIRONMENT_ID
    assert claims.role == NodeRole.CLIENT


def test_instance_method_threads_the_validator_identity() -> None:
    """The instance method's defaults are the validator's OWN identity
    — a cert that omits the environment extension inherits the
    configured environment, not an empty string."""
    permissive_validator = _validator(strict_mode=False)
    cert_der = _build_cert_der(
        common_name="some-other-cluster", organizational_unit="manager"
    )

    claims = permissive_validator.extract_peer_claims(cert_der)

    assert claims.cluster_id == "some-other-cluster", "CN must win over defaults"
    assert claims.environment_id == ENVIRONMENT_ID, (
        "missing environment extension must inherit the validator's "
        "configured environment"
    )
    assert claims.role == NodeRole.MANAGER
