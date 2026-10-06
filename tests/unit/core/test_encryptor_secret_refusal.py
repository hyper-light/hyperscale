"""
Both frame encryptors refuse a missing, short or known-weak cluster secret
-- and a weak previous (rotation) secret -- whatever ``HYPERSCALE_ENV``
says. Every frame is authenticated with a key derived from this secret and
workflows travel as by-value code, so a published default would let
anyone run code on an unconfigured cluster.
"""

from typing import Callable

import pytest

from hyperscale.core.jobs.models import Env as CoreEnv
from hyperscale.core.jobs.protocols.encryption import AESGCMFernet as CoreFernet
from hyperscale.core.jobs.protocols.encryption import WEAK_SECRETS as CORE_WEAK_SECRETS
from hyperscale.distributed.encryption.aes_gcm import AESGCMFernet as DistributedFernet
from hyperscale.distributed.encryption.aesgcm_fernet import WEAK_SECRETS as DISTRIBUTED_WEAK_SECRETS
from hyperscale.distributed.env import Env as DistributedEnv

STRONG_SECRET = "encryptor-refusal-strong-secret-0123456789"
PREVIOUS_STRONG_SECRET = "encryptor-refusal-previous-secret-0123456789"
DEPLOYMENT_ENVIRONMENT_ENVAR = "HYPERSCALE_ENV"

EncryptorFactory = Callable[[str | None, str | None], object]


def build_core_encryptor(secret: str | None, previous_secret: str | None) -> object:
    return CoreFernet(
        CoreEnv(MERCURY_SYNC_AUTH_SECRET=secret, MERCURY_SYNC_AUTH_SECRET_PREVIOUS=previous_secret)
    )


def build_distributed_encryptor(secret: str | None, previous_secret: str | None) -> object:
    return DistributedFernet(
        DistributedEnv(MERCURY_SYNC_AUTH_SECRET=secret, MERCURY_SYNC_AUTH_SECRET_PREVIOUS=previous_secret)
    )


ENCRYPTORS: list[tuple[str, EncryptorFactory, frozenset[str]]] = [
    ("core", build_core_encryptor, CORE_WEAK_SECRETS),
    ("distributed", build_distributed_encryptor, DISTRIBUTED_WEAK_SECRETS),
]

WEAK_SECRET_CASES = [
    pytest.param(factory, weak_secret, id=f"{encryptor_name}-{weak_secret}")
    for encryptor_name, factory, weak_secrets in ENCRYPTORS
    for weak_secret in sorted(weak_secrets)
]

FACTORIES = [pytest.param(factory, id=encryptor_name) for encryptor_name, factory, _ in ENCRYPTORS]


@pytest.fixture(autouse=True)
def no_deployment_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The refusal holds without HYPERSCALE_ENV set."""
    monkeypatch.delenv(DEPLOYMENT_ENVIRONMENT_ENVAR, raising=False)


@pytest.mark.parametrize(("factory", "weak_secret"), WEAK_SECRET_CASES)
def test_every_weak_secret_is_refused(factory: EncryptorFactory, weak_secret: str) -> None:
    with pytest.raises(ValueError, match="MERCURY_SYNC_AUTH_SECRET"):
        factory(weak_secret, None)


@pytest.mark.parametrize(("factory", "weak_secret"), WEAK_SECRET_CASES)
def test_every_weak_secret_is_refused_in_any_case_or_padding(factory: EncryptorFactory, weak_secret: str) -> None:
    with pytest.raises(ValueError):
        factory(f"  {weak_secret.upper()}  ", None)


@pytest.mark.parametrize(("factory", "weak_secret"), WEAK_SECRET_CASES)
def test_every_weak_previous_secret_is_refused(factory: EncryptorFactory, weak_secret: str) -> None:
    with pytest.raises(ValueError, match="MERCURY_SYNC_AUTH_SECRET_PREVIOUS"):
        factory(STRONG_SECRET, weak_secret)


@pytest.mark.parametrize("factory", FACTORIES)
@pytest.mark.parametrize("deployment_environment", ["development", "dev", "test", "staging"])
def test_the_published_default_is_refused_in_every_deployment_environment(
    factory: EncryptorFactory,
    deployment_environment: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DEPLOYMENT_ENVIRONMENT_ENVAR, deployment_environment)

    with pytest.raises(ValueError, match="weak/default"):
        factory("hyperscale-secret", None)


@pytest.mark.parametrize("factory", FACTORIES)
def test_a_missing_secret_is_refused_naming_both_ways_to_configure_it(factory: EncryptorFactory) -> None:
    with pytest.raises(ValueError) as refusal:
        factory(None, None)

    assert "MERCURY_SYNC_AUTH_SECRET" in str(refusal.value)
    assert "--acm-secret" in str(refusal.value)


@pytest.mark.parametrize("factory", FACTORIES)
def test_a_short_secret_is_refused(factory: EncryptorFactory) -> None:
    with pytest.raises(ValueError, match="at least 16 characters"):
        factory("short-secret", None)


@pytest.mark.parametrize("factory", FACTORIES)
def test_a_short_previous_secret_is_refused(factory: EncryptorFactory) -> None:
    with pytest.raises(ValueError, match="MERCURY_SYNC_AUTH_SECRET_PREVIOUS"):
        factory(STRONG_SECRET, "short-secret")


@pytest.mark.parametrize("factory", FACTORIES)
def test_strong_secrets_are_accepted_and_rotation_decrypts(factory: EncryptorFactory) -> None:
    old_encryptor = factory(PREVIOUS_STRONG_SECRET, None)
    rotated_encryptor = factory(STRONG_SECRET, PREVIOUS_STRONG_SECRET)

    frame = old_encryptor.encrypt(b"rotation-frame")

    assert rotated_encryptor.decrypt(frame) == b"rotation-frame"


def test_neither_env_has_a_published_default() -> None:
    assert CoreEnv().MERCURY_SYNC_AUTH_SECRET is None
    assert DistributedEnv().MERCURY_SYNC_AUTH_SECRET is None
