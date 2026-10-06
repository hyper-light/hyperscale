"""
Secure AES-256-GCM encryption with HKDF key derivation.

Security properties:
- Key derivation: HKDF-SHA256 from shared secret + per-message salt
- Encryption: AES-256-GCM (authenticated encryption)
- Nonce: 12-byte random per message (transmitted with ciphertext)
- The encryption key is NEVER transmitted - derived from shared secret

Message format:
    [salt (16 bytes)][nonce (12 bytes)][ciphertext (variable)][auth tag (16 bytes)]
    
    - salt: Random bytes used with HKDF to derive unique key per message
    - nonce: Random bytes for AES-GCM (distinct from salt for cryptographic separation)
    - ciphertext: AES-GCM encrypted data
    - auth tag: Included in ciphertext by AESGCM (last 16 bytes)

Note: This class is pickle-compatible for multiprocessing. The cryptography
backend is obtained on-demand rather than stored as an instance attribute.
"""

import secrets

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from hyperscale.core.jobs.models.env import Env


# Constants
SALT_SIZE = 16  # bytes
NONCE_SIZE = 12  # bytes (AES-GCM standard)
KEY_SIZE = 32  # bytes (AES-256)
HEADER_SIZE = SALT_SIZE + NONCE_SIZE  # 28 bytes
MIN_SECRET_LENGTH = 16  # Minimum secret length in bytes

# Domain separation context for HKDF
ENCRYPTION_CONTEXT = b"hyperscale-jobs-encryption-v1"

# Known weak/default secrets, always refused
WEAK_SECRETS = frozenset([
    "hyperscale",
    "hyperscalelocal",
    "hyperscale-local",
    "hyperscale-secret",
    "hyperscale-local-dev-secret",
    "secret",
    "password",
    "changeme",
    "default",
    "test",
    "testing",
    "development",
    "dev",
])


MISSING_SECRET_MESSAGE = (
    "MERCURY_SYNC_AUTH_SECRET is not set. Every node of a cluster must share one strong, "
    "random secret: set the MERCURY_SYNC_AUTH_SECRET environment variable or pass --acm-secret "
    "(generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\")."
)


def strong_secret_bytes(setting_name: str, secret: str) -> bytes:
    """The UTF-8 bytes of ``secret``, refused when it is too short or a
    known weak/default value -- whatever ``HYPERSCALE_ENV`` says, since the
    secret is the only thing standing between the network and code
    execution on every worker.

    Raises:
        ValueError: ``secret`` is shorter than ``MIN_SECRET_LENGTH`` bytes
            or is one of ``WEAK_SECRETS``.
    """
    secret_bytes = secret.encode("utf-8")
    if len(secret_bytes) < MIN_SECRET_LENGTH:
        raise ValueError(
            f"{setting_name} must be at least {MIN_SECRET_LENGTH} characters. "
            "Use a strong, random secret (python -c \"import secrets; print(secrets.token_urlsafe(32))\")."
        )

    if secret.strip().lower() in WEAK_SECRETS:
        raise ValueError(
            f"{setting_name} is set to a known weak/default value and is refused. "
            "Use a strong, random secret (python -c \"import secrets; print(secrets.token_urlsafe(32))\")."
        )

    return secret_bytes


class EncryptionError(Exception):
    """Raised when encryption or decryption fails."""
    pass


class AESGCMFernet:
    """
    AES-256-GCM encryption with HKDF key derivation from shared secret.
    
    The shared secret (MERCURY_SYNC_AUTH_SECRET) is used as the input keying
    material for HKDF. Each message uses a random salt to derive a unique
    encryption key, ensuring that:
    
    1. The encryption key is NEVER transmitted
    2. Each message uses a different derived key (via unique salt)
    3. Compromise of one message's key doesn't compromise others
    4. Both endpoints must know the shared secret to communicate
    
    Key Rotation:
    - Set MERCURY_SYNC_AUTH_SECRET to the new secret
    - Set MERCURY_SYNC_AUTH_SECRET_PREVIOUS to the old secret
    - Encryption uses the new secret
    - Decryption tries new secret first, then falls back to old
    - Once all nodes have rotated, remove MERCURY_SYNC_AUTH_SECRET_PREVIOUS
    
    This class is pickle-compatible for use with multiprocessing.
    """
    
    # Store primary and fallback secrets for key rotation
    __slots__ = ('_secret_bytes', '_fallback_secret_bytes')
    
    def __init__(self, env: Env) -> None:
        secret = env.MERCURY_SYNC_AUTH_SECRET
        if secret is None:
            raise ValueError(MISSING_SECRET_MESSAGE)

        self._secret_bytes = strong_secret_bytes("MERCURY_SYNC_AUTH_SECRET", secret)

        # Key rotation: the previous secret decrypts what peers not yet
        # rotated still send, so it is held to the same standard.
        previous_secret = env.MERCURY_SYNC_AUTH_SECRET_PREVIOUS
        self._fallback_secret_bytes: bytes | None = (
            strong_secret_bytes("MERCURY_SYNC_AUTH_SECRET_PREVIOUS", previous_secret)
            if previous_secret
            else None
        )

    def _derive_key(self, salt: bytes, secret_bytes: bytes | None = None) -> bytes:
        """
        Derive a unique encryption key from a secret and salt.
        
        Uses HKDF (HMAC-based Key Derivation Function) with SHA-256.
        The salt ensures each message gets a unique derived key.
        
        Args:
            salt: Random salt for key derivation
            secret_bytes: Secret to use (defaults to primary secret)
        
        Note: default_backend() is called inline rather than stored to
        maintain pickle compatibility for multiprocessing.
        """
        if secret_bytes is None:
            secret_bytes = self._secret_bytes
            
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=KEY_SIZE,
            salt=salt,
            info=ENCRYPTION_CONTEXT,
            backend=default_backend(),
        )
        return hkdf.derive(secret_bytes)

    def encrypt(self, data: bytes) -> bytes:
        """
        Encrypt data using AES-256-GCM with a derived key.
        
        Returns: salt (16B) || nonce (12B) || ciphertext+tag
        
        The encryption key is derived from:
            key = HKDF(shared_secret, salt, context)
        
        This ensures:
        - Different key per message (due to random salt)
        - Key is never transmitted (only salt is public)
        - Both sides can derive the same key from shared secret
        """
        # Generate random salt and nonce
        salt = secrets.token_bytes(SALT_SIZE)
        nonce = secrets.token_bytes(NONCE_SIZE)
        
        # Derive encryption key from shared secret + salt
        key = self._derive_key(salt)
        
        # Encrypt with AES-256-GCM (includes authentication tag)
        ciphertext = AESGCM(key).encrypt(nonce, data, associated_data=None)
        
        # Return: salt || nonce || ciphertext (includes auth tag)
        return salt + nonce + ciphertext

    def decrypt(self, data: bytes) -> bytes:
        """
        Decrypt data encrypted with encrypt().
        
        Expects: salt (16B) || nonce (12B) || ciphertext+tag
        
        Derives the same key using HKDF(shared_secret, salt, context)
        and decrypts. The auth tag is verified by AESGCM.
        
        Key Rotation: If decryption with the primary secret fails and a
        fallback secret is configured, tries the fallback secret. This
        enables seamless key rotation without downtime.
        
        Raises:
            EncryptionError: If decryption fails with all available keys
        """
        if len(data) < HEADER_SIZE + 16:  # Minimum: header + auth tag
            raise EncryptionError("Message too short to contain valid ciphertext")
        
        # Extract components
        salt = data[:SALT_SIZE]
        nonce = data[SALT_SIZE:HEADER_SIZE]
        ciphertext = data[HEADER_SIZE:]
        
        # Try primary secret first
        key = self._derive_key(salt, self._secret_bytes)
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, associated_data=None)
        except Exception:
            pass  # Try fallback if available
        
        # Try fallback secret if configured (key rotation scenario)
        if self._fallback_secret_bytes:
            key = self._derive_key(salt, self._fallback_secret_bytes)
            try:
                return AESGCM(key).decrypt(nonce, ciphertext, associated_data=None)
            except Exception:
                pass
        
        # All secrets failed
        raise EncryptionError("Decryption failed: invalid key or tampered data")

    def encrypt_with_aad(self, data: bytes, associated_data: bytes) -> bytes:
        """
        Encrypt with Additional Authenticated Data (AAD).
        
        AAD is authenticated but not encrypted. Useful for including
        metadata (like message type) that must be readable but tamper-proof.
        
        Returns: salt (16B) || nonce (12B) || ciphertext+tag
        """
        salt = secrets.token_bytes(SALT_SIZE)
        nonce = secrets.token_bytes(NONCE_SIZE)
        key = self._derive_key(salt)
        
        ciphertext = AESGCM(key).encrypt(nonce, data, associated_data=associated_data)
        return salt + nonce + ciphertext

    def decrypt_with_aad(self, data: bytes, associated_data: bytes) -> bytes:
        """
        Decrypt data encrypted with encrypt_with_aad().
        
        The same associated_data must be provided for authentication.
        Supports key rotation like decrypt().
        
        Raises:
            EncryptionError: If decryption fails or AAD doesn't match
        """
        if len(data) < HEADER_SIZE + 16:
            raise EncryptionError("Message too short to contain valid ciphertext")
        
        salt = data[:SALT_SIZE]
        nonce = data[SALT_SIZE:HEADER_SIZE]
        ciphertext = data[HEADER_SIZE:]
        
        # Try primary secret first
        key = self._derive_key(salt, self._secret_bytes)
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, associated_data=associated_data)
        except Exception:
            pass
        
        # Try fallback secret if configured
        if self._fallback_secret_bytes:
            key = self._derive_key(salt, self._fallback_secret_bytes)
            try:
                return AESGCM(key).decrypt(nonce, ciphertext, associated_data=associated_data)
            except Exception:
                pass
        
        raise EncryptionError("Decryption failed: invalid key, tampered data, or AAD mismatch")
    
    def __getstate__(self):
        """Return state for pickling - includes both secrets for key rotation."""
        return {
            '_secret_bytes': self._secret_bytes,
            '_fallback_secret_bytes': self._fallback_secret_bytes,
        }
    
    def __setstate__(self, state):
        """Restore state from pickle."""
        self._secret_bytes = state['_secret_bytes']
        self._fallback_secret_bytes = state.get('_fallback_secret_bytes')
