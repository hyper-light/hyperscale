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

import os
import secrets
import warnings

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

# Domain separation context for HKDF
ENCRYPTION_CONTEXT = b"hyperscale-jobs-encryption-v1"

# Known weak/default secrets that should be rejected in production
WEAK_SECRETS = frozenset([
    "hyperscale",
    "hyperscalelocal",
    "hyperscale-local",
    "hyperscale-dev-secret-change-in-prod",
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
    
    This class is pickle-compatible for use with multiprocessing.
    """
    
    # Only store the secret bytes - no unpicklable objects
    __slots__ = ('_secret_bytes',)
    
    def __init__(self, env: Env) -> None:
        # Convert secret to bytes and validate minimum length
        secret = env.MERCURY_SYNC_AUTH_SECRET
        if isinstance(secret, str):
            self._secret_bytes = secret.encode('utf-8')
        else:
            self._secret_bytes = secret
            
        # Validate secret has sufficient entropy
        if len(self._secret_bytes) < 16:
            raise ValueError(
                "MERCURY_SYNC_AUTH_SECRET must be at least 16 characters. "
                "Use a strong, random secret for production deployments."
            )
        
        # Check for weak/default secrets
        secret_lower = secret.lower() if isinstance(secret, str) else secret.decode('utf-8', errors='ignore').lower()
        is_production = os.environ.get('HYPERSCALE_ENV', '').lower() in ('production', 'prod')
        
        if secret_lower in WEAK_SECRETS:
            if is_production:
                raise ValueError(
                    f"MERCURY_SYNC_AUTH_SECRET is set to a known weak/default value. "
                    f"This is not allowed in production (HYPERSCALE_ENV=production). "
                    f"Please set a strong, random secret. "
                    f"Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
                )
            else:
                warnings.warn(
                    f"MERCURY_SYNC_AUTH_SECRET is set to a known weak/default value '{secret_lower}'. "
                    f"This is acceptable for development but will be rejected in production. "
                    f"Set HYPERSCALE_ENV=production to enforce strong secrets.",
                    UserWarning,
                    stacklevel=2
                )

    def _derive_key(self, salt: bytes) -> bytes:
        """
        Derive a unique encryption key from the shared secret and salt.
        
        Uses HKDF (HMAC-based Key Derivation Function) with SHA-256.
        The salt ensures each message gets a unique derived key.
        
        Note: default_backend() is called inline rather than stored to
        maintain pickle compatibility for multiprocessing.
        """
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=KEY_SIZE,
            salt=salt,
            info=ENCRYPTION_CONTEXT,
            backend=default_backend(),
        )
        return hkdf.derive(self._secret_bytes)

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
        
        Raises:
            EncryptionError: If decryption fails (wrong key, tampered data, etc.)
        """
        if len(data) < HEADER_SIZE + 16:  # Minimum: header + auth tag
            raise EncryptionError("Message too short to contain valid ciphertext")
        
        # Extract components
        salt = data[:SALT_SIZE]
        nonce = data[SALT_SIZE:HEADER_SIZE]
        ciphertext = data[HEADER_SIZE:]
        
        # Derive the same key from shared secret + salt
        key = self._derive_key(salt)
        
        try:
            # Decrypt and verify authentication tag
            return AESGCM(key).decrypt(nonce, ciphertext, associated_data=None)
        except Exception as e:
            # Don't leak details about why decryption failed
            raise EncryptionError("Decryption failed: invalid key or tampered data") from e

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
        
        Raises:
            EncryptionError: If decryption fails or AAD doesn't match
        """
        if len(data) < HEADER_SIZE + 16:
            raise EncryptionError("Message too short to contain valid ciphertext")
        
        salt = data[:SALT_SIZE]
        nonce = data[SALT_SIZE:HEADER_SIZE]
        ciphertext = data[HEADER_SIZE:]
        
        key = self._derive_key(salt)
        
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, associated_data=associated_data)
        except Exception as e:
            raise EncryptionError("Decryption failed: invalid key, tampered data, or AAD mismatch") from e
    
    def __getstate__(self):
        """Return state for pickling - only the secret bytes."""
        return {'_secret_bytes': self._secret_bytes}
    
    def __setstate__(self, state):
        """Restore state from pickle."""
        self._secret_bytes = state['_secret_bytes']
