"""
Secure AES-256-GCM encryption with HKDF key derivation.

Security properties:
- Key derivation: HKDF-SHA256 from shared secret + per-message salt
- Encryption: AES-256-GCM (authenticated encryption)
- Nonce: 12-byte random per message (transmitted with ciphertext)
- The encryption key is NEVER transmitted - derived from shared secret
- Weak/default secrets rejected in production
- Key rotation support via fallback secret

Message format:
    [salt (16 bytes)][nonce (12 bytes)][ciphertext (variable)][auth tag (16 bytes)]
    
    - salt: Random bytes used with HKDF to derive unique key per message
    - nonce: Random bytes for AES-GCM (distinct from salt for cryptographic separation)
    - ciphertext: AES-GCM encrypted data
    - auth tag: Included in ciphertext by AESGCM (last 16 bytes)

Note: This class is pickle-compatible for multiprocessing. The cryptography
backend is obtained on-demand rather than stored as an instance attribute.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import os
import secrets
import warnings
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from hyperscale.distributed.env import Env

from .aesgcm_fernet import SALT_SIZE
from .aesgcm_fernet import NONCE_SIZE
from .aesgcm_fernet import KEY_SIZE
from .aesgcm_fernet import HEADER_SIZE
from .aesgcm_fernet import MIN_SECRET_LENGTH
from .aesgcm_fernet import ENCRYPTION_CONTEXT
from .aesgcm_fernet import WEAK_SECRETS
from .aesgcm_fernet import AESGCMFernet
from .encryption_error import EncryptionError

_REHOMED = (
    EncryptionError,
    AESGCMFernet,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
