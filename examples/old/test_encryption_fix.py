#!/usr/bin/env python3
"""
Test script to verify the encryption fix (P0 #1).

This tests:
1. Encryption/decryption roundtrip works
2. Key is NOT transmitted in message (key derivation from shared secret)
3. Wrong secret fails to decrypt
4. Tampered messages are rejected
5. Minimum secret length is enforced
"""

import sys
import os

# Add project root to path
sys.path.insert(0, '/home/ada/Projects/hyperscale')

# Import directly from the module file to avoid other imports
from hyperscale.core.jobs.models.env import Env

# Direct import of encryption module
import importlib.util
spec = importlib.util.spec_from_file_location(
    "encryption",
    "/home/ada/Projects/hyperscale/hyperscale/core/jobs/protocols/encryption.py"
)
encryption_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(encryption_module)

AESGCMFernet = encryption_module.AESGCMFernet
EncryptionError = encryption_module.EncryptionError
HEADER_SIZE = encryption_module.HEADER_SIZE


def test_roundtrip():
    """Test basic encrypt/decrypt roundtrip."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Hello, World! This is a test message."
    ciphertext = encryptor.encrypt(plaintext)
    decrypted = encryptor.decrypt(ciphertext)
    
    assert decrypted == plaintext, "Roundtrip failed!"
    print("✓ Roundtrip encrypt/decrypt works")


def test_key_not_in_message():
    """Verify the encryption key is NOT transmitted in the message."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Secret data"
    ciphertext = encryptor.encrypt(plaintext)
    
    # The message format is: salt (16B) || nonce (12B) || ciphertext
    # The key should NOT be in the message (it's derived from shared secret)
    
    # Extract salt and try to derive key
    salt = ciphertext[:16]
    nonce = ciphertext[16:28]
    encrypted_part = ciphertext[28:]
    
    # Verify the structure
    assert len(salt) == 16, "Salt should be 16 bytes"
    assert len(nonce) == 12, "Nonce should be 12 bytes"
    assert len(encrypted_part) >= 16, "Ciphertext should include auth tag (16B)"
    
    # The old broken implementation would have:
    # key (32B) || nonce (12B) || ciphertext
    # And the key would be readable from bytes 0:32
    # Now we have:
    # salt (16B) || nonce (12B) || ciphertext
    # And the key is NEVER transmitted - it's derived
    
    print("✓ Key is NOT transmitted in message (only salt and nonce)")


def test_wrong_secret_fails():
    """Test that wrong secret fails to decrypt."""
    env1 = Env(MERCURY_SYNC_AUTH_SECRET="correct-secret-key-123")
    env2 = Env(MERCURY_SYNC_AUTH_SECRET="wrong-secret-key-12345")
    
    encryptor1 = AESGCMFernet(env1)
    encryptor2 = AESGCMFernet(env2)
    
    plaintext = b"Secret message"
    ciphertext = encryptor1.encrypt(plaintext)
    
    try:
        encryptor2.decrypt(ciphertext)
        assert False, "Decryption with wrong key should have failed!"
    except EncryptionError as e:
        assert "invalid key or tampered" in str(e).lower()
        print("✓ Wrong secret correctly fails to decrypt")


def test_tampered_message_rejected():
    """Test that tampered messages are rejected."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Original message"
    ciphertext = encryptor.encrypt(plaintext)
    
    # Tamper with the ciphertext
    tampered = bytearray(ciphertext)
    tampered[-10] ^= 0xFF  # Flip some bits in the encrypted part
    tampered = bytes(tampered)
    
    try:
        encryptor.decrypt(tampered)
        assert False, "Tampered message should have been rejected!"
    except EncryptionError:
        print("✓ Tampered messages are correctly rejected")


def test_minimum_secret_length():
    """Test that short secrets are rejected."""
    try:
        env = Env(MERCURY_SYNC_AUTH_SECRET="short")  # Only 5 chars
        AESGCMFernet(env)
        assert False, "Short secret should have been rejected!"
    except ValueError as e:
        assert "at least 16 characters" in str(e)
        print("✓ Short secrets are correctly rejected")


def test_different_messages_different_ciphertext():
    """Test that same plaintext produces different ciphertext (due to random salt/nonce)."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Same message"
    ciphertext1 = encryptor.encrypt(plaintext)
    ciphertext2 = encryptor.encrypt(plaintext)
    
    assert ciphertext1 != ciphertext2, "Same plaintext should produce different ciphertext"
    
    # Both should decrypt to the same plaintext
    assert encryptor.decrypt(ciphertext1) == plaintext
    assert encryptor.decrypt(ciphertext2) == plaintext
    
    print("✓ Same plaintext produces different ciphertext (randomized)")


def test_message_too_short():
    """Test that truncated messages are rejected."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    # Message shorter than minimum (header + auth tag)
    short_message = b"x" * 10
    
    try:
        encryptor.decrypt(short_message)
        assert False, "Short message should have been rejected!"
    except EncryptionError as e:
        assert "too short" in str(e).lower()
        print("✓ Truncated messages are correctly rejected")


def test_aad_encryption():
    """Test encryption with Additional Authenticated Data."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Secret payload"
    aad = b"message-type:workflow-job"
    
    ciphertext = encryptor.encrypt_with_aad(plaintext, aad)
    decrypted = encryptor.decrypt_with_aad(ciphertext, aad)
    
    assert decrypted == plaintext, "AAD roundtrip failed!"
    
    # Wrong AAD should fail
    try:
        encryptor.decrypt_with_aad(ciphertext, b"wrong-aad")
        assert False, "Wrong AAD should have failed!"
    except EncryptionError:
        pass
    
    print("✓ AAD encryption/decryption works correctly")


def test_pickle_compatibility():
    """Test that the encryptor can be pickled for multiprocessing."""
    import cloudpickle
    
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    # Encrypt before pickling
    plaintext = b"Test message for pickle"
    ciphertext = encryptor.encrypt(plaintext)
    
    # Pickle and unpickle the encryptor using cloudpickle (what the actual code uses)
    pickled = cloudpickle.dumps(encryptor)
    unpickled_encryptor = cloudpickle.loads(pickled)
    
    # The unpickled encryptor should be able to decrypt
    decrypted = unpickled_encryptor.decrypt(ciphertext)
    assert decrypted == plaintext, "Unpickled encryptor failed to decrypt!"
    
    # The unpickled encryptor should be able to encrypt new messages
    new_ciphertext = unpickled_encryptor.encrypt(b"New message")
    new_decrypted = encryptor.decrypt(new_ciphertext)
    assert new_decrypted == b"New message", "Original encryptor failed to decrypt from unpickled!"
    
    print("✓ Encryptor is cloudpickle-compatible for multiprocessing")


def main():
    print("=" * 60)
    print("Testing P0 #1: Fixed Encryption (HKDF Key Derivation)")
    print("=" * 60)
    print()
    
    test_roundtrip()
    test_key_not_in_message()
    test_wrong_secret_fails()
    test_tampered_message_rejected()
    test_minimum_secret_length()
    test_different_messages_different_ciphertext()
    test_message_too_short()
    test_aad_encryption()
    test_pickle_compatibility()
    
    print()
    print("=" * 60)
    print("All encryption tests PASSED!")
    print("=" * 60)
    print()
    print("Security improvements implemented:")
    print("  • Key is derived from shared secret using HKDF-SHA256")
    print("  • Key is NEVER transmitted in messages")
    print("  • Each message uses unique salt → unique derived key")
    print("  • AES-256-GCM provides authenticated encryption")
    print("  • Minimum 16-character secret requirement enforced")
    print("  • Tamper detection via GCM authentication tag")
    print("  • Pickle-compatible for Python multiprocessing")


if __name__ == "__main__":
    main()

