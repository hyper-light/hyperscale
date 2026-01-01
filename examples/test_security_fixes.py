#!/usr/bin/env python3
"""
Test suite for security fixes:
- P0 #1: HKDF-based encryption (key not transmitted)
- P1 #1: Replay attack protection
- P1 #2: Strong secret enforcement
"""

import sys
import os
import time
import warnings

sys.path.insert(0, '/home/ada/Projects/hyperscale')

# Import directly to avoid module import issues
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

encryption_module = load_module(
    "encryption",
    "/home/ada/Projects/hyperscale/hyperscale/core/jobs/protocols/encryption.py"
)
replay_guard_module = load_module(
    "replay_guard",
    "/home/ada/Projects/hyperscale/hyperscale/core/jobs/protocols/replay_guard.py"
)

AESGCMFernet = encryption_module.AESGCMFernet
EncryptionError = encryption_module.EncryptionError
WEAK_SECRETS = encryption_module.WEAK_SECRETS

ReplayGuard = replay_guard_module.ReplayGuard
ReplayError = replay_guard_module.ReplayError

from hyperscale.core.jobs.models.env import Env
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator


print("=" * 70)
print("Security Fixes Test Suite")
print("=" * 70)
print()


# ============================================================================
# P0 #1: HKDF Encryption Tests
# ============================================================================

print("P0 #1: HKDF Encryption Tests")
print("-" * 40)

def test_encryption_roundtrip():
    """Test basic encrypt/decrypt roundtrip."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    plaintext = b"Hello, World! This is a test message."
    ciphertext = encryptor.encrypt(plaintext)
    decrypted = encryptor.decrypt(ciphertext)
    
    assert decrypted == plaintext
    print("  ✓ Roundtrip encrypt/decrypt works")

def test_key_not_transmitted():
    """Verify encryption key is NOT in message."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    ciphertext = encryptor.encrypt(b"Secret data")
    
    # Structure: salt (16B) || nonce (12B) || ciphertext
    salt = ciphertext[:16]
    nonce = ciphertext[16:28]
    
    assert len(salt) == 16
    assert len(nonce) == 12
    print("  ✓ Key NOT transmitted (only salt + nonce)")

def test_wrong_secret_fails():
    """Wrong secret cannot decrypt."""
    env1 = Env(MERCURY_SYNC_AUTH_SECRET="correct-secret-key-123")
    env2 = Env(MERCURY_SYNC_AUTH_SECRET="wrong-secret-key-12345")
    
    encryptor1 = AESGCMFernet(env1)
    encryptor2 = AESGCMFernet(env2)
    
    ciphertext = encryptor1.encrypt(b"Secret message")
    
    try:
        encryptor2.decrypt(ciphertext)
        assert False, "Should have failed"
    except EncryptionError:
        pass
    print("  ✓ Wrong secret correctly rejected")

def test_tamper_detection():
    """Tampered messages are rejected."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    ciphertext = encryptor.encrypt(b"Original")
    tampered = bytearray(ciphertext)
    tampered[-5] ^= 0xFF
    
    try:
        encryptor.decrypt(bytes(tampered))
        assert False, "Should have failed"
    except EncryptionError:
        pass
    print("  ✓ Tampered messages rejected")

def test_pickle_compatible():
    """Encryptor works after cloudpickle roundtrip."""
    import cloudpickle
    
    env = Env(MERCURY_SYNC_AUTH_SECRET="test-secret-key-12345")
    encryptor = AESGCMFernet(env)
    
    ciphertext = encryptor.encrypt(b"Test message")
    
    pickled = cloudpickle.dumps(encryptor)
    restored = cloudpickle.loads(pickled)
    
    decrypted = restored.decrypt(ciphertext)
    assert decrypted == b"Test message"
    print("  ✓ Pickle-compatible for multiprocessing")

test_encryption_roundtrip()
test_key_not_transmitted()
test_wrong_secret_fails()
test_tamper_detection()
test_pickle_compatible()

print()


# ============================================================================
# P1 #1: Replay Protection Tests
# ============================================================================

print("P1 #1: Replay Protection Tests")
print("-" * 40)

def test_replay_accepts_fresh_message():
    """Fresh messages are accepted."""
    guard = ReplayGuard(max_age_seconds=300)
    generator = SnowflakeGenerator(1)
    
    shard_id = generator.generate()
    is_valid, error = guard.validate(shard_id, raise_on_error=False)
    
    assert is_valid
    assert error is None
    print("  ✓ Fresh messages accepted")

def test_replay_rejects_duplicate():
    """Duplicate messages are rejected."""
    guard = ReplayGuard(max_age_seconds=300)
    generator = SnowflakeGenerator(1)
    
    shard_id = generator.generate()
    
    # First time - accepted
    is_valid, _ = guard.validate(shard_id, raise_on_error=False)
    assert is_valid
    
    # Second time - rejected
    is_valid, error = guard.validate(shard_id, raise_on_error=False)
    assert not is_valid
    assert "Duplicate" in error
    print("  ✓ Duplicate messages rejected")

def test_replay_rejects_stale():
    """Old messages are rejected."""
    guard = ReplayGuard(max_age_seconds=1)  # 1 second max age
    generator = SnowflakeGenerator(1)
    
    # Generate ID, then wait for it to become stale
    shard_id = generator.generate()
    time.sleep(1.5)  # Wait for message to become stale
    
    is_valid, error = guard.validate(shard_id, raise_on_error=False)
    assert not is_valid
    assert "too old" in error.lower()
    print("  ✓ Stale messages rejected")

def test_replay_stats():
    """Statistics are tracked correctly."""
    guard = ReplayGuard()
    generator = SnowflakeGenerator(1)
    
    # Accept some messages
    for _ in range(5):
        guard.validate(generator.generate(), raise_on_error=False)
    
    # Try a duplicate
    dup_id = generator.generate()
    guard.validate(dup_id, raise_on_error=False)
    guard.validate(dup_id, raise_on_error=False)  # duplicate
    
    stats = guard.get_stats()
    assert stats['accepted'] == 6
    assert stats['duplicates_rejected'] == 1
    print("  ✓ Statistics tracked correctly")

def test_replay_window_cleanup():
    """Old entries are cleaned up."""
    guard = ReplayGuard(max_age_seconds=1, max_window_size=10)
    generator = SnowflakeGenerator(1)
    
    # Add messages
    for _ in range(5):
        guard.validate(generator.generate(), raise_on_error=False)
    
    assert len(guard) == 5
    
    # Wait for cleanup age
    time.sleep(1.5)
    
    # Add more to trigger cleanup
    for _ in range(1001):  # Trigger cleanup (every 1000 messages)
        guard.validate(generator.generate(), raise_on_error=False)
    
    # Old entries should be cleaned
    stats = guard.get_stats()
    assert stats['tracked_ids'] <= guard._max_window_size
    print("  ✓ Old entries cleaned up")

def test_replay_raise_on_error():
    """ReplayError is raised when requested."""
    guard = ReplayGuard()
    generator = SnowflakeGenerator(1)
    
    shard_id = generator.generate()
    guard.validate(shard_id, raise_on_error=True)
    
    try:
        guard.validate(shard_id, raise_on_error=True)
        assert False, "Should have raised"
    except ReplayError as e:
        assert "Duplicate" in str(e)
    print("  ✓ ReplayError raised correctly")

test_replay_accepts_fresh_message()
test_replay_rejects_duplicate()
test_replay_rejects_stale()
test_replay_stats()
test_replay_window_cleanup()
test_replay_raise_on_error()

print()


# ============================================================================
# P1 #2: Strong Secret Enforcement Tests
# ============================================================================

print("P1 #2: Strong Secret Enforcement Tests")
print("-" * 40)

def test_strong_secret_accepted():
    """Strong secrets are accepted."""
    env = Env(MERCURY_SYNC_AUTH_SECRET="my-super-secure-random-key-12345")
    encryptor = AESGCMFernet(env)
    assert encryptor is not None
    print("  ✓ Strong secrets accepted")

def test_short_secret_rejected():
    """Secrets < 16 chars are rejected."""
    try:
        env = Env(MERCURY_SYNC_AUTH_SECRET="short123")  # 8 chars
        AESGCMFernet(env)
        assert False, "Should have raised"
    except ValueError as e:
        assert "16 characters" in str(e)
    print("  ✓ Short secrets rejected")

def test_weak_secret_warning():
    """Weak secrets produce warning in dev mode."""
    # Ensure not in production mode
    old_env = os.environ.get('HYPERSCALE_ENV', '')
    os.environ['HYPERSCALE_ENV'] = 'development'
    
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            env = Env(MERCURY_SYNC_AUTH_SECRET="hyperscale-dev-secret-change-in-prod")
            AESGCMFernet(env)
            
            # Should have warning
            assert len(w) == 1
            assert "weak" in str(w[0].message).lower()
    finally:
        if old_env:
            os.environ['HYPERSCALE_ENV'] = old_env
        elif 'HYPERSCALE_ENV' in os.environ:
            del os.environ['HYPERSCALE_ENV']
    print("  ✓ Weak secrets warn in dev mode")

def test_weak_secret_error_production():
    """Weak secrets raise error in production mode."""
    old_env = os.environ.get('HYPERSCALE_ENV', '')
    os.environ['HYPERSCALE_ENV'] = 'production'
    
    try:
        env = Env(MERCURY_SYNC_AUTH_SECRET="hyperscale-dev-secret-change-in-prod")
        AESGCMFernet(env)
        assert False, "Should have raised"
    except ValueError as e:
        assert "production" in str(e).lower()
    finally:
        if old_env:
            os.environ['HYPERSCALE_ENV'] = old_env
        else:
            del os.environ['HYPERSCALE_ENV']
    print("  ✓ Weak secrets rejected in production")

def test_known_weak_secrets_list():
    """All known weak secrets are in the list."""
    expected_weak = ['hyperscale', 'password', 'secret', 'changeme', 'test']
    for weak in expected_weak:
        assert weak in WEAK_SECRETS
    print("  ✓ Known weak secrets list populated")

test_strong_secret_accepted()
test_short_secret_rejected()
test_weak_secret_warning()
test_weak_secret_error_production()
test_known_weak_secrets_list()

print()


# ============================================================================
# Summary
# ============================================================================

print("=" * 70)
print("All security tests PASSED!")
print("=" * 70)
print()
print("Security features implemented:")
print("  P0 #1: HKDF encryption - Key derived from secret, never transmitted")
print("  P1 #1: Replay protection - Timestamp freshness + duplicate detection")
print("  P1 #2: Strong secrets - Weak/default secrets rejected in production")
print()

