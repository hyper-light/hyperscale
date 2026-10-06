"""``EncryptionError`` -- pickled under the namespace
``hyperscale.distributed.encryption.aes_gcm`` (see that module)."""



class EncryptionError(Exception):
    """Raised when encryption or decryption fails."""
    pass
