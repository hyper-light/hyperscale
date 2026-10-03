# Copyright (c) 2015-2021 by Ron Frederick <ronf@timeheart.net> and others.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License v2.0 which accompanies this
# distribution and is available at:
#
#     http://www.eclipse.org/legal/epl-2.0/
#
# This program may also be made available under the following secondary
# licenses when the conditions for such availability set forth in the
# Eclipse Public License v2.0 are satisfied:
#
#    GNU General Public License, Version 2.0, or any later versions of
#    that license
#
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
#
# Contributors:
#     Ron Frederick - initial implementation, API, and documentation

"""Chacha20-Poly1305 symmetric encryption handler"""

from ctypes import c_ulonglong, create_string_buffer
from typing import Optional, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends.openssl import backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import ChaCha20
from cryptography.hazmat.primitives.poly1305 import Poly1305

from .cipher import register_cipher


if backend.poly1305_supported():
    _CTR_0 = (0).to_bytes(8, 'little')
    _CTR_1 = (1).to_bytes(8, 'little')

    _POLY1305_KEYBYTES = 32

    # Block 0 of a packet's main key stream: its first bytes are the
    # Poly1305 key, and the packet data is enciphered from block 1 on.
    _POLY1305_KEY_BLOCK = bytes(64)

    def chacha20(key: bytes, data: bytes, nonce: bytes, ctr: int) -> bytes:
        """Encrypt/decrypt a block of data with the ChaCha20 cipher"""

        return Cipher(ChaCha20(key, (_CTR_1 if ctr else _CTR_0) + nonce),
                      mode=None).encryptor().update(data)

    def seal(key: bytes, adkey: bytes, header: bytes, data: bytes,
             nonce: bytes) -> Tuple[bytes, bytes]:
        """Encrypt a packet's header and data, and sign them"""

        counter_nonce = _CTR_0 + nonce
        packet = Cipher(ChaCha20(adkey, counter_nonce),
                        mode=None).encryptor().update(header)

        # One key stream gives both the Poly1305 key and the data cipher.
        stream = Cipher(ChaCha20(key, counter_nonce), mode=None).encryptor()
        polykey = stream.update(_POLY1305_KEY_BLOCK)[:_POLY1305_KEYBYTES]
        packet += stream.update(data)

        return packet, Poly1305.generate_tag(polykey, packet)

    def unseal(key: bytes, header: bytes, data: bytes, nonce: bytes,
               tag: bytes) -> Optional[bytes]:
        """Verify a packet's signature, then decrypt its data"""

        stream = Cipher(ChaCha20(key, _CTR_0 + nonce), mode=None).encryptor()
        polykey = stream.update(_POLY1305_KEY_BLOCK)[:_POLY1305_KEYBYTES]

        try:
            Poly1305.verify_tag(polykey, header + data, tag)
        except InvalidSignature:
            return None

        return stream.update(data)

    chacha_available = True
else: # pragma: no cover
    try:
        from libnacl import nacl

        _chacha20 = nacl.crypto_stream_chacha20
        _chacha20_xor_ic = nacl.crypto_stream_chacha20_xor_ic

        _POLY1305_BYTES = nacl.crypto_onetimeauth_poly1305_bytes()
        _POLY1305_KEYBYTES = nacl.crypto_onetimeauth_poly1305_keybytes()

        _poly1305 = nacl.crypto_onetimeauth_poly1305
        _poly1305_verify = nacl.crypto_onetimeauth_poly1305_verify

        def chacha20(key: bytes, data: bytes, nonce: bytes, ctr: int) -> bytes:
            """Encrypt/decrypt a block of data with the ChaCha20 cipher"""

            datalen = len(data)
            result = create_string_buffer(datalen)
            ull_datalen = c_ulonglong(datalen)
            ull_ctr = c_ulonglong(ctr)

            _chacha20_xor_ic(result, data, ull_datalen, nonce, ull_ctr, key)

            return result.raw

        def poly1305_key(key: bytes, nonce: bytes) -> bytes:
            """Derive a Poly1305 key"""

            polykey = create_string_buffer(_POLY1305_KEYBYTES)
            ull_polykeylen = c_ulonglong(_POLY1305_KEYBYTES)

            _chacha20(polykey, ull_polykeylen, nonce, key)

            return polykey.raw

        def poly1305(key: bytes, data: bytes, nonce: bytes) -> bytes:
            """Compute a Poly1305 tag for a block of data"""

            tag = create_string_buffer(_POLY1305_BYTES)
            ull_datalen = c_ulonglong(len(data))
            polykey = poly1305_key(key, nonce)

            _poly1305(tag, data, ull_datalen, polykey)

            return tag.raw

        def poly1305_verify(key: bytes, data: bytes,
                            nonce: bytes, tag: bytes) -> bool:
            """Verify a Poly1305 tag for a block of data"""

            ull_datalen = c_ulonglong(len(data))
            polykey = poly1305_key(key, nonce)

            return _poly1305_verify(tag, data, ull_datalen, polykey) == 0

        def seal(key: bytes, adkey: bytes, header: bytes, data: bytes,
                 nonce: bytes) -> Tuple[bytes, bytes]:
            """Encrypt a packet's header and data, and sign them"""

            packet = chacha20(adkey, header, nonce, 0) + \
                chacha20(key, data, nonce, 1)

            return packet, poly1305(key, packet, nonce)

        def unseal(key: bytes, header: bytes, data: bytes, nonce: bytes,
                   tag: bytes) -> Optional[bytes]:
            """Verify a packet's signature, then decrypt its data"""

            if poly1305_verify(key, header + data, nonce, tag):
                return chacha20(key, data, nonce, 1)
            else:
                return None

        chacha_available = True
    except (ImportError, OSError, AttributeError):
        chacha_available = False


class ChachaCipher:
    """Shim for Chacha20-Poly1305 symmetric encryption"""

    def __init__(self, key: bytes):
        keylen = len(key) // 2
        self._key = key[:keylen]
        self._adkey = key[keylen:]

    def encrypt_and_sign(self, header: bytes, data: bytes,
                         nonce: bytes) -> Tuple[bytes, bytes]:
        """Encrypt and sign a block of data"""

        return seal(self._key, self._adkey, header, data, nonce)

    def decrypt_header(self, header: bytes, nonce: bytes) -> bytes:
        """Decrypt header data"""

        return chacha20(self._adkey, header, nonce, 0)

    def verify_and_decrypt(self, header: bytes, data: bytes,
                           nonce: bytes, tag: bytes) -> Optional[bytes]:
        """Verify the signature of and decrypt a block of data"""

        return unseal(self._key, header, data, nonce, tag)


if chacha_available: # pragma: no branch
    register_cipher('chacha20-poly1305', 64, 0, 1)
