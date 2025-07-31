from typing import Protocol


class Hash(Protocol):
    """Protocol for hashing data"""

    @property
    def digest_size(self) -> int:
        """Return the hash digest size"""

    @property
    def block_size(self) -> int:
        """Return the hash block size"""

    @property
    def name(self) -> str:
        """Return the hash name"""

    def digest(self) -> bytes:
        """Return the digest value as a bytes object"""

    def hexdigest(self) -> str:
        """Return the digest value as a string of hexadecimal digits"""

    def update(self, __data: bytes) -> None:
        """Update this hash object's state with the provided bytes"""



class HashType(Protocol):
    """Protocol for returning the type of a hash function"""

    def __call__(self, __data: bytes = ...) -> Hash:
        """Create a new hash object"""
