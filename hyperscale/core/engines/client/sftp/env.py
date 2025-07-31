from typing import Iterator


def decode_env(env: dict[bytes, bytes]) -> Iterator[tuple[str, str]]:
    """Convert bytes-based environemnt dict to Unicode strings"""

    for key, value in env.items():
        try:
            yield key.decode('utf-8'), value.decode('utf-8')
        except UnicodeDecodeError:
            pass
