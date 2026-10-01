from __future__ import annotations

from pathlib import Path

from hyperscale.core.runtime.filesystem import Filesystem
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import StorageFormatUnrecognized
from hyperscale.distributed.ledger.storage_format.unstable_storage_read_error import (
    UnstableStorageReadError,
)


async def set_aside_unrecognized(
    filesystem: Filesystem,
    path: Path,
    data: bytes,
    reason: str,
    logger: Logger,
) -> Path:
    """Move a file this node cannot read out of the way, loudly.

    Its bytes are preserved under ``<name>.unrecognized-<n>`` (never
    overwriting an earlier set-aside) and the original path is freed, so
    the node proceeds without that file instead of misreading it -- and
    an operator can still recover the data.

    The file is read again first: an unreadable verdict reached from bytes
    a faulty read path flipped must not remove an intact file, so reads
    that disagree raise ``UnstableStorageReadError`` and nothing moves.
    """
    if await filesystem.read_bytes(path) != data:
        raise UnstableStorageReadError(path)
    attempt = 0
    while await filesystem.exists(aside := path.with_name(f"{path.name}.unrecognized-{attempt}")):
        attempt += 1
    await filesystem.atomic_write(aside, data)
    await filesystem.remove(path)
    await logger.log(
        StorageFormatUnrecognized(
            message=(
                f"{path} cannot be read ({reason}); "
                f"moved to {aside}, proceeding without it"
            ),
            path=str(path),
            set_aside_path=str(aside),
            reason=reason,
        )
    )
    return aside
