"""
Snapshot import / export (AD-52 §13).

Disaster-recovery primitives. Export builds a structured tarball
containing cluster_uuid, membership_epoch, Raft state, and soft-state.
Import validates that the target cluster has no committed state, mints
a NEW cluster_uuid (prevents accidental fork), and replays the snapshot.

The serialization format is the existing AD-25 wire format (msgspec).
This module owns the snapshot manifest schema + the import/export
invariants — never the wire-level serialization of individual entries.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING

from .membership_state import MembershipState
from .models.cluster_metadata import ClusterMetadata
from .models.member_record import MemberRecord

if TYPE_CHECKING:
    from hyperscale.logging import Logger


SNAPSHOT_FORMAT_VERSION: int = 1


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    """
    Top-level manifest carried inside the tarball / blob. Members are
    serialized separately as a sorted list of dicts.
    """

    format_version: int
    source_cluster_uuid: str
    source_cluster_id: str
    membership_epoch: int
    last_membership_lsn: int
    cluster_size: int
    exported_at_unix_seconds: float
    exporter_node_id: str
    members_count: int


@dataclass(frozen=True, slots=True)
class SnapshotPayload:
    """In-memory representation; the wire format is JSON / msgspec —
    both work because all fields are primitive."""

    manifest: SnapshotManifest
    members: tuple[MemberRecord, ...]


class SnapshotImportRefused(Exception):
    """Raised when import is attempted on a cluster that already has
    committed state — AD-52 §13 explicitly refuses this."""


class SnapshotExporter:
    """Exporter is straightforward: read MembershipState, build a
    SnapshotPayload, hand it to the caller for wire serialization."""

    __slots__ = ("_membership_state_provider", "_node_id", "_logger")

    def __init__(
        self,
        membership_state_provider: Callable[[], MembershipState],
        node_id: str,
        logger: "Logger | None" = None,
    ) -> None:
        self._membership_state_provider = membership_state_provider
        self._node_id = node_id
        self._logger = logger

    async def export(self) -> SnapshotPayload:
        state = self._membership_state_provider()
        sorted_members = tuple(
            sorted(state.members.values(), key=lambda record: record.node_id)
        )
        manifest = SnapshotManifest(
            format_version=SNAPSHOT_FORMAT_VERSION,
            source_cluster_uuid=state.cluster_metadata.cluster_uuid,
            source_cluster_id=state.cluster_metadata.cluster_id,
            membership_epoch=state.cluster_metadata.membership_epoch,
            last_membership_lsn=state.cluster_metadata.last_membership_lsn,
            cluster_size=state.cluster_metadata.cluster_size,
            exported_at_unix_seconds=time.time(),
            exporter_node_id=self._node_id,
            members_count=len(sorted_members),
        )
        if self._logger is not None:
            await self._logger.log({
                "event": "ClusterSnapshotExported",
                "source_cluster_uuid": manifest.source_cluster_uuid,
                "members_count": manifest.members_count,
            })
        return SnapshotPayload(manifest=manifest, members=sorted_members)


class SnapshotImporter:
    """
    Importer validates that the target cluster is fresh and mints a NEW
    cluster_uuid before installing the snapshot. Per AD-52 §13:

      "Import always mints a new cluster_uuid so an imported cluster
       cannot be confused with the original. This prevents the 'I
       restored a snapshot and now have two clusters with the same
       identity' failure mode."
    """

    __slots__ = (
        "_membership_state_provider",
        "_install_snapshot",
        "_logger",
    )

    def __init__(
        self,
        membership_state_provider: Callable[[], MembershipState],
        install_snapshot: Callable[[MembershipState], Awaitable[None]],
        logger: "Logger | None" = None,
    ) -> None:
        self._membership_state_provider = membership_state_provider
        self._install_snapshot = install_snapshot
        self._logger = logger

    async def import_snapshot(self, payload: SnapshotPayload) -> str:
        """Validates + installs. Returns the new cluster_uuid minted at
        import time."""

        if payload.manifest.format_version != SNAPSHOT_FORMAT_VERSION:
            raise SnapshotImportRefused(
                f"snapshot format_version {payload.manifest.format_version} "
                f"!= local SNAPSHOT_FORMAT_VERSION {SNAPSHOT_FORMAT_VERSION}"
            )

        current_state = self._membership_state_provider()
        if current_state.cluster_metadata.cluster_uuid != "":
            raise SnapshotImportRefused(
                "target cluster already has committed state — refuse to "
                "import (AD-52 §13)"
            )

        new_cluster_uuid = uuid.uuid4().hex
        new_state = MembershipState(
            cluster_metadata=ClusterMetadata(
                cluster_uuid=new_cluster_uuid,
                cluster_id=payload.manifest.source_cluster_id,
                cluster_size=payload.manifest.cluster_size,
                membership_epoch=payload.manifest.membership_epoch,
                last_membership_lsn=payload.manifest.last_membership_lsn,
            ),
            members={record.node_id: record for record in payload.members},
        )
        await self._install_snapshot(new_state)
        if self._logger is not None:
            await self._logger.log({
                "event": "ClusterSnapshotImported",
                "source_cluster_uuid": payload.manifest.source_cluster_uuid,
                "new_cluster_uuid": new_cluster_uuid,
                "members_count": payload.manifest.members_count,
            })
        return new_cluster_uuid
