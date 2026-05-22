"""
Watch-stream protocol messages (AD-52 §9).

The watch stream is the membership-distribution mechanism — every node
opens a long-lived stream to the cluster and receives membership deltas
as they commit. Replaces the polling-based AD-27 / AD-30 distribution
paths. Equivalent in spirit to etcd watch and Envoy xDS.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .member_record import MemberRecord


@dataclass(frozen=True, slots=True)
class WatchFilter:
    """
    Server-side filter applied to deltas before dispatch. Reduces wire
    traffic when a watcher only cares about a subset of entries (e.g.,
    a worker watching only its owner manager).

    Fields:
        entry_types     Optional whitelist of MembershipLogEntry class
                        names. Empty tuple → all entry types.
        node_id_subset  Optional whitelist of MemberRecord.node_ids the
                        watcher cares about. Empty tuple → all members.
    """

    entry_types: tuple[str, ...] = field(default_factory=tuple)
    node_id_subset: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class WatchOpen:
    """
    Client → server: open a watch stream.

    Fields:
        last_seen_membership_epoch  Epoch of the last delta the client
                                    saw. The server resumes from
                                    last_seen_lsn + 1 when the LSN is
                                    within retention, otherwise serves
                                    a fresh WatchSnapshot.
        last_seen_lsn               LSN of the last delta the client
                                    processed; 0 on first connect.
        watch_filter                Optional server-side filter.
        watcher_node_id             uuid4() of the watching node; used
                                    in WatchStream metrics labels.
    """

    last_seen_membership_epoch: int
    last_seen_lsn: int
    watcher_node_id: str
    watch_filter: WatchFilter = field(default_factory=WatchFilter)


@dataclass(frozen=True, slots=True)
class WatchSnapshot:
    """
    Server → client: full snapshot at a specific LSN. Sent either on
    initial connect or when the client's last_seen_lsn has rolled past
    the server's snapshot retention.

    Fields:
        snapshot_epoch     Membership epoch at snapshot time.
        snapshot_lsn       Raft LSN at snapshot time.
        membership_at_lsn  Complete membership table at that LSN, sorted
                           by node_id for AD-52 §15 determinism.
        cluster_uuid       Bootstrap-minted cluster UUID.
        cluster_size       Configured cluster size at snapshot time.
    """

    snapshot_epoch: int
    snapshot_lsn: int
    membership_at_lsn: tuple[MemberRecord, ...]
    cluster_uuid: str
    cluster_size: int


@dataclass(frozen=True, slots=True)
class WatchDelta:
    """
    Server → client: a single committed log entry. The entry payload is
    encoded as opaque bytes — the client decodes via the membership_log
    dispatch table per AD-52 §14.

    Fields:
        epoch         Epoch after applying this entry.
        lsn           Raft LSN of the entry.
        entry_type    Class name of the MembershipLogEntry (e.g.,
                      "AddLearner"). Used for the watch_filter check
                      and for client-side dispatch.
        schema_version  AD-52 §14 schema version of the encoded payload.
        encoded_entry   Opaque entry payload; the client decodes via
                        the dispatch table.
        apply_after_lsn LSN the client must have already applied before
                        applying this one — enforces strict ordering.
    """

    epoch: int
    lsn: int
    entry_type: str
    schema_version: int
    encoded_entry: bytes
    apply_after_lsn: int
