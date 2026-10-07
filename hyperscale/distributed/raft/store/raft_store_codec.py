"""
The on-disk form of a node's Raft store (D1): CRC-framed msgspec records,
replayed into each group's persistent state, and re-encoded live-only for
compaction.

Frame: ``[4: crc32 of body][4: body length][body]``, big-endian.
"""

import struct
import zlib
from functools import partial

import msgspec

from hyperscale.distributed.raft.models import RaftLogEntry

from .models import (
    EntriesRecord,
    GroupCreatedRecord,
    GroupReleasedRecord,
    HardStateRecord,
    RaftStoreHeader,
    RecoveredRaftGroup,
    SnapshotRecord,
    TruncateFromRecord,
)
from .raft_store_untrustworthy_error import RaftStoreUntrustworthyError

RaftStoreRecord = (
    RaftStoreHeader
    | GroupCreatedRecord
    | HardStateRecord
    | EntriesRecord
    | TruncateFromRecord
    | SnapshotRecord
    | GroupReleasedRecord
)

STORE_FORMAT_VERSION = 1
FRAME_HEADER = struct.Struct(">II")


class RaftStoreCodec:
    """Encodes store records into frames, and replays a store file back
    into each unreleased group's persistent Raft state -- refusing any
    file that is not this identity's records plus, at most, a torn last
    frame (D1 P5)."""

    __slots__ = ("_encoder", "_decoder")

    def __init__(self) -> None:
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(RaftStoreRecord)

    def encode_frames(self, records: list[RaftStoreRecord]) -> bytes:
        """The records' frames, in order, as one write."""
        frames = bytearray()
        for record in records:
            body = self._encoder.encode(record)
            frames += FRAME_HEADER.pack(zlib.crc32(body), len(body))
            frames += body
        return bytes(frames)

    def replay(self, data: bytes, stamp: bytes) -> tuple[dict[str, RecoveredRaftGroup], int]:
        """Each unreleased group's state, and how many leading bytes of
        ``data`` hold whole records -- fewer than ``len(data)`` only when
        the last frame was torn by a power loss (it was never fsynced, so
        never acknowledged). A zero-filled tail counts as torn: a file
        system may extend a file with zeros its crash never wrote.

        Raises:
            RaftStoreUntrustworthyError: damage before the last frame, a
                header of another identity or format, or a record that
                breaks Raft's invariants.
        """
        groups: dict[str, RecoveredRaftGroup] = {}
        offset = 0
        while (whole_record := self._read_whole_record(data, offset)) is not None:
            frame_end, record = whole_record
            self._replay_record(groups, record, offset, stamp)
            offset = frame_end
        if offset == 0:
            # The header is written whole, before the identity, at creation.
            raise RaftStoreUntrustworthyError("the store's header is missing or damaged")
        return groups, offset

    def _read_whole_record(self, data: bytes, offset: int) -> tuple[int, RaftStoreRecord] | None:
        """The record framed at ``offset`` and the byte its frame ends at,
        or None where the file's whole records end: at its end, or at a
        torn last frame.

        Raises:
            RaftStoreUntrustworthyError: a damaged frame with written bytes
                or a whole frame after it, or an intact frame that does not
                decode.
        """
        if (frame := self._frame_at(data, offset)) is None:
            self._raise_if_whole_frame_after(data, offset)
            return None
        frame_end, checksum, body_length, body = frame
        if self._frame_is_damaged(checksum, body_length, body):
            self._raise_unless_torn_tail(data, offset, frame_end)
            self._raise_if_whole_frame_after(data, offset)
            return None
        return frame_end, self._decode_record(body, offset)

    @staticmethod
    def _raise_if_whole_frame_after(data: bytes, offset: int) -> None:
        """A frame whose length was damaged claims to run past the end of
        the file (or to end exactly there): only the whole frames written
        after it tell it from a torn tail.

        Raises:
            RaftStoreUntrustworthyError: a whole frame begins after ``offset``.
        """
        if RaftStoreCodec._has_whole_frame_after(data, offset):
            raise RaftStoreUntrustworthyError(f"the record at byte {offset} is damaged with whole records after it")

    @staticmethod
    def _has_whole_frame_after(data: bytes, offset: int) -> bool:
        """Whether a whole frame -- a possible length and a checksum that
        holds -- begins at any byte after ``offset``."""
        size = len(data)
        data_view = memoryview(data)
        candidate_offsets = range(offset + 1, size - FRAME_HEADER.size + 1)
        return any(
            zlib.crc32(data_view[candidate_offset + FRAME_HEADER.size :][:body_length]) == checksum
            for candidate_offset, (checksum, body_length) in zip(
                candidate_offsets, map(partial(FRAME_HEADER.unpack_from, data), candidate_offsets)
            )
            if 0 < body_length <= size - candidate_offset - FRAME_HEADER.size
        )

    @staticmethod
    def _frame_is_damaged(checksum: int, body_length: int, body: bytes) -> bool:
        """Whether a frame is empty or its body fails its checksum."""
        return body_length == 0 or zlib.crc32(body) != checksum

    @staticmethod
    def _frame_at(data: bytes, offset: int) -> tuple[int, int, int, bytes] | None:
        """The frame at ``offset`` -- its end, checksum, body length and
        body -- or None when the file ends before the frame does (at its
        end, or the last frame cut short in its header or its body)."""
        size = len(data)
        if offset + FRAME_HEADER.size > size:
            # The last frame, cut short in its header.
            return None
        checksum, body_length = FRAME_HEADER.unpack_from(data, offset)
        if (frame_end := offset + FRAME_HEADER.size + body_length) > size:
            # The last frame, cut short in its body.
            return None
        return frame_end, checksum, body_length, data[offset + FRAME_HEADER.size : frame_end]

    @staticmethod
    def _raise_unless_torn_tail(data: bytes, offset: int, frame_end: int) -> None:
        """Damage in the last frame, or zeros to the end of the file where
        nothing was written, is a torn tail; damage anywhere else is not.

        Raises:
            RaftStoreUntrustworthyError: the damaged frame has written bytes
                after it.
        """
        size = len(data)
        if frame_end == size or not any(memoryview(data)[offset:]):
            return
        raise RaftStoreUntrustworthyError(
            f"the record at byte {offset} fails its checksum with {size - frame_end} bytes after it"
        )

    def _decode_record(self, body: bytes, offset: int) -> RaftStoreRecord:
        """The record a checksummed ``body`` holds.

        Raises:
            RaftStoreUntrustworthyError: its checksum held, so it was written
                whole, in a form this build does not read.
        """
        try:
            return self._decoder.decode(body)
        except msgspec.DecodeError as decode_error:
            # Its checksum held: written whole, in a form this build
            # does not read.
            raise RaftStoreUntrustworthyError(
                f"the record at byte {offset} does not decode: {decode_error}"
            ) from decode_error

    def _replay_record(
        self, groups: dict[str, RecoveredRaftGroup], record: RaftStoreRecord, offset: int, stamp: bytes
    ) -> None:
        """Replays one record into ``groups``: the first must be this
        identity's header, every later one a group record."""
        if offset == 0:
            self._check_header(record, stamp)
            return
        self._RECORD_REPLAYERS[type(record)](self, groups, record, offset)

    @staticmethod
    def _check_header(record: RaftStoreRecord, stamp: bytes) -> None:
        """Raises unless ``record`` is a header of this identity and format.

        Raises:
            RaftStoreUntrustworthyError: the store does not begin with a
                header, or begins with another identity's or format's.
        """
        if not isinstance(record, RaftStoreHeader):
            raise RaftStoreUntrustworthyError("the store does not begin with its header")
        RaftStoreCodec._check_header_identity(record, stamp)

    @staticmethod
    def _check_header_identity(record: RaftStoreHeader, stamp: bytes) -> None:
        """Raises unless the header is this identity's, in this format.

        Raises:
            RaftStoreUntrustworthyError: another identity's or format's.
        """
        if record.stamp != stamp:
            raise RaftStoreUntrustworthyError("the store was written under another identity")
        if record.format_version != STORE_FORMAT_VERSION:
            raise RaftStoreUntrustworthyError(
                f"the store is format {record.format_version}; this build reads {STORE_FORMAT_VERSION}"
            )

    @staticmethod
    def _created_group(
        groups: dict[str, RecoveredRaftGroup],
        record: HardStateRecord | EntriesRecord | TruncateFromRecord | SnapshotRecord,
    ) -> RecoveredRaftGroup:
        """The group ``record`` belongs to.

        Raises:
            RaftStoreUntrustworthyError: the record precedes its group's
                creation.
        """
        if record.group_id not in groups:
            raise RaftStoreUntrustworthyError(
                f"group {record.group_id} has a {type(record).__name__} before its creation"
            )
        return groups[record.group_id]

    def _replay_group_created(
        self, groups: dict[str, RecoveredRaftGroup], record: GroupCreatedRecord, offset: int
    ) -> None:
        """Creates the group, once, with at least one voter."""
        if record.group_id in groups:
            raise RaftStoreUntrustworthyError(f"group {record.group_id} was created twice")
        if not record.initial_voters:
            raise RaftStoreUntrustworthyError(f"group {record.group_id} was created with no voters")
        groups[record.group_id] = RecoveredRaftGroup(member_id=record.member_id, initial_voters=record.initial_voters)

    def _replay_hard_state(self, groups: dict[str, RecoveredRaftGroup], record: HardStateRecord, offset: int) -> None:
        """Adopts the group's term and vote. Raft (section 5.1/5.2): the
        term never falls, and a vote cast in a term never changes."""
        group = self._created_group(groups, record)
        if record.term < group.term or self._changes_vote_within_term(group, record.term, record.voted_for):
            raise RaftStoreUntrustworthyError(
                f"group {record.group_id} went from term {group.term} (vote {group.voted_for}) "
                f"to term {record.term} (vote {record.voted_for})"
            )
        group.term = record.term
        group.voted_for = record.voted_for

    @staticmethod
    def _changes_vote_within_term(group: RecoveredRaftGroup, term: int, voted_for: str | None) -> bool:
        """Whether a hard state in the group's own term replaces a vote it
        already cast (section 5.2: at most one vote per term)."""
        return term == group.term and group.voted_for is not None and voted_for != group.voted_for

    def _replay_entries(self, groups: dict[str, RecoveredRaftGroup], record: EntriesRecord, offset: int) -> None:
        """Appends at least one entry, each at the next index, in a term no
        lower than the entry before it and no higher than the group's."""
        group = self._created_group(groups, record)
        self._check_entries_follow_log(group, record.group_id, record.entries)
        if not record.entries:
            raise RaftStoreUntrustworthyError(f"group {record.group_id} appended no entries")
        group.entries.extend(record.entries)

    @staticmethod
    def _last_log_term(group: RecoveredRaftGroup) -> int:
        """The term of the group's last entry, else its snapshot's, else 0."""
        return group.entries[-1].term if group.entries else (
            0 if group.snapshot is None else group.snapshot.last_term
        )

    def _check_entries_follow_log(
        self, group: RecoveredRaftGroup, group_id: str, entries: list[RaftLogEntry]
    ) -> None:
        """Raises unless each entry takes the next index, in a term between
        its predecessor's and the group's (section 5.3: terms in a log never
        fall; no entry is newer than its writer's term)."""
        expected_index = group.last_index + 1
        previous_term = self._last_log_term(group)
        for entry in entries:
            if self._entry_out_of_place(entry, expected_index, previous_term, group.term):
                raise RaftStoreUntrustworthyError(
                    f"group {group_id} appended index {entry.index} term {entry.term} where index "
                    f"{expected_index}, term {previous_term}..{group.term} was due"
                )
            expected_index += 1
            previous_term = entry.term

    @staticmethod
    def _entry_out_of_place(entry: RaftLogEntry, expected_index: int, previous_term: int, group_term: int) -> bool:
        """Whether ``entry`` misses the next index or the allowed terms."""
        return entry.index != expected_index or not previous_term <= entry.term <= group_term

    def _replay_truncate_from(
        self, groups: dict[str, RecoveredRaftGroup], record: TruncateFromRecord, offset: int
    ) -> None:
        """Cuts the log from an index it holds after its base."""
        group = self._created_group(groups, record)
        if not group.base_index < record.index <= group.last_index:
            raise RaftStoreUntrustworthyError(
                f"group {record.group_id} cut its log from {record.index}, outside "
                f"{group.base_index + 1}..{group.last_index}"
            )
        del group.entries[record.index - group.base_index - 1 :]

    def _replay_snapshot(self, groups: dict[str, RecoveredRaftGroup], record: SnapshotRecord, offset: int) -> None:
        """Installs a snapshot no older than the base, of no later term."""
        group = self._created_group(groups, record)
        if record.last_index < group.base_index or record.last_term > group.term:
            raise RaftStoreUntrustworthyError(
                f"group {record.group_id} snapshotted through {record.last_index} (term {record.last_term}) "
                f"behind its base {group.base_index} or past its term {group.term}"
            )
        group.entries = self._entries_after_snapshot(group, record.last_index, record.last_term)
        group.snapshot = record

    @staticmethod
    def _entry_at_snapshot_index(group: RecoveredRaftGroup, last_index: int) -> RaftLogEntry | None:
        """The log's entry at the snapshot's last index, if the log holds it."""
        return (
            group.entries[last_index - group.base_index - 1]
            if group.base_index < last_index <= group.last_index
            else None
        )

    def _entries_after_snapshot(self, group: RecoveredRaftGroup, last_index: int, last_term: int) -> list[RaftLogEntry]:
        """Raft section 7: entries after the snapshot are kept when the log
        holds its last entry; otherwise the snapshot replaces the whole log."""
        held = self._entry_at_snapshot_index(group, last_index)
        return group.entries[last_index - group.base_index :] if held is not None and held.term == last_term else []

    def _replay_group_released(
        self, groups: dict[str, RecoveredRaftGroup], record: GroupReleasedRecord, offset: int
    ) -> None:
        """Forgets a released group (released before creation is a no-op)."""
        groups.pop(record.group_id, None)

    def _replay_second_header(
        self, groups: dict[str, RecoveredRaftGroup], record: RaftStoreHeader, offset: int
    ) -> None:
        """Raises: only the first record is a header."""
        raise RaftStoreUntrustworthyError(f"a second header at byte {offset}")

    def materialize(
        self, groups: dict[str, RecoveredRaftGroup], stamp: bytes
    ) -> tuple[bytes, dict[str, int], dict[str, int]]:
        """The store holding exactly ``groups`` -- header, then per group
        (in id order) its hard state, snapshot and entries -- with each
        group's bytes and, of those, its hard state's."""
        frames = bytearray(self.encode_frames([RaftStoreHeader(stamp=stamp, format_version=STORE_FORMAT_VERSION)]))
        group_bytes: dict[str, int] = {}
        hard_state_bytes: dict[str, int] = {}
        for group_id in sorted(groups):
            group_frames, hard_state_frame = self._materialize_group(group_id, groups[group_id])
            frames += group_frames
            group_bytes[group_id] = len(group_frames)
            hard_state_bytes[group_id] = len(hard_state_frame)
        return bytes(frames), group_bytes, hard_state_bytes

    def _materialize_group(self, group_id: str, group: RecoveredRaftGroup) -> tuple[bytes, bytes]:
        """One group's frames -- created, hard state, snapshot (if any),
        entries (if any) -- and, of those, its hard state's frame."""
        hard_state_frame = self.encode_frames(
            [HardStateRecord(group_id=group_id, term=group.term, voted_for=group.voted_for)]
        )
        group_frames = self.encode_frames(
            [
                GroupCreatedRecord(
                    group_id=group_id, member_id=group.member_id, initial_voters=group.initial_voters
                )
            ]
        ) + hard_state_frame + self.encode_frames(
            ([group.snapshot] if group.snapshot is not None else [])
            + ([EntriesRecord(group_id=group_id, entries=group.entries)] if group.entries else [])
        )
        return group_frames, hard_state_frame

    # Each group record's replay step, by its type; a record's type is
    # exactly one of the store's union (msgspec decodes tagged structs).
    _RECORD_REPLAYERS = {
        GroupCreatedRecord: _replay_group_created,
        HardStateRecord: _replay_hard_state,
        EntriesRecord: _replay_entries,
        TruncateFromRecord: _replay_truncate_from,
        SnapshotRecord: _replay_snapshot,
        GroupReleasedRecord: _replay_group_released,
        RaftStoreHeader: _replay_second_header,
    }
