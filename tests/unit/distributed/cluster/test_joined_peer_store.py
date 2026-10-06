"""
Runtime joins survive a restart (JoinedPeerStore).

A gate or manager joined at runtime (`hyperscale join`) kept the join in
memory only, so a restart forgot it. Joined peers are now saved in the
node's data directory and reloaded on start. Persistence is
opportunistic: the join is live whether or not it is saved, a failed
save is logged, and a missing, foreign or damaged file reads as no
joined peers -- never as a reason to refuse to start.

* saved peers are loaded by the next store over the same directory;
* re-adding a known peer does not rewrite the file;
* no file reads as no peers;
* a foreign or damaged file reads as no peers, with a warning;
* a failed save is logged as an error and the peer stays joined.
"""

import json
from pathlib import Path

import pytest

from hyperscale.distributed.cluster.joined_peer import JoinedPeer
from hyperscale.distributed.cluster.joined_peer_store import JoinedPeerStore
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerWarning
from tests.simulation.harness.sim import SimFilesystem

DATA_DIRECTORY = Path("/node/data")
STORE_PATH = DATA_DIRECTORY / "joined_peers.json"
GATE_PEER = JoinedPeer(
    datacenter="global",
    tcp_address=("gate-0.hyperscale-gate.default.svc.cluster.local", 8431),
    udp_address=("gate-0.hyperscale-gate.default.svc.cluster.local", 8441),
)
SECOND_GATE_PEER = JoinedPeer(
    datacenter="global",
    tcp_address=("gate-1.hyperscale-gate.default.svc.cluster.local", 8431),
    udp_address=("gate-1.hyperscale-gate.default.svc.cluster.local", 8441),
)


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


class FailingWriteFilesystem(SimFilesystem):
    """Every save fails, as on a full or read-only disk."""

    async def atomic_write(self, path, data: bytes) -> None:
        raise OSError(28, "No space left on device")


def make_store(filesystem: SimFilesystem) -> tuple[JoinedPeerStore, RecordingLogger]:
    logger = RecordingLogger()
    return JoinedPeerStore(DATA_DIRECTORY, filesystem, logger, "manager-0", 8231), logger


@pytest.mark.asyncio
async def test_saved_peers_are_loaded_by_the_next_store() -> None:
    filesystem = SimFilesystem()
    first_store, _ = make_store(filesystem)
    await first_store.load()
    await first_store.add([GATE_PEER, SECOND_GATE_PEER])

    restarted_store, logger = make_store(filesystem)

    assert sorted(await restarted_store.load(), key=lambda peer: peer.tcp_address) == [GATE_PEER, SECOND_GATE_PEER]
    assert logger.entries == []


@pytest.mark.asyncio
async def test_re_adding_a_known_peer_does_not_rewrite_the_file() -> None:
    filesystem = SimFilesystem()
    store, _ = make_store(filesystem)
    await store.add([GATE_PEER])
    saved = await filesystem.read_text(STORE_PATH, encoding="utf-8")
    await filesystem.atomic_write(STORE_PATH, b"sentinel")

    await store.add([GATE_PEER])

    assert await filesystem.read_text(STORE_PATH, encoding="utf-8") == "sentinel"
    assert json.loads(saved)["peers"][0]["tcp"] == list(GATE_PEER.tcp_address)


@pytest.mark.asyncio
async def test_no_file_reads_as_no_peers() -> None:
    store, logger = make_store(SimFilesystem())

    assert await store.load() == []
    assert logger.entries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "contents",
    [
        b"not json",
        json.dumps({"format": "something-else", "version": 1, "peers": []}).encode(),
        json.dumps({"format": "hyperscale-joined-peers", "version": 1, "peers": [{"tcp": ["host"]}]}).encode(),
    ],
)
async def test_a_foreign_or_damaged_file_reads_as_no_peers_with_a_warning(contents: bytes) -> None:
    filesystem = SimFilesystem()
    await filesystem.mkdir(DATA_DIRECTORY, parents=True, exist_ok=True)
    await filesystem.atomic_write(STORE_PATH, contents)
    store, logger = make_store(filesystem)

    assert await store.load() == []
    assert [type(entry) for entry in logger.entries] == [ServerWarning]


@pytest.mark.asyncio
async def test_a_failed_save_is_logged_and_the_peer_stays_joined() -> None:
    store, logger = make_store(FailingWriteFilesystem())

    await store.add([GATE_PEER])

    assert [type(entry) for entry in logger.entries] == [ServerError]
    assert "will not reconnect" in logger.entries[0].message

    # Known in memory: adding it again is a no-op, not a second failed save.
    await store.add([GATE_PEER])
    assert len(logger.entries) == 1
