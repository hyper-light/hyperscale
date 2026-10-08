import json
from collections.abc import Callable
from pathlib import Path

from hyperscale.distributed.runtime import Filesystem
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerWarning

from .joined_peer import JoinedPeer

STORE_FORMAT = "hyperscale-joined-peers"
STORE_VERSION = 1


def _validate_format(stored: dict[str, object]) -> None:
    """Refuse a file that is not this store's format and version.

    Raises:
        ValueError: the file is foreign or of another version.
    """
    if stored.get("format") != STORE_FORMAT or stored.get("version") != STORE_VERSION:
        raise ValueError(f"not a {STORE_FORMAT} v{STORE_VERSION} file")


class JoinedPeerStore:
    """
    The nodes a durable node (gate, manager) was joined to at runtime,
    kept in its data directory so a restart reconnects to them.

    Persistence is opportunistic: the join is live whether or not it is
    saved, a failed save is logged, and a missing, foreign or damaged
    file reads as no joined peers -- never as a reason to refuse to
    start.
    """

    __slots__ = ("_path", "_filesystem", "_logger", "_node_host", "_node_port", "_peers")

    def __init__(
        self,
        data_directory: Path,
        filesystem: Filesystem,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
    ) -> None:
        self._path = data_directory / "joined_peers.json"
        self._filesystem = filesystem
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._peers: dict[tuple[str, int], JoinedPeer] = {}

    async def load(self) -> list[JoinedPeer]:
        """The joined peers saved by earlier runs (none when the file is
        missing or cannot be read as this store's format)."""
        try:
            peers = await self._read_peers()

        except (OSError, ValueError, KeyError, IndexError, TypeError) as load_error:
            await self._log(
                ServerWarning,
                f"Joined peers at {self._path} were not loaded (read as none): {load_error}",
            )
            return []

        if peers is None:
            return []

        return self._remember(peers)

    async def _read_peers(self) -> list[JoinedPeer] | None:
        """The peers the saved file holds, or None when there is no file.

        Raises:
            OSError, ValueError, KeyError, IndexError, TypeError: the file
                cannot be read as this store's format.
        """
        if not await self._filesystem.exists(self._path):
            return None

        stored = json.loads(await self._filesystem.read_text(self._path, encoding="utf-8"))
        _validate_format(stored)

        return [
            JoinedPeer(
                datacenter=entry["datacenter"],
                tcp_address=(entry["tcp"][0], int(entry["tcp"][1])),
                udp_address=(entry["udp"][0], int(entry["udp"][1])),
            )
            for entry in stored["peers"]
        ]

    def _remember(self, peers: list[JoinedPeer]) -> list[JoinedPeer]:
        """Hold the loaded peers as the known set; returns them."""
        self._peers = {peer.tcp_address: peer for peer in peers}
        return peers

    async def add(self, peers: list[JoinedPeer]) -> None:
        """Record newly joined peers and save the whole set."""
        new_peers = self._changed_peers(peers)
        if not new_peers:
            return

        self._peers.update((peer.tcp_address, peer) for peer in new_peers)
        await self._save(
            lambda save_error: (
                f"Joined peers were not saved to {self._path} ({save_error}); "
                "this node will not reconnect to them after a restart"
            )
        )

    def _changed_peers(self, peers: list[JoinedPeer]) -> list[JoinedPeer]:
        """The peers not already recorded exactly as given."""
        return [peer for peer in peers if self._peers.get(peer.tcp_address) != peer]

    async def remove(self, tcp_addresses: list[tuple[str, int]]) -> None:
        """Forget peers that left their cluster (a resize took their
        address out of its cohort) and save the rest: a restart must not
        reconnect to them."""
        removed = self._known_addresses(tcp_addresses)
        if not removed:
            return

        for address in removed:
            del self._peers[address]
        await self._save(
            lambda save_error: (
                f"Departed peers were not removed from {self._path} ({save_error}); "
                "a restart may try to reconnect to them"
            )
        )

    def _known_addresses(self, tcp_addresses: list[tuple[str, int]]) -> list[tuple[str, int]]:
        """The given addresses this store records a peer at."""
        return [address for address in tcp_addresses if address in self._peers]

    async def _save(self, failure_message: Callable[[OSError], str]) -> None:
        """Save the whole set; a failed save is logged as ``failure_message``
        describes it (persistence is opportunistic)."""
        stored = {
            "format": STORE_FORMAT,
            "version": STORE_VERSION,
            "peers": [
                {
                    "datacenter": peer.datacenter,
                    "tcp": list(peer.tcp_address),
                    "udp": list(peer.udp_address),
                }
                for peer in self._peers.values()
            ],
        }

        try:
            await self._filesystem.mkdir(self._path.parent, parents=True, exist_ok=True)
            await self._filesystem.atomic_write(self._path, json.dumps(stored).encode("utf-8"))

        except OSError as save_error:
            await self._log(ServerError, failure_message(save_error))

    async def _log(self, model: type[ServerWarning] | type[ServerError], message: str) -> None:
        await self._logger.log(
            model(
                message=message,
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=0,
            )
        )
