import asyncio
import msgspec
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal
from pydantic import BaseModel, StrictStr
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server import tcp, udp, task
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer

Message = Literal[b'ack', b'nack', b'join', b'leave', b'probe']
Status = Literal[b'JOIN', b'OK', b'SUSPECT', b'DEAD']

Nodes = dict[tuple[str, int], asyncio.Queue[tuple[int, Status]]]
Ctx = dict[Literal['nodes'], Nodes]


@dataclass
class LocalHealthMultiplier:
    """
    Lifeguard Local Health Multiplier (LHM).
    
    Tracks the node's own health state. A score of 0 indicates healthy,
    higher scores indicate potential issues with this node's ability
    to process messages in a timely manner.
    
    The score saturates at max_score to prevent unbounded growth.
    
    Events that increment LHM:
    - Missed nack (failed to respond in time)
    - Failed refutation (suspicion about self received)
    - Probe timeout when we initiated the probe
    
    Events that decrement LHM:
    - Successful probe round completion
    - Successful nack response received
    """
    score: int = 0
    max_score: int = 8  # Saturation limit 'S' from paper
    
    # Scoring weights for different events
    PROBE_TIMEOUT_PENALTY: int = 1
    REFUTATION_PENALTY: int = 2
    MISSED_NACK_PENALTY: int = 1
    SUCCESSFUL_PROBE_REWARD: int = 1
    SUCCESSFUL_NACK_REWARD: int = 1
    
    def increment(self, amount: int = 1) -> int:
        """
        Increment LHM score (node health is degrading).
        Returns the new score.
        """
        self.score = min(self.max_score, self.score + amount)
        return self.score
    
    def decrement(self, amount: int = 1) -> int:
        """
        Decrement LHM score (node health is improving).
        Returns the new score.
        """
        self.score = max(0, self.score - amount)
        return self.score
    
    def on_probe_timeout(self) -> int:
        """Called when a probe we sent times out."""
        return self.increment(self.PROBE_TIMEOUT_PENALTY)
    
    def on_refutation_needed(self) -> int:
        """Called when we receive a suspicion about ourselves."""
        return self.increment(self.REFUTATION_PENALTY)
    
    def on_missed_nack(self) -> int:
        """Called when we failed to respond in time."""
        return self.increment(self.MISSED_NACK_PENALTY)
    
    def on_successful_probe(self) -> int:
        """Called when a probe round completes successfully."""
        return self.decrement(self.SUCCESSFUL_PROBE_REWARD)
    
    def on_successful_nack(self) -> int:
        """Called when we successfully respond with a nack."""
        return self.decrement(self.SUCCESSFUL_NACK_REWARD)
    
    def get_multiplier(self) -> float:
        """
        Get the timeout multiplier based on current health score.
        Returns a value >= 1.0 that should multiply base timeouts.
        """
        # Linear scaling: healthy (0) = 1x, max unhealthy = 2x
        return 1.0 + (self.score / self.max_score)
    
    def is_healthy(self) -> bool:
        """Returns True if the node considers itself healthy."""
        return self.score == 0
    
    def reset(self) -> None:
        """Reset health score to 0 (healthy)."""
        self.score = 0


@dataclass
class NodeState:
    """
    Tracks the state of a known node in the SWIM membership.
    
    Includes status, incarnation number, and timing information
    for the suspicion subprotocol.
    """
    status: Status = b'OK'
    incarnation: int = 0
    last_update_time: float = 0.0
    
    def update(self, new_status: Status, new_incarnation: int, timestamp: float) -> bool:
        """
        Update node state if the new information is fresher.
        Returns True if the state was updated, False if ignored.
        
        Per SWIM protocol:
        - Higher incarnation always wins
        - Same incarnation: DEAD > SUSPECT > OK
        - Lower incarnation is always ignored
        """
        if new_incarnation > self.incarnation:
            self.status = new_status
            self.incarnation = new_incarnation
            self.last_update_time = timestamp
            return True
        elif new_incarnation == self.incarnation:
            # Same incarnation - apply status priority
            status_priority = {b'OK': 0, b'JOIN': 0, b'SUSPECT': 1, b'DEAD': 2}
            if status_priority.get(new_status, 0) > status_priority.get(self.status, 0):
                self.status = new_status
                self.last_update_time = timestamp
                return True
        return False


@dataclass 
class IncarnationTracker:
    """
    Tracks incarnation numbers for SWIM protocol.
    
    Each node maintains:
    - Its own incarnation number (incremented on refutation)
    - Known incarnation numbers for all other nodes
    
    Incarnation numbers are used to:
    - Order messages about the same node
    - Allow refutation of false suspicions
    - Prevent old messages from overriding newer state
    """
    self_incarnation: int = 0
    node_states: dict[tuple[str, int], NodeState] = field(default_factory=dict)
    
    def get_self_incarnation(self) -> int:
        """Get current incarnation number for this node."""
        return self.self_incarnation
    
    def increment_self_incarnation(self) -> int:
        """
        Increment own incarnation number.
        Called when refuting a suspicion about ourselves.
        Returns the new incarnation number.
        """
        self.self_incarnation += 1
        return self.self_incarnation
    
    def get_node_state(self, node: tuple[str, int]) -> NodeState | None:
        """Get the current state for a known node."""
        return self.node_states.get(node)
    
    def get_node_incarnation(self, node: tuple[str, int]) -> int:
        """Get the incarnation number for a node, or 0 if unknown."""
        state = self.node_states.get(node)
        return state.incarnation if state else 0
    
    def update_node(
        self, 
        node: tuple[str, int], 
        status: Status, 
        incarnation: int,
        timestamp: float,
    ) -> bool:
        """
        Update the state of a node.
        Returns True if the state was updated, False if the message was stale.
        """
        if node not in self.node_states:
            self.node_states[node] = NodeState(
                status=status,
                incarnation=incarnation,
                last_update_time=timestamp,
            )
            return True
        return self.node_states[node].update(status, incarnation, timestamp)
    
    def remove_node(self, node: tuple[str, int]) -> bool:
        """Remove a node from tracking. Returns True if it existed."""
        if node in self.node_states:
            del self.node_states[node]
            return True
        return False
    
    def get_all_nodes(self) -> list[tuple[tuple[str, int], NodeState]]:
        """Get all known nodes and their states."""
        return list(self.node_states.items())
    
    def is_message_fresh(
        self, 
        node: tuple[str, int], 
        incarnation: int, 
        status: Status,
    ) -> bool:
        """
        Check if a message about a node is fresh (should be processed).
        
        A message is fresh if:
        - We don't know about the node yet
        - It has a higher incarnation number
        - Same incarnation but higher priority status
        """
        state = self.node_states.get(node)
        if state is None:
            return True
        if incarnation > state.incarnation:
            return True
        if incarnation == state.incarnation:
            status_priority = {b'OK': 0, b'JOIN': 0, b'SUSPECT': 1, b'DEAD': 2}
            return status_priority.get(status, 0) > status_priority.get(state.status, 0)
        return False


class TestServer(MercurySyncBaseServer[Ctx]):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_health = LocalHealthMultiplier()
        self._incarnation_tracker = IncarnationTracker()

    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        nodes: Nodes = self._context.read('nodes')

        return [
            (
                host,
                port,
            ) for host, port in nodes if target_host != host and target_port != port
        ]
    
    async def send_if_ok(
        self,
        node: tuple[str, int],
        message: bytes,
    ):
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        _, status = node[-1]
        if status == b'OK':
            self._tasks.run(
                self.send,
                node,
                message,
                timeout=timeout,
            )


    async def poll_node(self, target: tuple[str, int]):
        status: Status = await self._context.read_with_lock(target)
        while self._running and status == b'OK':
            await self.send_if_ok(
                target, 
                b'ack>' + target,
            )

            await asyncio.sleep(
                self._context.read('udp_poll_interval', 1)
            )

            status = await self._context.read_with_lock(target)

    async def increase_failure_detector(self, event_type: str = 'probe_timeout'):
        """
        Increase local health score based on event type.
        Uses the Local Health Multiplier (LHM) from Lifeguard.
        """
        if event_type == 'probe_timeout':
            self._local_health.on_probe_timeout()
        elif event_type == 'refutation':
            self._local_health.on_refutation_needed()
        elif event_type == 'missed_nack':
            self._local_health.on_missed_nack()
        else:
            self._local_health.increment()

    async def decrease_failure_detector(self, event_type: str = 'successful_probe'):
        """
        Decrease local health score based on event type.
        Uses the Local Health Multiplier (LHM) from Lifeguard.
        """
        if event_type == 'successful_probe':
            self._local_health.on_successful_probe()
        elif event_type == 'successful_nack':
            self._local_health.on_successful_nack()
        else:
            self._local_health.decrement()
    
    def get_lhm_adjusted_timeout(self, base_timeout: float) -> float:
        """
        Get timeout adjusted by Local Health Multiplier.
        When node is unhealthy, timeouts are extended to reduce false positives.
        """
        return base_timeout * self._local_health.get_multiplier()
    
    def get_self_incarnation(self) -> int:
        """Get this node's current incarnation number."""
        return self._incarnation_tracker.get_self_incarnation()
    
    def increment_incarnation(self) -> int:
        """Increment and return this node's incarnation number (for refutation)."""
        return self._incarnation_tracker.increment_self_incarnation()
    
    def encode_message_with_incarnation(
        self, 
        msg_type: bytes, 
        target: tuple[str, int] | None = None,
        incarnation: int | None = None,
    ) -> bytes:
        """
        Encode a SWIM message with incarnation number.
        Format: msg_type:incarnation>target_host:target_port
        """
        inc = incarnation if incarnation is not None else self.get_self_incarnation()
        msg = msg_type + b':' + str(inc).encode()
        if target:
            msg += b'>' + f'{target[0]}:{target[1]}'.encode()
        return msg
    
    def decode_message_with_incarnation(
        self, 
        data: bytes,
    ) -> tuple[bytes, int, tuple[str, int] | None]:
        """
        Decode a SWIM message with incarnation number.
        Returns: (msg_type, incarnation, target or None)
        """
        # Split on '>' first to separate message from target
        parts = data.split(b'>', maxsplit=1)
        msg_part = parts[0]
        
        target = None
        if len(parts) > 1:
            target_str = parts[1].decode()
            host, port = target_str.split(':', maxsplit=1)
            target = (host, int(port))
        
        # Split message part to get type and incarnation
        msg_parts = msg_part.split(b':', maxsplit=1)
        msg_type = msg_parts[0]
        incarnation = int(msg_parts[1].decode()) if len(msg_parts) > 1 else 0
        
        return msg_type, incarnation, target
    
    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
    ) -> bool:
        """Check if a message about a node should be processed."""
        return self._incarnation_tracker.is_message_fresh(node, incarnation, status)
    
    def update_node_state(
        self,
        node: tuple[str, int],
        status: Status,
        incarnation: int,
        timestamp: float,
    ) -> bool:
        """Update the state of a node. Returns True if state changed."""
        return self._incarnation_tracker.update_node(node, status, incarnation, timestamp)

    @udp.send('receive')
    async def send(
        self,
        addr: tuple[str, int],
        message: bytes,
        timeout: int | None = None,
    ) -> bytes:
        return (
            addr,
            message,
            timeout,
        )
    
    @udp.handle('receive')
    async def process(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> Message:
        return data

    
    @udp.receive()
    async def receive(
        self,
        addr: tuple[str, int],
        data: Message,
        clock_time: int,
    ) -> Message:
        try:

            parsed = data.split(b'>', maxsplit=1)
            message = data

            target: tuple[str, int] | None = None
            target_addr: bytes | None = None
            source_addr = f'{addr[0]}:{addr[1]}'
            if len(parsed) > 1:
                message, target_addr = parsed
                host, port = target_addr.decode().split(':', maxsplit=1)
                target = (host, int(port))

            match message:
                case b'ack' | b'nack':

                    if target not in nodes:
                        await self.increase_failure_detector('missed_nack')
                        return b'nack>' + self._udp_addr_slug
                    
                    # Successful ack/nack processing improves our health
                    await self.decrease_failure_detector('successful_nack')
                    return b'ack>' + self._udp_addr_slug
                
                case b'join':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'ack' + b'>' + self._udp_addr_slug
                        
                        self._context.write(target, b'OK')

                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'OK'))
                        # self._task_runner.run(
                        #     self.poll_node,
                        #     target,
                        # )

                        return b'ack>' + self._udp_addr_slug

                case b'leave':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'leave>' + self._udp_addr_slug
                        

                        if target not in nodes:
                            await self.increase_failure_detector('missed_nack')
                            return b'nack>' + self._udp_addr_slug
                        
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'DEAD'))
                        self._context.write('nodes', nodes)

                        return b'ack>' + self._udp_addr_slug
                
                case b'probe':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            # Refute - we're being probed, indicates someone suspects us
                            await self.increase_failure_detector('refutation')
                            return b'ack>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            # We missed something
                            return b'nack>' + self._udp_addr_slug
                        
                        base_timeout = self._context.read('current_timeout')
                        timeout = self.get_lhm_adjusted_timeout(base_timeout)

                        # Tell the suspect node to forward an ack.
                        self._tasks.run(
                            self.send,
                            target,
                            b'ack>' + source_addr.encode(),
                            timeout=timeout,
                        )
                        
                        # Broadcast the suspicion
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])
                            
                        return b'ack'
                    
                case _:
                    return b'nack'
                
        except Exception:
            import traceback
            print(traceback.format_exc())


async def run():
    server = TestServer(
        '127.0.0.1',
        8667,
        8668,
        Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='1s',
        ),
    )

    await server.start_server(init_context={
        'max_probe_timeout': 10,
        'min_probe_timeout': 1,
        'current_timeout': 1,
        'nodes': defaultdict(asyncio.Queue),
        'udp_poll_interval': 1,
    })
    
    loop = asyncio.get_event_loop()
    waiter = loop.create_future()

    await waiter

    await server.shutdown()


asyncio.run(run())