"""``LamportClock`` -- pickled under the namespace
``hyperscale.distributed.server.events.lamport_clock`` (see that module)."""

import asyncio


class LamportClock:
    """
    Basic Lamport logical clock for event ordering.

    Thread-safe via asyncio.Lock. All operations are atomic.

    Usage:
        clock = LamportClock()

        # Local event - increment clock
        time = await clock.increment()

        # Send message with current time
        message = {'data': ..., 'clock': clock.time}

        # Receive message - update clock
        time = await clock.update(message['clock'])

        # Acknowledge - sync without increment
        await clock.ack(received_time)
    """

    __slots__ = ("time", "_lock")

    def __init__(self, initial_time: int = 0):
        self.time: int = initial_time
        self._lock = asyncio.Lock()

    async def increment(self) -> int:
        """
        Increment clock for a local event.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time += 1
            return self.time

    # Alias for increment - used in some contexts
    tick = increment

    async def update(self, received_time: int) -> int:
        """
        Update clock on receiving a message.

        Sets clock to max(received_time, current_time) + 1.

        Args:
            received_time: The sender's clock time.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time = max(received_time, self.time) + 1
            return self.time

    async def ack(self, received_time: int) -> int:
        """
        Acknowledge a message without incrementing.

        Sets clock to max(received_time, current_time).
        Used for responses where we don't want to increment.

        Args:
            received_time: The sender's clock time.

        Returns:
            The new clock time.
        """
        async with self._lock:
            self.time = max(received_time, self.time)
            return self.time

    def compare(self, other_time: int) -> int:
        """
        Compare this clock's time with another.

        Args:
            other_time: Another clock's time.

        Returns:
            -1 if this < other, 0 if equal, 1 if this > other.
        """
        if self.time < other_time:
            return -1
        elif self.time > other_time:
            return 1
        return 0

    def is_stale(self, other_time: int) -> bool:
        """
        Check if another time is stale (older than our current time).

        Args:
            other_time: The time to check.

        Returns:
            True if other_time < self.time (stale), False otherwise.
        """
        return other_time < self.time
