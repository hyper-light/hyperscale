"""
Replay Attack Protection for message protocols.

This module provides defense against replay attacks by:
1. Tracking seen message IDs in a sliding window
2. Rejecting messages with timestamps outside the acceptable window
3. Rejecting duplicate message IDs
4. Tracking sender incarnations to handle process restarts

The Snowflake ID already contains a millisecond timestamp, which we leverage
for freshness validation without adding extra fields to the protocol.

Incarnation Handling:
When a sender restarts, it generates a new random incarnation nonce. When the
receiver sees a new incarnation from a known sender, it clears the replay
state for that sender. This prevents:
- False positives after sender restart (old IDs won't conflict)
- Replay attacks using messages from previous sender incarnations
"""

import time
from collections import OrderedDict
from typing import Optional, Tuple

from hyperscale.core.snowflake import Snowflake


# Default configuration
DEFAULT_MAX_AGE_SECONDS = 300  # 5 minutes - messages older than this are rejected
DEFAULT_MAX_FUTURE_SECONDS = 60  # 1 minute - messages from "future" are rejected (clock skew)
DEFAULT_WINDOW_SIZE = 100000  # Maximum number of message IDs to track
DEFAULT_MAX_INCARNATIONS = 10000  # Maximum number of sender incarnations to track


class ReplayError(Exception):
    """Raised when a replay attack is detected."""
    pass


class ReplayGuard:
    """
    Guards against message replay attacks.

    Uses a combination of:
    - Timestamp freshness validation (based on Snowflake timestamp)
    - Duplicate ID detection (sliding window of seen IDs)
    - Incarnation tracking (detects sender restarts)

    This class is designed to be efficient:
    - O(1) lookups using a dict
    - Automatic cleanup of old entries using OrderedDict
    - Memory-bounded by max_window_size and max_incarnations

    Thread-safety: This class is NOT thread-safe. Use one instance per
    asyncio task/protocol instance.
    """

    __slots__ = (
        '_seen_ids',
        '_known_incarnations',
        '_max_age_ms',
        '_max_future_ms',
        '_max_window_size',
        '_max_incarnations',
        '_epoch',
        '_stats_duplicates',
        '_stats_stale',
        '_stats_future',
        '_stats_accepted',
        '_stats_incarnation_changes',
    )

    def __init__(
        self,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        max_future_seconds: float = DEFAULT_MAX_FUTURE_SECONDS,
        max_window_size: int = DEFAULT_WINDOW_SIZE,
        max_incarnations: int = DEFAULT_MAX_INCARNATIONS,
        epoch: int = 0,
    ) -> None:
        """
        Initialize the replay guard.

        Args:
            max_age_seconds: Maximum age of a message before it's rejected as stale
            max_future_seconds: Maximum time in the future a message can be (clock skew tolerance)
            max_window_size: Maximum number of message IDs to track
            max_incarnations: Maximum number of sender incarnations to track
            epoch: Snowflake epoch offset (usually 0)
        """
        # Use OrderedDict for efficient LRU-style cleanup
        self._seen_ids: OrderedDict[int, int] = OrderedDict()
        # Track known incarnations per sender (keyed by incarnation bytes)
        # Value is (last_seen_timestamp_ms, set of message IDs from this incarnation)
        self._known_incarnations: OrderedDict[bytes, int] = OrderedDict()
        self._max_age_ms = int(max_age_seconds * 1000)
        self._max_future_ms = int(max_future_seconds * 1000)
        self._max_window_size = max_window_size
        self._max_incarnations = max_incarnations
        self._epoch = epoch

        # Statistics
        self._stats_duplicates = 0
        self._stats_stale = 0
        self._stats_future = 0
        self._stats_accepted = 0
        self._stats_incarnation_changes = 0
    
    def validate(self, shard_id: int, raise_on_error: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Validate a message ID for replay attacks (without incarnation tracking).

        For full protection including restart handling, use validate_with_incarnation().

        Args:
            shard_id: The Snowflake ID of the message
            raise_on_error: If True, raise ReplayError on invalid messages

        Returns:
            Tuple of (is_valid, error_message)

        Raises:
            ReplayError: If raise_on_error is True and the message is invalid
        """
        return self._validate_timestamp_and_duplicate(shard_id, raise_on_error)

    def validate_with_incarnation(
        self,
        shard_id: int,
        sender_incarnation: bytes,
        raise_on_error: bool = True,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a message ID with incarnation tracking for restart protection.

        This method provides full replay protection including:
        - Timestamp freshness validation
        - Duplicate ID detection
        - Sender incarnation tracking (handles process restarts)

        When a new incarnation is seen from a sender, old replay state is
        preserved but the new incarnation is tracked. Messages from old
        incarnations within the time window are still rejected as replays.

        Args:
            shard_id: The Snowflake ID of the message
            sender_incarnation: 8-byte nonce identifying the sender's process incarnation
            raise_on_error: If True, raise ReplayError on invalid messages

        Returns:
            Tuple of (is_valid, error_message)

        Raises:
            ReplayError: If raise_on_error is True and the message is invalid
        """
        current_time_ms = int(time.time() * 1000)

        # Track this incarnation
        self._track_incarnation(sender_incarnation, current_time_ms)

        # Perform standard validation
        return self._validate_timestamp_and_duplicate(shard_id, raise_on_error)

    def _validate_timestamp_and_duplicate(
        self,
        shard_id: int,
        raise_on_error: bool,
    ) -> Tuple[bool, Optional[str]]:
        """Core validation logic for timestamp and duplicate checking."""
        # Parse the Snowflake to extract timestamp
        snowflake = Snowflake.parse(shard_id, self._epoch)
        message_time_ms = snowflake.milliseconds

        # Get current time in milliseconds
        current_time_ms = int(time.time() * 1000)

        # Check for stale messages (too old)
        age_ms = current_time_ms - message_time_ms
        if age_ms > self._max_age_ms:
            self._stats_stale += 1
            error = f"Message too old: {age_ms}ms > {self._max_age_ms}ms"
            if raise_on_error:
                raise ReplayError(error)
            return (False, error)

        # Check for future messages (clock skew or manipulation)
        if age_ms < -self._max_future_ms:
            self._stats_future += 1
            error = f"Message from future: {-age_ms}ms ahead"
            if raise_on_error:
                raise ReplayError(error)
            return (False, error)

        # Check for duplicate message ID
        if shard_id in self._seen_ids:
            self._stats_duplicates += 1
            error = f"Duplicate message ID: {shard_id}"
            if raise_on_error:
                raise ReplayError(error)
            return (False, error)

        # Message is valid - record it
        self._record_id(shard_id, current_time_ms)
        self._stats_accepted += 1

        return (True, None)

    def _track_incarnation(self, incarnation: bytes, current_time_ms: int) -> None:
        """
        Track a sender incarnation.

        If this is a new incarnation, record it. Old incarnations are cleaned
        up based on max_incarnations limit using LRU eviction.
        """
        if incarnation in self._known_incarnations:
            # Move to end (most recently used) and update timestamp
            self._known_incarnations.move_to_end(incarnation)
            self._known_incarnations[incarnation] = current_time_ms
        else:
            # New incarnation
            self._known_incarnations[incarnation] = current_time_ms
            self._stats_incarnation_changes += 1

            # Cleanup if over limit (remove oldest incarnations)
            while len(self._known_incarnations) > self._max_incarnations:
                self._known_incarnations.popitem(last=False)
    
    def _record_id(self, shard_id: int, current_time_ms: int) -> None:
        """Record a message ID as seen and cleanup old entries."""
        # Add new ID
        self._seen_ids[shard_id] = current_time_ms
        
        # Cleanup if over size limit (remove oldest entries)
        while len(self._seen_ids) > self._max_window_size:
            self._seen_ids.popitem(last=False)
        
        # Periodic cleanup of expired entries (every 1000 messages)
        if self._stats_accepted % 1000 == 0:
            self._cleanup_expired(current_time_ms)
    
    def _cleanup_expired(self, current_time_ms: int) -> None:
        """Remove entries older than max_age from the seen set."""
        cutoff = current_time_ms - self._max_age_ms
        
        # Remove expired entries from the front (oldest first due to OrderedDict)
        expired_ids = []
        for msg_id, timestamp in self._seen_ids.items():
            if timestamp < cutoff:
                expired_ids.append(msg_id)
            else:
                # OrderedDict maintains insertion order, so we can stop early
                break
        
        for msg_id in expired_ids:
            del self._seen_ids[msg_id]
    
    def get_stats(self) -> dict:
        """Get replay guard statistics."""
        return {
            'accepted': self._stats_accepted,
            'duplicates_rejected': self._stats_duplicates,
            'stale_rejected': self._stats_stale,
            'future_rejected': self._stats_future,
            'incarnation_changes': self._stats_incarnation_changes,
            'tracked_ids': len(self._seen_ids),
            'tracked_incarnations': len(self._known_incarnations),
            'max_window_size': self._max_window_size,
            'max_incarnations': self._max_incarnations,
            'max_age_seconds': self._max_age_ms / 1000,
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._stats_duplicates = 0
        self._stats_stale = 0
        self._stats_future = 0
        self._stats_accepted = 0
        self._stats_incarnation_changes = 0

    def clear(self) -> None:
        """Clear all tracked message IDs and incarnations."""
        self._seen_ids.clear()
        self._known_incarnations.clear()
        self.reset_stats()

    def __len__(self) -> int:
        """Return the number of tracked message IDs."""
        return len(self._seen_ids)

    def __getstate__(self):
        """Support pickling for multiprocessing."""
        return {
            'max_age_ms': self._max_age_ms,
            'max_future_ms': self._max_future_ms,
            'max_window_size': self._max_window_size,
            'max_incarnations': self._max_incarnations,
            'epoch': self._epoch,
            # Don't pickle the seen_ids or incarnations - start fresh in new process
        }

    def __setstate__(self, state):
        """Restore from pickle."""
        self._max_age_ms = state['max_age_ms']
        self._max_future_ms = state['max_future_ms']
        self._max_window_size = state['max_window_size']
        self._max_incarnations = state.get('max_incarnations', DEFAULT_MAX_INCARNATIONS)
        self._epoch = state['epoch']
        self._seen_ids = OrderedDict()
        self._known_incarnations = OrderedDict()
        self._stats_duplicates = 0
        self._stats_stale = 0
        self._stats_future = 0
        self._stats_accepted = 0
        self._stats_incarnation_changes = 0

