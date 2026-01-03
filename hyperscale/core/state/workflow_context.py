import asyncio
from typing import Any, Dict


class WorkflowContext:
    """
    Context storage for a single workflow with LWW (Last-Write-Wins) semantics.
    
    Used for sharing state between dependent workflows. Updates are applied
    using Lamport timestamps, with source_node as a tiebreaker for concurrent
    updates with the same timestamp.
    
    Thread-safe via async write lock.
    """
    
    def __init__(self) -> None:
        self._context: Dict[str, Any] = {}
        self._timestamps: Dict[str, int] = {}
        self._sources: Dict[str, str] = {}  # key -> source_node for tiebreaking
        self._write_lock = asyncio.Lock()

    def __str__(self) -> str:
        return str(self._context)

    def __repr__(self) -> str:
        return str(self._context)

    def get(self, key: str, default: Any = None):
        return self._context.get(key, default)

    def __getitem__(self, key: str):
        return self._context[key]

    def dict(self) -> Dict[str, Any]:
        """Return the context as a dictionary."""
        return self._context
    
    def get_timestamps(self) -> Dict[str, int]:
        """Return timestamps for all keys (used for context sync)."""
        return self._timestamps.copy()
    
    def get_sources(self) -> Dict[str, str]:
        """Return source nodes for all keys (used for debugging)."""
        return self._sources.copy()

    async def set(
        self,
        key: str,
        value: Any,
        timestamp: int | None = None,
        source_node: str | None = None,
    ):
        """
        Set a context value with LWW conflict resolution.
        
        Args:
            key: The context key
            value: The value to store
            timestamp: Lamport timestamp for ordering (None = always update)
            source_node: Node ID that originated this update (tiebreaker)
        
        LWW Resolution:
            1. If no existing value: accept
            2. If new timestamp > existing: accept
            3. If timestamps equal and source_node > existing source: accept
            4. Otherwise: reject (stale update)
        """
        async with self._write_lock:
            existing_ts = self._timestamps.get(key)
            existing_src = self._sources.get(key)
            
            # Determine if we should update
            if timestamp is None:
                # No timestamp provided = always update (local write)
                should_update = True
            elif existing_ts is None:
                # No existing value = accept
                should_update = True
            elif timestamp > existing_ts:
                # Newer timestamp = accept
                should_update = True
            elif timestamp == existing_ts:
                # Same timestamp: use source_node as tiebreaker
                # Lexicographically greater node_id wins (deterministic)
                if source_node and existing_src:
                    should_update = source_node > existing_src
                elif source_node:
                    # New has source, existing doesn't = new wins
                    should_update = True
                else:
                    # Neither has source or only existing has = reject
                    should_update = False
            else:
                # Older timestamp = reject
                should_update = False
            
            if should_update:
                self._context[key] = value
                if timestamp is not None:
                    self._timestamps[key] = timestamp
                if source_node is not None:
                    self._sources[key] = source_node

    def items(self):
        return self._context.items()
