import io
import cloudpickle
from typing import Self

from hyperscale.distributed_rewrite.models.restricted_unpickler import RestrictedUnpickler


class Message:
    """
    Base class for all distributed messages.
    
    Uses restricted unpickling for secure deserialization - only allows
    safe standard library modules and hyperscale.* modules.
    """

    @classmethod
    def load(cls, data: bytes) -> Self:
        """
        Securely deserialize a message using restricted unpickling.
        
        This prevents arbitrary code execution by blocking dangerous
        modules like os, subprocess, sys, etc.
        
        Args:
            data: Pickled message bytes
            
        Returns:
            The deserialized message
            
        Raises:
            SecurityError: If the data tries to load blocked modules/classes
        """
        return RestrictedUnpickler(io.BytesIO(data)).load()
    
    def dump(self) -> bytes:
        """Serialize the message using cloudpickle."""
        return cloudpickle.dumps(self)
