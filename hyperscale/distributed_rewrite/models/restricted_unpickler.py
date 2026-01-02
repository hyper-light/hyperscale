"""
Restricted Unpickler for secure deserialization.

This module provides a secure unpickler that restricts which modules and
classes can be loaded during deserialization. This prevents arbitrary code
execution attacks via malicious pickle payloads.

Security Model:
- ALLOW: Safe standard library modules (typing, collections, asyncio, etc.)
- ALLOW: All hyperscale.* modules
- ALLOW: cloudpickle internals (required for class reconstruction)
- BLOCK: Dangerous modules (os, subprocess, sys, shutil, etc.)
- BLOCK: Dangerous builtins (eval, exec, compile, open, __import__)

Usage:
    from hyperscale.distributed_rewrite.models.restricted_unpickler import restricted_loads
    
    # Instead of cloudpickle.loads(data)
    obj = restricted_loads(data)
"""

import io
import pickle
from typing import Any, FrozenSet, Tuple


class SecurityError(Exception):
    """Raised when deserialization attempts to load blocked modules/classes."""
    pass


# Modules that are completely blocked - no classes from these can be loaded
BLOCKED_MODULES: FrozenSet[str] = frozenset([
    # Command execution
    'os',
    'subprocess',
    'commands',
    'popen2',
    
    # Code execution
    'code',
    'codeop',
    
    # System access
    'sys',
    'ctypes',
    'ctypes.util',
    
    # File operations (beyond what workflows need)
    'shutil',
    'pathlib',  # Can be used for path traversal
    
    # Note: socket is intentionally ALLOWED - hyperscale is a networking framework
    
    # Process/thread manipulation
    'multiprocessing',
    'threading',
    '_thread',
    'concurrent.futures',
    
    # Dangerous introspection
    'gc',
    'inspect',  # Can be used to access frames/locals
    
    # Import manipulation
    'importlib',
    'importlib.util',
    'pkgutil',
    
    # Misc dangerous
    'pty',
    'tty',
    'termios',
    'resource',
    'signal',
    'fcntl',
    'mmap',
    'sysconfig',
    'platform',
])

# Specific module.class combinations that are blocked even if module is allowed
BLOCKED_CLASSES: FrozenSet[Tuple[str, str]] = frozenset([
    # Builtins that execute code
    ('builtins', 'eval'),
    ('builtins', 'exec'),
    ('builtins', 'compile'),
    ('builtins', 'open'),
    ('builtins', '__import__'),
    ('builtins', 'input'),
    ('builtins', 'breakpoint'),
    
    # Alternative names
    ('__builtin__', 'eval'),
    ('__builtin__', 'exec'),
    ('__builtin__', 'compile'),
    ('__builtin__', 'open'),
    ('__builtin__', '__import__'),
    ('__builtin__', 'file'),
    ('__builtin__', 'execfile'),
    
    # io module - block file operations
    ('io', 'open'),
    ('io', 'FileIO'),
    ('io', 'RawIOBase'),
    ('_io', 'open'),
    ('_io', 'FileIO'),
])

# Modules that are explicitly allowed (safe standard library)
ALLOWED_MODULES: FrozenSet[str] = frozenset([
    # Core types
    'builtins',
    '__builtin__',
    
    # Collections and data structures
    'collections',
    'collections.abc',
    'array',
    'heapq',
    'bisect',
    'queue',
    
    # Typing
    'typing',
    'typing_extensions',
    'types',
    
    # Async
    'asyncio',
    'asyncio.tasks',
    'asyncio.futures',
    'asyncio.coroutines',
    'asyncio.locks',
    'asyncio.events',
    'asyncio.queues',
    'asyncio.streams',
    'asyncio.subprocess',
    'asyncio.base_events',
    'asyncio.protocols',
    'asyncio.transports',
    'asyncio.exceptions',
    '_asyncio',  # C extension for asyncio (Future, Task, etc.)
    
    # Time/dates
    'datetime',
    'time',
    'calendar',
    'zoneinfo',
    
    # Numbers and math
    'decimal',
    'fractions',
    'numbers',
    'math',
    'cmath',
    'statistics',
    'random',
    
    # String/text processing
    'string',
    're',
    'difflib',
    'textwrap',
    'unicodedata',
    
    # Data formats
    'json',
    'base64',
    'binascii',
    'struct',
    'codecs',
    
    # Functional programming
    'functools',
    'itertools',
    'operator',
    
    # Context managers
    'contextlib',
    
    # Enum
    'enum',
    
    # Dataclasses
    'dataclasses',
    
    # ABC
    'abc',
    
    # Weakref (needed for some patterns)
    'weakref',
    
    # Copy (needed for deep copy patterns)
    'copy',
    
    # UUID
    'uuid',
    
    # Hashlib (for hashing, not crypto operations)
    'hashlib',
    
    # Logging (needed by workflows)
    'logging',
    
    # Warnings
    'warnings',
    
    # cloudpickle internals (required for deserialization)
    'cloudpickle',
    'cloudpickle.cloudpickle',
    'cloudpickle.cloudpickle_fast',
    
    # pickle itself
    'pickle',
    '_pickle',
    
    # copyreg (needed for pickle)
    'copyreg',
    
    # Pydantic (used in hyperscale)
    'pydantic',
    'pydantic.main',
    'pydantic.fields',
    'pydantic.types',
    
    # msgspec (used in hyperscale)
    'msgspec',
    
    # zstandard (used for compression)
    'zstandard',
    
    # Requests/HTTP (for workflow HTTP clients)
    'urllib',
    'urllib.parse',
    'http',
    'http.client',
    'http.cookies',
    
    # SSL (for HTTPS)
    'ssl',
    '_ssl',
    
    # Socket (hyperscale is a networking framework)
    'socket',
    '_socket',
    
    # Async HTTP libs commonly used
    'aiohttp',
    'httpx',
    
    # Core hyperscale dependencies (base modules)
    'networkx',
    'psutil',
    'attr',
    'attrs',
    'orjson',
    'numpy',
    'aiodns',
    'aioquic',
    
    # Common Python C extensions (base)
    '_collections',
    '_collections_abc',
    '_socket',
    '_json',
    '_struct',
    '_codecs',
    '_datetime',
    '_decimal',
    '_random',
    '_bisect',
    '_heapq',
    '_functools',
    '_operator',
    '_weakref',
    '_weakrefset',
    '_io',
    # Note: _thread, posix, nt are intentionally NOT allowed (OS access)
    
    # HTTP/network related
    'multidict',
    'yarl',
    'frozenlist',
    'aiosignal',
    'idna',
    'certifi',
    'charset_normalizer',
    
    # Crypto
    'cffi',
    '_cffi_backend',
    'nacl',
    'bcrypt',
    
    # Other common
    'annotated_types',
])

# Module prefixes that are allowed (e.g., hyperscale.*)
ALLOWED_MODULE_PREFIXES: Tuple[str, ...] = (
    # Hyperscale
    'hyperscale.',
    
    # Python standard library (submodules)
    'asyncio.',
    '_asyncio.',
    'collections.',
    'logging.',
    'unittest.',
    'email.',
    'html.',
    'http.',
    'urllib.',
    'xml.',
    'json.',
    
    # Core hyperscale dependencies
    'pydantic.',
    'pydantic_core.',
    'msgspec.',
    'cloudpickle.',
    'networkx.',
    'psutil.',
    'zstandard.',
    'cryptography.',
    'attr.',
    'attrs.',
    'orjson.',
    'numpy.',
    'dotenv.',
    'aiodns.',
    'aioquic.',
    
    # Optional hyperscale dependencies (clients & reporters)
    'aiohttp.',
    'httpx.',
    'grpc.',
    'grpcio.',
    'gql.',
    'graphql.',
    'playwright.',
    'azure.',
    'libhoney.',
    'influxdb_client.',
    'newrelic.',
    'aio_statsd.',
    'prometheus_client.',
    'prometheus_api_client.',
    'cassandra.',
    'datadog.',
    'motor.',
    'redis.',
    'aioredis.',
    'aiomysql.',
    'psycopg2.',
    'asyncpg.',
    'sqlalchemy.',
    'boto3.',
    'botocore.',
    'snowflake.',
    'google.',
    'dicttoxml.',
    'xmltodict.',
    'opentelemetry.',
    'aiokafka.',
    'haralyzer.',
    'aiosonic.',
    'bcrypt.',
    'fido2.',
    'gssapi.',
    'libnacl.',
    'pkcs11.',
    'OpenSSL.',
    'pyOpenSSL.',
    'nacl.',
    'cffi.',
    '_cffi_backend.',
    
    # Common Python C extensions
    '_collections.',
    '_socket.',
    '_ssl.',
    '_json.',
    '_struct.',
    '_codecs.',
    '_datetime.',
    '_decimal.',
    '_random.',
    '_bisect.',
    '_heapq.',
    
    # Multidict/yarl (used by aiohttp)
    'multidict.',
    'yarl.',
    'frozenlist.',
    'aiosignal.',
    'idna.',
    'certifi.',
    'charset_normalizer.',
)


class RestrictedUnpickler(pickle.Unpickler):
    """
    A restricted unpickler that only allows safe modules and classes.
    
    This prevents arbitrary code execution by blocking dangerous modules
    like os, subprocess, sys, etc.
    """
    
    def find_class(self, module: str, name: str) -> Any:
        """
        Override to restrict which classes can be loaded.
        
        Args:
            module: The module name containing the class
            name: The class/function name
            
        Returns:
            The class/function if allowed
            
        Raises:
            SecurityError: If the module or class is blocked
        """
        # Check for blocked module.class combinations first
        if (module, name) in BLOCKED_CLASSES:
            raise SecurityError(
                f"Blocked dangerous class: {module}.{name}"
            )
        
        # Check if module is completely blocked
        if module in BLOCKED_MODULES:
            raise SecurityError(
                f"Blocked dangerous module: {module}"
            )
        
        # Check module prefixes for blocked modules
        for blocked in BLOCKED_MODULES:
            if module.startswith(blocked + '.'):
                raise SecurityError(
                    f"Blocked dangerous module: {module}"
                )
        
        # Allow explicitly listed modules
        if module in ALLOWED_MODULES:
            return super().find_class(module, name)
        
        # Allow modules matching allowed prefixes
        for prefix in ALLOWED_MODULE_PREFIXES:
            if module.startswith(prefix):
                return super().find_class(module, name)
        
        # Allow __main__ (user's workflow module)
        if module == '__main__':
            return super().find_class(module, name)
        
        # Block everything else
        raise SecurityError(
            f"Module not in allowlist: {module}.{name}. "
            f"Only hyperscale.* and safe standard library modules are allowed."
        )


def restricted_loads(data: bytes) -> Any:
    """
    Securely deserialize data using the restricted unpickler.
    
    This is a drop-in replacement for cloudpickle.loads() that restricts
    which modules can be loaded during deserialization.
    
    Args:
        data: Pickled data bytes
        
    Returns:
        The deserialized object
        
    Raises:
        SecurityError: If the data tries to load blocked modules/classes
        pickle.UnpicklingError: For other unpickling errors
    """
    return RestrictedUnpickler(io.BytesIO(data)).load()

