"""Deterministic protocol node-id derivation.

The executor protocols originally minted node ids with
``uuid.uuid4().int >> 64`` — fresh OS entropy per process per run, a
direct bypass of the SIM random seam (the exact class already fixed in
``taskex``'s SnowflakeGenerator instancing and banned by
``distributed/jobs/logical_id_generator.py``'s "NOT secrets/uuid4"
rule, but left behind here). Those random ids seeded the
``Provisioner``'s worker-selection set, whose iteration order IS its
content — so which executor shard ran which workflow permuted run to
run, observable whenever a fault window made shards asymmetric: the
measured one-in-a-dozen replay-divergence flake in the chaos VOPR.

A node id here only needs uniqueness across the processes of one
cluster and stability across runs; the listen address provides both.
BLAKE2b keeps the id well-distributed for Snowflake instance packing
without depending on ``PYTHONHASHSEED``.
"""

import hashlib


def derive_protocol_node_id(host: str, port: int) -> int:
    """63-bit non-negative node id, a pure function of the bind address."""
    address_digest = hashlib.blake2b(
        f"{host}:{port}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(address_digest, "big") >> 1
