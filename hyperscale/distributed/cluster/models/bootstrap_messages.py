"""
Bootstrap handshake messages (AD-52 §4).

Sent only during cold formation. Once the seed membership log entry is
committed, founding nodes transition to JOINED and bootstrap messages
are no longer exchanged. Restarted founding nodes proceed via the join
protocol instead — they cannot replay bootstrap because their peers are
in JOINED state, not BOOTSTRAPPING.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BootstrapHello:
    """
    Initial peer probe during cold formation.

    Fields:
        my_node_id            Operator-assigned founding identifier from
                              the --initial-members list (NOT the
                              uuid4() runtime id — that is `runtime_uuid`
                              below). Used to map founding-member slots
                              to addresses.
        cluster_id            --cluster-id. Mismatches abort bootstrap.
        role                  "gate" | "manager".
        initial_members_hash  sha256 of the sorted, comma-joined
                              --initial-members list. Mismatches across
                              founding nodes abort bootstrap.
        runtime_uuid          The sender's uuid4() runtime id, used in
                              the seed EnterJoint log entry once
                              bootstrap proceeds.
        cluster_size          Operator-supplied --cluster-size; must
                              match initial_members length.
        protocol_version      AD-25 major version. Mismatches abort.
    """

    my_node_id: str
    cluster_id: str
    role: str
    initial_members_hash: str
    runtime_uuid: str
    cluster_size: int
    protocol_version: str
