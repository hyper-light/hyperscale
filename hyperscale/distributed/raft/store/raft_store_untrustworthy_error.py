class RaftStoreUntrustworthyError(Exception):
    """A Raft store's contents cannot be explained by what this node
    wrote and a power loss: damage before its last record, a record from
    another identity, or a record that breaks Raft's invariants. Such a
    store is set aside, never resumed (D1 P5)."""
