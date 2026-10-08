"""Constants shared by the wire models of
``hyperscale.distributed.models.worker_state`` (see that module)."""


# Field delimiter for serialization
_DELIM = b":"

# Pre-encode reason bytes for workflow reassignment
_REASSIGNMENT_REASON_BYTES_CACHE: dict[str, bytes] = {
    "worker_dead": b"worker_dead",
    "worker_evicted": b"worker_evicted",
    "worker_overloaded": b"worker_overloaded",
    "rebalance": b"rebalance",
}
