"""Test fixtures for the distributed simulation harness.

Exists under ``hyperscale.distributed.testing.*`` (rather than ``tests/``)
so that fixtures pickled into network messages survive the
``RestrictedUnpickler`` allowlist on the receiving end. Production code
should never import from here.
"""
