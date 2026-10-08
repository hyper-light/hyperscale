"""
Phase 7 ratcheted snapshot — production files under
``hyperscale/distributed/`` and ``hyperscale/logging/`` that still
contain a direct disk call outside the ``Filesystem`` seam.

The accompanying lint
(``tests/simulation/lints/test_no_direct_disk_io.py``) fails when the
discovered set differs from this snapshot in either direction, so the
snapshot must always reflect current state exactly and may only shrink.

Current entries, each deliberate:

- ``hyperscale/logging/streams/logger_stream.py`` — the stdout/stderr
  console-stream setup dups terminal file descriptors via
  ``os.fdopen``. That is TERMINAL transport plumbing, not disk IO with
  a durability contract; under SIM the logger runs disabled or writes
  through the seam, and the console path never executes.
- ``hyperscale/logging/streams/retention_policy.py`` — rotation-policy
  matching globs log directories and reads mtimes. Rotation is already
  documented as outside SIM's scope (retention policies may not run
  under SIM — the same boundary as the zstd-compression executor
  exception), and these metadata reads carry no durability contract.
- ``hyperscale/distributed/env/load_env.py`` — a boot-time
  ``os.path.exists`` on an optional ``.env`` config file, executed at
  process startup before any event loop or SIM swap exists.

Stored as Python (not text) so the project's ``*.txt`` gitignore rule
doesn't hide it from version control.
"""

EXPECTED_DISK_VIOLATIONS: frozenset[str] = frozenset(
    {
        "hyperscale/distributed/env/load_env.py",
        "hyperscale/logging/streams/logger_stream.py",
        "hyperscale/logging/streams/retention_policy.py",
    }
)
