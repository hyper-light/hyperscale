# TigerBeetle client sessions mapped onto hyperscale (rigor row L6)

TigerBeetle treats clients as replicas-of-a-kind: a client REGISTERS a
session in the replicated state machine, every request carries a
strictly monotone per-session request number, the state machine keeps a
per-session REPLY CACHE so a retried request returns the original reply
(exactly-once per session), sessions are EVICTED when too many clients
register, and the whole request/reply history is verified strictly
serializable across client crashes and link faults.

This document maps that table onto hyperscale's job-submission edge —
what corresponds, what deliberately does not, and which committed tests
pin each claim. Companion rows in
`docs/dev/simulation_rigor_checklist.md`: §L (client/edge faults),
especially L1/L2/L4/L6.

## The mapping at a glance

| TigerBeetle | hyperscale | Delta |
|---|---|---|
| session registration in the state machine | none — clients are stateless to the cluster | no registration round, no session capacity limit |
| per-session monotone request number | AD-40 idempotency key `client_id:sequence:nonce`, one per LOGICAL submission | per-submission, not per-session; the server never enforces sequence continuity |
| per-session reply cache in the state machine | `ManagerIdempotencyLedger` (WAL-persisted ack cache) + durable `JobLedger` job records | TTL-bounded window, not session-lifetime |
| session eviction | none (nothing registered to evict) | dead-client state is TTL/cleanup-scoped instead: ledger TTLs, best-effort push drop, 120s orphan scan |
| strict serializability of the request/reply history | `JobStatusOracle` over the client-observed history + the cross-node trace oracle (G3, program in flight) | judged post-run over the deterministic trace — equivalent under replay determinism |
| client crash/restart resumes the session exactly-once | restart = NEW logical client; gen-1 jobs reach durable terminals server-side | the open L4 durable-key aspirational would close this delta |

## Request number ↔ AD-40 idempotency key

`hyperscale/distributed/idempotency/idempotency_key.py` defines the key
as `client_id:sequence:nonce`. `IdempotencyKeyGenerator` draws
`sequence` from a monotone counter and a fresh 8-byte `nonce` per
generator instance; the client constructs it with
`client_id=f"{host}:{port}"`
(`hyperscale/distributed/nodes/client/client.py`, ~line 234).

The load-bearing property is IDENTITY PER LOGICAL SUBMISSION, pinned at
the submission builder (`hyperscale/distributed/nodes/client/
submission.py`, ~line 367): one key is minted per logical `submit_job`
call and the retry loop REUSES the same message across managers and
leader redirects — a cross-manager retry of one call cannot duplicate
the job, because every manager dedups on the same key.

Deltas from TigerBeetle's request numbers:

- Per-submission, not per-session (the L4 delta): there is no
  server-side session window asserting "I have seen requests 1..N from
  this client". Two DIFFERENT logical submissions never dedup against
  each other, and the server does not reject gaps or reordering across
  keys.
- Identity is address-shaped, uniqueness is nonce-shaped: a restarted
  client at the same `host:port` reuses `client_id` and restarts
  `sequence` at 0, but the fresh nonce keeps its keys disjoint from the
  previous incarnation's — deliberate, given restart-as-new-client
  semantics (below).

History: AD-40 landed in `dc7937dd`; the client-side monotone status
order and the oracle that judges it landed in `57592233`.

## Reply cache ↔ manager idempotency ledger + durable JobLedger

The manager's submission chokepoint
(`hyperscale/distributed/nodes/manager/server.py`, `job_submission` at ~line 11017; the ledger check at ~line 11535)
implements the reply-cache contract:

1. A duplicate key with a stored result returns the ORIGINAL serialized
   ack bytes verbatim; a COMMITTED/REJECTED entry without stored bytes
   returns an equivalent accepted/duplicate ack; a PENDING entry
   returns an explicit "retry" — never a second execution.
2. A new key is RESERVED (`check_or_reserve` writes a PENDING entry)
   before dispatch; the ack is `commit`ted / `reject`ed with the
   serialized response afterward.

`hyperscale/distributed/idempotency/manager_ledger.py` persists every
transition through the storage seam (`append_fsync`; since 2026-10-05 an
`HSIL` format header and CRC-checked `[crc32][length][entry]` frames, an
unrecognized file set aside) and replays the WAL on start up to the first
torn or damaged frame, preserving the bytes after it — so the
dedup window survives manager power loss. The disk_full VOPR events
exercise exactly this surface: a manager that cannot persist the
reservation must REJECT the submission loudly
(`tests/simulation/vopr/vopr_runner.py`, the `submit-rejected`
acceptance path).

The durable half of "the reply outlives the conversation" is the
`JobLedger` (`hyperscale/distributed/ledger/job_ledger.py`): accepted
jobs survive restart (`ac1b53fb`), and a restarted manager tells the
TRUTH about recovered ACTIVE jobs — it fails them loudly to the
recorded `requestor_id` rather than pretending they still run
(`93c6c7c6`).

Delta: the ledger is a TTL-bounded cache
(`hyperscale/distributed/idempotency/idempotency_config.py`: pending
60s, committed 300s, rejected 60s, cleanup every 10s), where
TigerBeetle's reply cache lives as long as the session. A duplicate
arriving after TTL expiry re-executes as a new job — exactly-once here
is exactly-once WITHIN THE DEDUP WINDOW, sized to production retry
cadences (~1s), not to arbitrarily late replays.

## Session eviction ↔ none (and dead-client state cleanup)

There is no session table, so there is nothing to evict and no
`session_too_old` error class. What bounds server-side state about
clients instead:

- idempotency entries expire by status TTL (above);
- completion/status pushes to a vanished client are BEST-EFFORT: send
  failures are dropped internally and the manager stays healthy (the
  L4 scenario's assertion);
- the manager's orphan scan (default `orphan_scan_interval_seconds`
  120s, `hyperscale/distributed/env/env.py`) sweeps job state whose
  owners are gone.

SIM caveat, measured by the gate-cluster program (`c8b6cf99`): a
VANISHED client trips a client-orphan virtual-time spin
(`Timeout._on_timeout` re-arms at the same frozen instant) at roughly
vanish+150s — a documented liveness gap in the harness, which is why
the gate VOPR ceiling is 145s and why every soak/VOPR client entry
stays alive to its job's terminal.

## Strict serializability ↔ the oracle pair

- `tests/simulation/oracle/job_status_oracle.py` linearizes the
  client-OBSERVED history against production's own rank table
  (`JobStatusOrder`): ranks never regress (forward skips legal —
  pushes are periodic, polls sample), terminals absorb, `job-finished`
  agrees with the observed terminal and delivers exactly once. Wired
  into every VOPR-style suite and the long-horizon soak
  (`tests/simulation/soak/`), which applies it PER JOB across an
  1800-virtual-second multi-job horizon.
- The cross-node trace oracle (checklist G3, in flight) adds the
  replica-comparison half TigerBeetle gets from state-machine hash
  equality: manager-recorded terminal == client-observed terminal, at
  most one leadership interval holder, exactly-one-DC execution.
- The client's ordering guard
  (`hyperscale/distributed/nodes/client/status_application.py`) absorbs
  late/duplicate pushes; when pushes are LOST, the poll fallback
  converges: `client.py::_poll_gate_for_job_status` walks gate-for-job
  → any gate → ANY MANAGER with a `job_status` query — the gateless
  fallback pinned by `93c6c7c6` (checklist L2 exercises the
  cut-across-completion window).

Because every schedule is deterministic and byte-identically
replayable, post-run trace checking is equivalent to TigerBeetle's
continuous online checking: a violation at any instant is in the log
at that instant, with the seed as its permanent reproducer.

## Client crash/restart ↔ the open L4 delta

TigerBeetle: a restarted client resumes its session; retried requests
hit the reply cache and the history stays exactly-once per session.

hyperscale today (all pinned or in-flight under checklist L4, with the
gate-suite client power-loss scenario committed in `c8b6cf99`):

1. gen-1's in-flight job still reaches its durable terminal on the
   manager — the cluster never depended on the client's liveness;
2. gen-2 is a NEW logical client (fresh nonce): its resubmission is a
   new job with a new key, completing independently;
3. nothing wedges — pushes toward the dead generation drop internally.
   (The c8b6cf99 pins also record the gate-topology second-job
   dispatch gap as a skip-marked aspirational — the reason multi-job
   soaks run GATELESS today.)

The ASPIRATIONAL row (open, deliberately not implemented): a client
that persisted its idempotency key across restart could resume
exactly-once TigerBeetle-style — resubmitting with the SAME key would
dedup in the ledger to the SAME job, and the gateless `job_status`
query would recover the outcome. That requires durable client-side key
storage and a dedup TTL sized to restart windows; until then the
restart delta is the documented, tested behavior above.
