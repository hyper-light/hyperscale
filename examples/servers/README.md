# Distributed System Test Servers

This directory contains example test servers for the distributed system components.

## Port Allocation

| Node Type | Instance | TCP Port | UDP Port |
|-----------|----------|----------|----------|
| Manager 1 | 1        | 9000     | 9001     |
| Manager 2 | 2        | 9002     | 9003     |
| Manager 3 | 3        | 9004     | 9005     |
| Manager 4 | 4        | 9006     | 9007     |
| Manager 5 | 5        | 9008     | 9009     |
| Gate 1    | 1        | 9100     | 9101     |
| Gate 2    | 2        | 9102     | 9103     |
| Gate 3    | 3        | 9104     | 9105     |
| Gate 4    | 4        | 9106     | 9107     |
| Gate 5    | 5        | 9108     | 9109     |
| Worker 1  | 1        | 9200     | 9201     |
| Worker 2  | 2        | 9202     | 9203     |
| Worker 3  | 3        | 9204     | 9205     |
| Worker 4  | 4        | 9206     | 9207     |
| Worker 5  | 5        | 9208     | 9209     |

## Running the Test Servers

### Quick Integration Test

Run the cluster test to verify manager connectivity and leader election:

```bash
python examples/servers/test_manager_cluster.py
```

This will:
1. Start 3 managers (ports 9000-9005)
2. Join them into a SWIM cluster
3. Wait for leader election (~18s for 2 election cycles)
4. Verify connectivity, state, leadership, and quorum
5. Clean up

### Running Individual Servers

Start servers in separate terminals (order doesn't matter for managers):

**Terminal 1 - Manager 1:**
```bash
python examples/servers/manager_1.py
```

**Terminal 2 - Manager 2:**
```bash
python examples/servers/manager_2.py
```

**Terminal 3 - Manager 3:**
```bash
python examples/servers/manager_3.py
```

Each manager will:
- Start in SYNCING state
- Join the SWIM cluster with other managers
- Participate in leader election
- Transition to ACTIVE state (counted in quorum)
- Accept worker registrations
- Display periodic status updates

### Adding Workers

Once managers are running, start workers:

```bash
python examples/servers/worker_1.py
python examples/servers/worker_2.py
```

Workers will:
- Register with managers
- Accept workflow dispatch
- Send progress updates
- Participate in SWIM for health monitoring

### Adding Gates

Gates are optional entry points for client requests:

```bash
python examples/servers/gate_1.py
python examples/servers/gate_2.py
```

Gates will:
- Accept job submissions from clients
- Forward to appropriate datacenter managers
- Aggregate job progress
- Elect a gate leader for coordination

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                     SWIM Cluster                         │
                        │  (UDP healthchecks, state embedding, leader election)   │
                        └─────────────────────────────────────────────────────────┘
                                                    │
                    ┌───────────────────────────────┼───────────────────────────────┐
                    │                               │                               │
              ┌─────▼─────┐                   ┌─────▼─────┐                   ┌─────▼─────┐
              │  Gate 1   │                   │  Gate 2   │                   │  Gate 3   │
              │ (leader)  │                   │(follower) │                   │(follower) │
              └─────┬─────┘                   └───────────┘                   └───────────┘
                    │ Job Submissions
                    ▼
    ┌───────────────────────────────────────────────────────────────────────────────────────┐
    │                              Manager Cluster (DC-EAST)                                 │
    ├─────────────────────────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐          ┌─────────────┐          ┌─────────────┐                     │
    │  │  Manager 1  │◄────────►│  Manager 2  │◄────────►│  Manager 3  │                     │
    │  │ (leader)    │  Quorum  │ (follower)  │  Quorum  │ (follower)  │                     │
    │  └──────┬──────┘          └──────┬──────┘          └──────┬──────┘                     │
    │         │ Workflow                │                        │                           │
    │         │ Dispatch                │                        │                           │
    │         ▼                         ▼                        ▼                           │
    │  ┌─────────────┐          ┌─────────────┐          ┌─────────────┐                     │
    │  │  Worker 1   │          │  Worker 2   │          │  Worker 3   │                     │
    │  │ (healthy)   │          │ (healthy)   │          │ (healthy)   │                     │
    │  └─────────────┘          └─────────────┘          └─────────────┘                     │
    └───────────────────────────────────────────────────────────────────────────────────────┘
```

## Configuration

All servers use configurable settings from the `Env` class:

```python
from hyperscale.distributed_rewrite.env import Env

env = Env(
    # SWIM Protocol Settings
    SWIM_MAX_PROBE_TIMEOUT=10,
    SWIM_MIN_PROBE_TIMEOUT=1,
    SWIM_CURRENT_TIMEOUT=2,
    SWIM_UDP_POLL_INTERVAL=2,
    SWIM_SUSPICION_MIN_TIMEOUT=2.0,
    SWIM_SUSPICION_MAX_TIMEOUT=15.0,
    
    # Circuit Breaker Settings
    CIRCUIT_BREAKER_MAX_ERRORS=3,
    CIRCUIT_BREAKER_WINDOW_SECONDS=30.0,
    CIRCUIT_BREAKER_HALF_OPEN_AFTER=10.0,
)
```

## Key Behaviors

### Leader Election

- Uses pre-voting to prevent split-brain scenarios
- Election timeout: 5-7 seconds (base 5s + random jitter)
- Pre-vote timeout: 2 seconds
- Full election cycle: ~7-9 seconds
- If split votes occur, retry with higher term

### Quorum

- Requires majority of ACTIVE managers: `(n // 2) + 1`
- SYNCING managers don't count toward quorum
- Circuit breaker protects against repeated failures

### State Embedding

- Heartbeats embedded in SWIM messages for passive dissemination
- Reduces TCP traffic for status updates
- Leadership changes propagated via SWIM

## Troubleshooting

### No Leader Elected

If no leader is elected after 15-20 seconds:
- Check that all managers can reach each other (UDP ports)
- Verify SWIM `nodes` dict is populated (connectivity check in test)
- Increase wait time for election cycles

### Workers Not Registering

- Ensure managers are in ACTIVE state first
- Check that worker's manager_addrs match manager ports
- Verify TCP connectivity between worker and manager

### Quorum Unavailable

- At least `(n // 2) + 1` managers must be ACTIVE
- SYNCING managers are excluded from quorum
- Check circuit breaker state in quorum status
