# Hyperscale Test Servers

This directory contains test server scripts for each node type in the distributed Hyperscale system.

## Quick Start

Start a minimal cluster with 3 managers and 2 workers:

```bash
# Terminal 1 - Manager 1
python manager_1.py

# Terminal 2 - Manager 2
python manager_2.py

# Terminal 3 - Manager 3
python manager_3.py

# Terminal 4 - Worker 1
python worker_1.py

# Terminal 5 - Worker 2
python worker_2.py
```

## Port Allocation

| Node Type | Server | TCP Port | UDP Port |
|-----------|--------|----------|----------|
| Manager   | 1      | 9000     | 9001     |
| Manager   | 2      | 9002     | 9003     |
| Manager   | 3      | 9004     | 9005     |
| Manager   | 4      | 9006     | 9007     |
| Manager   | 5      | 9008     | 9009     |
| Gate      | 1      | 9100     | 9101     |
| Gate      | 2      | 9102     | 9103     |
| Gate      | 3      | 9104     | 9105     |
| Gate      | 4      | 9106     | 9107     |
| Gate      | 5      | 9108     | 9109     |
| Worker    | 1      | 9200     | 9201     |
| Worker    | 2      | 9202     | 9203     |
| Worker    | 3      | 9204     | 9205     |
| Worker    | 4      | 9206     | 9207     |
| Worker    | 5      | 9208     | 9209     |

## Node Types

### Managers

Managers form a SWIM cluster with leader election:
- Coordinate workflow dispatch to workers
- Track worker capacity and health
- Form quorum for job acceptance
- Report status to gates (if present)

### Gates

Gates are optional - used for multi-datacenter deployments:
- Route jobs to datacenters
- Form their own SWIM cluster
- Track datacenter health
- Manage at-most-once leases

### Workers

Workers execute workflows:
- Register with managers
- Report capacity (CPU cores)
- Execute dispatched workflows
- Report progress back to managers

## Topology

```
                    ┌─────────────────────────────────────┐
                    │             Gates (Optional)         │
                    │   gate_1  gate_2  gate_3  ...       │
                    │   :9100   :9102   :9104             │
                    └─────────────┬───────────────────────┘
                                  │ Job Dispatch
                                  ▼
┌─────────────────────────────────────────────────────────────┐
│                    Managers (DC-EAST)                        │
│   manager_1  manager_2  manager_3  manager_4  manager_5     │
│   :9000      :9002      :9004      :9006      :9008         │
│                         │                                    │
│                    SWIM Cluster                              │
│                   Leader Election                            │
└─────────────────────────┬───────────────────────────────────┘
                          │ Workflow Dispatch
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                        Workers                               │
│   worker_1   worker_2   worker_3   worker_4   worker_5      │
│   :9200      :9202      :9204      :9206      :9208         │
│   4 cores    4 cores    4 cores    4 cores    4 cores       │
└─────────────────────────────────────────────────────────────┘
```

## Status Display

Each server displays periodic status updates:
- Leadership status (who is leader)
- Quorum status (active nodes, required quorum)
- Worker/job counts
- Circuit breaker states

Press Ctrl+C to gracefully shut down any server.

