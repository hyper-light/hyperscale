# hyperscale

A Hyperscale cluster on Kubernetes: a gate tier, and for each datacenter a
manager cohort and its workers. Every node runs a CLI node command
(`hyperscale run gate|manager|worker`).

## Topology

| Role    | Workload          | Advertised as                   | State                     |
|---------|-------------------|---------------------------------|---------------------------|
| gate    | StatefulSet       | its pod's DNS name              | volume: ledger, WAL       |
| manager | StatefulSet / DC  | its pod's DNS name              | volume: ledger, WAL       |
| worker  | Deployment / DC   | its pod IP                      | none                      |

A node is identified by the exact address it advertises, so gates and
managers advertise their stable pod names: a restarted pod comes back as
the same address. Each manager is given its datacenter's whole cohort
(`--managers`/`--manager-udp`; it skips itself), so `managers.replicas`
fixes the cohort's election quorum. Managers report to every gate
(`--gates`/`--gate-udp`); workers are seeded with every manager of their
datacenter.

## On KIND

```sh
# 1. Build the image from this repository.
docker build -f docker/images/Dockerfile.source -t hyperscale:dev .

# 2. Load it into the cluster's nodes.
kind load docker-image hyperscale:dev --name <cluster>

# 3. Install.
helm install demo helm/hyperscale --namespace hyperscale --create-namespace

# 4. Run one workflow through the gates to completion.
helm test demo --namespace hyperscale --logs
```

## Values

- `datacenters[]`: one entry per datacenter, with `managers.replicas`
  (the cohort size) and `workers.replicas` / `workers.cores`.
- `gates.replicas`: the gate tier's size.
- `clusterSecret`: the secret every node encrypts traffic with;
  generated (and kept across upgrades) when left empty.
- `env`: any Hyperscale setting, as an environment variable on every node.

A client can reach a gate at any address that routes to it -- a pod name,
a Service, a port-forward. The cluster then pushes the job's status and
results to the client's own address (its `host:port`), so that address
must be reachable from the gate pods: run clients inside the cluster, as
the chart's test pod does, or give an outside client an address the pods
can dial.
