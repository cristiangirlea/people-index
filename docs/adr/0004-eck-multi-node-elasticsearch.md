# ADR 0004: Multi-node Elasticsearch on ECK, load-time settings, alias publishing

Status: accepted, 2026-10-15

## Context

The output is one searchable index that has to keep serving through the loss of a node, be
replaced atomically by the next run, and be loaded as fast as the cluster allows. A
single-node Elasticsearch is a development convenience, not a deployment: it has no replica
placement, no quorum, and a heavy bulk load competes with cluster-state work on the same node.

## Decision

**Topology** (`deploy/k8s/base/elasticsearch`), run by the ECK operator (3.5.0, manifests pinned
by checksum), Elasticsearch 9.5.5:

- **Three dedicated master nodes.** A quorum survives the loss of one; masters do no indexing,
  so a bulk load on the data nodes never starves cluster-state updates.
- **A data node set** (three in the base, six in the production example) spread across zones
  with topology spread constraints, and **shard allocation awareness** on a `zone` node
  attribute copied from `topology.kubernetes.io/zone` (ECK's downward node labels). A primary
  and its replica never share a zone, so losing a zone loses no data once replicas exist.
- Memory request equal to the limit, heap set explicitly, no CPU limit; TLS on HTTP and
  transport; a file-realm `pipeline` user whose role covers only `persons*`; NetworkPolicies
  that admit only labelled clients; an optional snapshot component (S3 repository under
  `snapshots/`, nightly SLM policy).
- Compose runs three master+data nodes, one per zone attribute, with the same awareness
  setting: the same shape on one host, for development.

**Load-time settings.** `prepare-index` creates a new index `<alias>-<run_id>` per run with
`number_of_replicas: 0`, `refresh_interval: -1`, `translog.durability: async`,
`best_compression`, and 2 primaries per data node (read from `_nodes`). No replicas means each
document is indexed once; no refresh means no small segments during the load.

**Publishing.** `publish` refreshes and flushes while replicas are still 0, so the "every shard
answered" check covers every copy; checks the document count against the sum the `assemble`
commits recorded; then restores replicas, `refresh_interval: 1s` and request durability; waits
for green; moves the alias in one `_aliases` request; and deletes the indexes the alias pointed
at before. Readers only ever query the alias.

## Consequences

- Readers see either the old index or the complete new one, never a partial load; a failed run
  leaves the alias where it was.
- Replicas are built once by Elasticsearch from finished segments, instead of every document
  being indexed twice during the load.
- Async translog during the load can lose the last acknowledged operations if a data node
  crashes; the count check catches it, and the fix is a new run (load tasks are already
  committed).
- Disk use doubles during a run: the old and the new index coexist until publish. After
  publish the old index is deleted, so going back means restoring a snapshot, not moving the
  alias back.
- The shard count is fixed when the index is created; new data nodes help the next run.
- The ECK operator and its CRDs are a cluster prerequisite, and the local overlay needs about
  10 GiB of memory for Docker.
- Not yet verified on a real cluster: ECK reconciliation and NetworkPolicy enforcement. The
  node roles, awareness settings, the writer role and the snapshot setup were checked against
  Elasticsearch containers.
