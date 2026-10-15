# Architecture

people-index joins a person feed with an organization feed and publishes one searchable
Elasticsearch index. It is built to run on many nodes from the first line: every stage is a set
of independent, idempotent tasks over hash partitions, the only shared state is an object store
(S3, MinIO, or a local directory for one machine), and Elasticsearch is a multi-node cluster
with dedicated master nodes.

This document is the contract between the crates, the deployment and the docs. When code and
this file disagree, one of them is wrong: fix it in the same change.

## The data

Both feeds are gzipped NDJSON (`*.ndjson.gz`), one envelope per line:

```json
{"id": 1234, "updated_at": "2026-01-31T12:00:00Z", "record": { ... }}
```

`id` is a non-negative 64-bit integer, or a string of decimal digits that is one. `record` must
be an object. Anything else on a line is *malformed*: counted, quarantined with its origin, and
the run fails only past `max_malformed` per feed.

**Organization record:** `id`, `name`, `domain`, `industry`, `country`, `city`, `size_range`,
`employee_count`, `founded_year`, `keywords[]`, `technologies[]`, `description`, `addresses[]`,
`locations[]`, `funding_rounds[]`.

**Person record:** `id`, `first_name`, `last_name`, `headline`, `summary`, `country`, `region`,
`city`, `industry`, `profile_url`, `skills[]`, `connections`, `followers`, `open_to_work`,
`roles[]`, `memberships[]`, `educations[]`, `certifications[]`, `languages[]`.
A role is `{id, title, organization_id, organization_name, organization_domain,
employment_type, start_date, end_date, location, description}`; `organization_id` may be null.

Unknown fields are carried through untouched (the record is kept as raw JSON; only the fields
the join needs are parsed).

## The output document

The person record, plus:

- `organizations[]`: the full organization record for each distinct resolved
  `roles[].organization_id`, in order of first appearance in `roles[]`;
- `roles[].organization_resolved`: `true` when the id resolved, `false` when it did not, absent
  when the id is null (a null id is ordinary data, not a dangling reference);
- `unresolved_organization_ids[]`: the distinct ids that did not resolve, in role order; absent
  when there are none.

Organization bodies are spliced in as raw JSON; they are never re-parsed on the person side.
Duplicate ids resolve to the **last record in feed order** (input files sorted by key, then
line number), for organizations and for persons alike, whatever the partition or worker count.
The document `_id` is the person id, so a resend is idempotent.

The explicit mapping lives in `crates/pi-es/src/mapping.json` (`dynamic: false`). Term queries on
`roles.title.keyword` and `organizations.name.keyword` are part of the acceptance check.

## Stages

A run is a DAG of stages. A stage is a list of tasks; a task reads committed inputs, writes its
outputs, and commits. Stages run in this order, each starting once the previous one has
committed every task:

| stage | one task per | reads | writes |
|---|---|---|---|
| `plan` | run | the input listing | `plan.json`: input files, partition count `P`, index name |
| `extract-orgs` | organization file | `raw/organizations/<file>` | `orgs/p=<p>/` segments of `OrgRow` |
| `extract-persons` | person file | `raw/persons/<file>` | `persons/q=<q>/` `PersonRow`, `edges/p=<p>/` `EdgeRow`, `quarantine/` |
| `join` | partition `p` | `orgs/p`, `edges/p` | `joined/q=<q>/` segments of `JoinedRow` |
| `assemble` | partition `q` | `persons/q`, `joined/q` | `docs/q=<q>/` segments of `DocRow` |
| `prepare-index` | run | cluster state | the run's index, load-time settings |
| `load` | partition `q` | `docs/q` | bulk requests into the run's index |
| `publish` | run | the run's index | settings restored, count checked, alias moved, `summary.json` |

- `p = xxh3_64(org_id as u64 little-endian) % P` and `q = xxh3_64(person_id LE) % P`
  (`xxhash-rust`, seed 0). One `P` for both keys keeps the plan simple.
- `plan` picks `P` from the total input bytes and `partition_target_bytes` (default 256 MiB),
  at least `min_partitions` (default 16), rounded up to a power of two. A task's memory is
  bounded by its partition, so more data means more partitions, not bigger workers.
- `join` holds one org partition in memory (deduplicated last-wins) and streams the edges;
  `assemble` holds one partition's joined rows (grouped by person) and streams the persons
  (deduplicated last-wins). Both are the two halves of a partitioned hash join: no node ever
  needs the whole organization table.
- `load` sends byte-bounded bulk requests (`bulk_max_bytes`, default 8 MiB) with
  `senders_per_worker` in flight (default 2), retries 429/502/503/504 and transport errors with
  jittered exponential backoff, resends only the failed items, and checks every item's status.
  Total pressure on the cluster is `workers × senders_per_worker`, which is the knob to size
  against the data nodes.
- `prepare-index` creates `<alias>-<run_id>` with `number_of_replicas: 0`,
  `refresh_interval: -1`, `translog.durability: async`, the codec, and `shards` primaries
  (default: 2 per data node, read from `_nodes`). `publish` restores replicas (default 1),
  `refresh_interval: 1s` and request durability, refreshes, checks that the index count equals
  the number of documents `assemble` committed, moves the alias in one `_aliases` request, and
  deletes the indexes the alias pointed at before. A failed run never touches the alias.

## Storage layout

All paths are under one prefix in the object store (`PI_STORE_URL`, e.g. `s3://people-index/`
or `file:///data/store/`):

```
raw/organizations/*.ndjson.gz          inputs (or PI_INPUT_URL, a separate prefix)
raw/persons/*.ndjson.gz
runs/<run_id>/plan.json
runs/<run_id>/stages/<stage>.json      the stage's task list, written by the coordinator
runs/<run_id>/<stage>/_commit/<task>.json
runs/<run_id>/<stage>/_lease/<task>.json
runs/<run_id>/<stage>/data/...         task outputs, named <task>-<attempt>-<n>.seg
runs/<run_id>/quarantine/<task>-<attempt>.ndjson
runs/<run_id>/summary.json
```

### Segment files

A segment is one zstd stream (level 3 by default) of records, each a LEB128 varint length
followed by the `postcard` encoding of the row. Files start with the 8-byte magic `PISEG001`
outside the zstd stream. Writers roll to a new file past `segment_target_bytes` (default
64 MiB compressed). Rows (`crates/pi-model`):

- `OrgRow { org_id: u64, seq: Seq, body: Vec<u8> }`: `body` is the raw JSON of the record.
- `PersonRow { person_id: u64, seq: Seq, body: Vec<u8>, org_ids: Vec<u64> }`: distinct
  non-null organization ids in role order.
- `EdgeRow { org_id: u64, person_id: u64, seq: Seq }`
- `JoinedRow { person_id: u64, org_id: u64, seq: Seq, body: Option<Vec<u8>> }`: `None` means
  unresolved.
- `DocRow { person_id: u64, body: Vec<u8> }`: the final document JSON.
- `Seq { file: u32, line: u64 }`: position in feed order, for last-wins deduplication.

### Commits

A task writes its outputs under a fresh attempt id, then commits by creating
`_commit/<task>.json` with a create-if-absent put (`PutMode::Create`). The commit lists the
exact output keys and the task's counters. **Readers enumerate commits, never data prefixes**,
so a crashed or duplicate attempt's files are invisible; the first commit wins and the loser's
files are orphans that `publish` removes. A task that finds its commit already present returns
at once. This makes every task safe to retry and safe to run twice at the same time.

### Leases (distributed executor)

A worker claims a task by creating `_lease/<task>.json` (`{worker, expires_at}`) with
`PutMode::Create`, renews it every `lease_ttl / 3` with `PutMode::Update` on the etag it holds,
and a worker that finds an expired lease takes it over with `PutMode::Update` on the expired
etag. Conditional updates need an S3-compatible store (S3, MinIO); the local filesystem store
supports only the local executor.

## Components

| crate | what it owns |
|---|---|
| `pi-model` | the schema types, envelope parsing and validation, ids, `Partitioner`, the row types, `DocumentBuilder` (the join's output rules). Pure: no I/O. |
| `pi-segment` | `SegmentWriter` / `SegmentReader` for the segment format, rolling, magic, varint framing. |
| `pi-store` | `Store`: a thin wrapper over `object_store` (S3 / MinIO / local) with put, get, stream, list, create-if-absent and update-if-match; `RunLayout`: every path above, built in one place. |
| `pi-coord` | `TaskSpec`, `StageSpec`, `CommitLog`, `LeaseManager`, the `Stage` trait, the `Executor` trait with `LocalExecutor` (a bounded pool in one process) and `DistributedExecutor` (coordinator + lease-claiming workers), `RunState`. |
| `pi-es` | `EsClient` (reqwest, rustls, basic or API-key auth, custom CA), `IndexAdmin`, `BulkIndexer`, `Publisher`, `Verifier` (expected counts, content digest), the mapping. |
| `pi-stages` | one type per stage implementing `Stage`, and the stage registry. |
| `pi-datagen` | a seeded generator of both feeds with realistic skew (Zipf organization popularity, a third null organization ids, a configurable unresolved and malformed rate, duplicate ids across files) that also writes `expected.json`: exact counts for the acceptance check. |
| `people-index` | the binary: `run`, `worker`, `datagen`, `verify`, `stage` (one task, for debugging). |

The seams are traits so each piece is tested alone: a stage is tested against a local
`Store` without Elasticsearch, the bulk indexer against a mock HTTP server, the executors with
fake stages.

```rust
#[async_trait]
pub trait Stage: Send + Sync {
    fn name(&self) -> StageName;
    /// The tasks of this stage, from the plan and the previous stages' commits.
    async fn tasks(&self, ctx: &RunContext) -> Result<Vec<TaskSpec>>;
    /// Run one task to completion and return what to commit. Must be idempotent.
    async fn run(&self, ctx: &RunContext, task: &TaskSpec, attempt: &AttemptId) -> Result<TaskReport>;
}

#[async_trait]
pub trait Executor: Send + Sync {
    async fn run_stage(&self, stage: Arc<dyn Stage>, ctx: &RunContext) -> Result<StageReport>;
}
```

CPU-bound work (gzip, JSON, zstd) runs on a rayon pool or `spawn_blocking`, never on the async
reactor threads.

## Configuration

Every option is a CLI flag with a `PI_*` environment variable (clap `env`). The ones the
deployment sets:

| variable | meaning | default |
|---|---|---|
| `PI_STORE_URL` | object store prefix | `file://./store` |
| `PI_INPUT_URL` | input prefix, if not `<store>/raw` | unset |
| `PI_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3 / MinIO | unset |
| `PI_ES_URL` | Elasticsearch | `http://localhost:9200` |
| `PI_ES_USERNAME`, `PI_ES_PASSWORD` or `PI_ES_API_KEY` | auth | unset |
| `PI_ES_CA_CERT` | PEM file to trust | unset |
| `PI_ALIAS` | the published alias | `persons` |
| `PI_RUN_ID` | run id | generated (UTC time + random suffix) |
| `PI_EXECUTOR` | `local` or `distributed` | `local` |
| `PI_CONCURRENCY` | tasks at once in this process | number of CPUs |
| `PI_WORKER_ID` | worker name in leases | hostname |
| `PI_LEASE_TTL` | lease lifetime | `60s` |
| `PI_PARTITION_TARGET_BYTES`, `PI_MIN_PARTITIONS` | partitioning | 256 MiB, 16 |
| `PI_BULK_MAX_BYTES`, `PI_SENDERS_PER_WORKER` | load pressure | 8 MiB, 2 |
| `PI_INDEX_SHARDS`, `PI_INDEX_REPLICAS`, `PI_INDEX_CODEC` | the index | 2 per data node, 1, `best_compression` |
| `PI_MAX_MALFORMED` | per feed | 1000 |
| `PI_METRICS_ADDR` | Prometheus endpoint | `0.0.0.0:9464` |
| `PI_LOG_FORMAT` | `json` or `pretty` | `json` |

## Commands

- `people-index run`: the coordinator. With `PI_EXECUTOR=local` it runs every stage itself;
  with `distributed` it writes each stage's task list, runs the single-task stages, waits for
  the workers' commits, and moves on. Exits 0 only after `publish`.
- `people-index worker`: claims and runs tasks of the current stage of `--run-id` (or of the
  newest run) until the run is published or failed, then exits 0.
- `people-index datagen --persons N --organizations M --files F --seed S --out <url>`.
- `people-index verify --expected <file>`: runs the acceptance queries against the alias, and
  `--digest` folds every `_id` and `_source` into one hash.
- `people-index stage <name> --run-id R --task T`: one task, for debugging.

## Deployment

- **Kubernetes** (`deploy/k8s`, Kustomize): ECK runs Elasticsearch with three dedicated master
  nodes and a data node set spread across zones (`topology.kubernetes.io/zone`, shard
  allocation awareness); MinIO stands in for S3 locally; the coordinator is a Job and the
  workers a Job with `parallelism: N`, spread across nodes. `deploy/k3d` creates a local
  cluster of one server and three agents labelled with three zones, so the multi-node layout is
  exercised on one machine. Components add monitoring (Elasticsearch exporter, ServiceMonitor,
  dashboards) and snapshots to the object store.
- **Compose** (`deploy/compose`): the same topology on one Docker host: three Elasticsearch
  nodes, MinIO, the coordinator and `--scale worker=N`.

Failure model: a worker that dies loses its lease and another worker takes the task over; a
coordinator that dies is restarted by its Job and resumes from the commits already in the store
(`run` with an existing `PI_RUN_ID` skips committed stages and tasks). Elasticsearch loss of a
data node during the load is absorbed by retries; the count check in `publish` refuses to move
the alias if documents are missing.
