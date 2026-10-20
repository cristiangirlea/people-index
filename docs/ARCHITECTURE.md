# Architecture

people-index joins a person feed with an organization feed and publishes one searchable
Elasticsearch index. It is built to run on many nodes from the first line: every stage is a set
of independent, idempotent tasks over hash partitions, the only shared state is an object store
(any S3-compatible server with conditional writes, or a local directory for one machine), and
Elasticsearch is a multi-node cluster with dedicated master nodes.

This document is the contract between the crates, the deployment and the docs. When code and
this file disagree, one of them is wrong: fix it in the same change. The reasons behind the
main choices are in [adr/](adr/).

## The data

Both feeds are gzipped NDJSON (`*.ndjson.gz`), one envelope per line:

```json
{"id": 1234, "updated_at": "2026-01-31T12:00:00Z", "record": { ... }}
```

`id` is a non-negative 64-bit integer, or a string of decimal digits that is one; it is the
person or organization id (the record's own `id` field is carried through, never used as the
id; like any mapped field it is only type-checked, see below).
`record` must be an object. Anything else on a line is *malformed*: counted, quarantined with
its origin, and the run fails past `max_malformed` per feed. Malformed includes an empty or
whitespace-only line, a duplicate `id` or `record` key in the envelope, and a line longer than
`max_line_bytes` (kind `too_long`). A leading byte-order mark and a trailing `\r` are accepted.

**Organization record:** `id`, `name`, `domain`, `industry`, `country`, `city`, `size_range`,
`employee_count`, `founded_year`, `keywords[]`, `technologies[]`, `description`, `addresses[]`,
`locations[]`, `funding_rounds[]`.

**Person record:** `id`, `first_name`, `last_name`, `headline`, `summary`, `country`, `region`,
`city`, `industry`, `profile_url`, `skills[]`, `connections`, `followers`, `open_to_work`,
`roles[]`, `memberships[]`, `educations[]`, `certifications[]`, `languages[]`.
A role is `{id, title, organization_id, organization_name, organization_domain,
employment_type, start_date, end_date, location, description}`; `organization_id` absent or
null means no organization, otherwise it follows the envelope `id` rule (a non-negative 64-bit
integer, or a string of decimal digits that is one; `"7"` and `7` are the same organization).
`roles` absent or null means no roles. A `roles` that is neither an array nor null, a role that
is not an object, or any other `organization_id` makes the line malformed (kind `record`).

Unknown fields are carried through untouched (the record is kept as raw JSON; only the fields
the join needs are parsed).

**A number token longer than 1000 characters** (as written: sign, digits, `.`, exponent)
anywhere in the record, mapped, unknown or inside a disabled object, makes the line malformed
(kind `record`, the reason names the path, as in `addresses[2].zip: a number of 1001
characters; Elasticsearch accepts at most 1000`): Elasticsearch 9.5.5 refuses it while parsing
the document, whatever the field's mapping (observed: 1001 digits fail the bulk item even in a
`text` field or with `ignore_malformed`, 999 are indexed). Counting the sign, point and
exponent too is deliberately strict. Only the numbers the stored body keeps count: one inside
an earlier occurrence of a repeated key, which the body drops (below), does not quarantine the
line, and the reason names the first kept one. `pi-model` notes them in the same pass of its
raw-JSON scanner (`scan`) that finds repeated keys and decides after it; the limit is
`MAX_NUMBER_CHARS`.

**A key repeated in one object**, at any depth (the top level, a role, an object inside an
array, an unknown field, a disabled object like `addresses`), is reduced to its last
occurrence: Elasticsearch 9.5.5 fails the bulk item with "Duplicate field" for a repeat at any
of those places (observed), and the last occurrence is the one every common JSON parser, and
the checks here, read. The same scan pass notes the repeats (`RepeatedKeys`, a second visitor;
keys compare as written and, when one has an escape, decoded, so `"\u006eame"` repeats
`"name"`); a record with one is stored without each repeated key's earlier occurrences,
from its key to the next key of the same object, every other byte as written (key spellings,
value text, whitespace, order); a record without one, nearly all of them, is stored byte for
byte. Such a record is kept, not malformed, and counted
in the extract counter `records_with_repeated_keys` (per extracted line, like
`values_ignored_by_index`).

**Mapped fields must have the JSON kind their mapping expects**, or the record is malformed
(kind `record`, the reason names the field, as in `roles[2].title: expected a string, number
or boolean, found an object`): Elasticsearch would reject the document with a mapper error, and one bad record must
cost that record, not the load. The rule is a table in `pi-model` (`PERSON_FIELDS`,
`ROLE_FIELDS`, `ORGANIZATION_FIELDS`), checked by `extract-persons` (the record and every role)
and `extract-orgs`; a test in `pi-es` keeps it in step with `mapping.json`. Identifiers (`id`,
`roles[].id`, `roles[].organization_id`, the organization's `id`) are strict: a number or a
string the id rule above accepts (decimal digits within `u64`, so `"007"` passes and `"7a"`,
`" 7"`, `"-1"` and `""` do not). Measures (`connections`, `followers`, `open_to_work`, `employee_count`,
`founded_year`) are lenient, since their mapping has `ignore_malformed`: any scalar or array
of scalars passes. Elasticsearch skips a value it cannot read as the field's type: the
malformed values are not indexed and the field is listed in the document's `_ignored`; the
record stays searchable by its other fields, and the value stays in `_source`. Extraction keeps
such a record and counts in the extract counter `values_ignored_by_index` the values
Elasticsearch skips (not malformed, not against `max_malformed`), following the rules
Elasticsearch 9.5.5 was observed to apply (`pi-model`'s `es_number`, pinned in its tests):
- either number type: a boolean is skipped, and so is a string of spaces only; `""` is not;
- `long` (`connections`, `followers`, `employee_count`): a string must be an exact decimal
  `[+-]? (digits [. digits?] | . digits) ([eE] [+-]? digits)?` with no surrounding whitespace
  (`"n/a"`, `" 5"` are skipped), truncated toward zero within `i64`; Java's `BigDecimal` limits
  also skip a string whose exponent is outside `int`, or whose scale (the digits after the
  point, as written, minus the exponent) is outside `int` or below -19, whatever its value
  (`"0e20"`, `"0.0e21"`, `"0.5e-2147483647"` are skipped; `"0e19"`, `"0.0e20"`,
  `"1e-2147483647"` are not); a JSON integer must be within `i64`; a JSON number with a
  fraction or exponent is compared as a double with `±2^63`, so values that round to 2^63
  pass;
- `integer` (`founded_year`): a string is read by Java's `Double.parseDouble` (trimmed; `NaN`,
  `Infinity`, `d`/`f` suffixes and hexadecimal accepted; `NaN` indexes as 0), a JSON number as
  a double, and either is skipped when outside `i32` before truncation (`2147483647.5`). The
  forms named here were observed; the rest of the grammar is derived from Java's;
- `boolean`: anything but `true`, `false`, `"true"`, `"false"` and `""`.

Each element of an array counts on its own. One known divergence: a non-ASCII decimal digit in
a `long` string (`"٥"`) is counted, though Elasticsearch indexes it. The counter counts per
extracted line, before person deduplication, and an organization's value once however many
person documents embed it, so it differs from the number of documents an `exists` query on
`_ignored` finds. Extraction logs the first few such lines per task with file, line, field and
kind, never the value. `text`, `keyword` and `date` fields (the lists `skills`,
`keywords` and `technologies` included) take any scalar, which Elasticsearch reads as text
(`"city": 10001`, `"start_date": 2020`), or an array of scalars. No field but `roles` takes an
object at any depth, `ignore_malformed` or not, and `roles` takes an array of objects. `null`
is allowed everywhere. Disabled objects (`memberships`, `addresses`, ...) take anything, and
the names the builder writes (`organizations`, `unresolved_organization_ids`,
`roles[].organization_resolved`) are replaced, so neither is checked. A value of the right kind
that still does not parse (a number out of range, a date in another format) is not indexed for
that field and stays in `_source` (`ignore_malformed`).

## The output document

The person record, plus:

- `organizations[]`: the full organization record for each distinct resolved
  `roles[].organization_id`, in order of first appearance in `roles[]`; always present, `[]`
  when nothing resolved;
- `roles[].organization_resolved`: `true` when the id resolved, `false` when it did not, absent
  when the id is null (a null id is ordinary data, not a dangling reference);
- `unresolved_organization_ids[]`: the distinct ids that did not resolve, in role order; absent
  when there are none.

These three names are owned by the builder: an input field of the same name is replaced. When a
record repeats a key in any object, at any depth, the last occurrence wins and the output has no
duplicate key anywhere: both extract stages rewrite such a record before it is stored (rather
than quarantine it; "The data" has the rule), so person bodies and the organization bodies
spliced in as raw JSON (never re-parsed on the person side) arrive without repeats. A document larger than
`max_document_bytes` (clamped to `bulk_max_bytes` minus 4 KiB, so every kept document fits in
one bulk request) is quarantined as `document_too_large` instead of indexed.

Duplicate ids resolve to the **last record in feed order** (input files sorted by key, then
line number), for organizations and for persons alike, whatever the partition or worker count.
The document `_id` is the person id, so a resend is idempotent.

The explicit mapping lives in `crates/pi-es/src/mapping.json` (`dynamic: false`; `roles` and
`organizations` are `object`, not `nested`), with the reasons in `crates/pi-es/src/mapping.md`.
Term queries on `roles.title.keyword` and `organizations.name.keyword` are part of the
acceptance check.

## Stages

A run is a DAG of stages. A stage is a list of tasks; a task reads committed inputs, writes its
outputs, and returns a report that the executor commits. Stages run in this order, each starting
once the previous one has committed every task:

| stage | one task per | reads | writes |
|---|---|---|---|
| `plan` | run | the input listing | `plan.json`: input files, partition count `P`, index name |
| `extract-orgs` | organization file | `raw/organizations/<file>` | `orgs/p=<p>/` segments of `OrgRow`, quarantine |
| `extract-persons` | person file | `raw/persons/<file>` | `persons/q=<q>/` `PersonRow`, `edges/p=<p>/` `EdgeRow`, quarantine |
| `join` | partition `p` | `orgs/p`, `edges/p` | `joined/q=<q>/` segments of `JoinedRow` |
| `assemble` | partition `q` | `persons/q`, `joined/q` | `docs/q=<q>/` segments of `DocRow`, quarantine |
| `prepare-index` | run | cluster state | the run's index, load-time settings |
| `load` | partition `q` | `docs/q` | bulk requests into the run's index |
| `publish` | run | the run's index | count checked, settings restored, `publish.json`, alias moved, `summary.json` |

- `p = xxh3_64(org_id as u64 little-endian) % P` and `q = xxh3_64(person_id LE) % P`
  (`xxhash-rust`, seed 0; `P` is a power of two, so the modulo is a mask). One `P` for both keys
  keeps the plan simple. Known value for tests: `xxh3_64(0u64 LE) = 0xC77B3ABB6F87ACD9`.
- `plan` picks `P` from the total input bytes (compressed, as listed) and
  `partition_target_bytes` (default 256 MiB), at least `min_partitions` (default 16), rounded up
  to a power of two. More data means more partitions, not bigger workers; what one task holds
  is bounded by the settings below, not by how large its partition turned out.
- `extract-*` stream one gzip file (every member's CRC and length checked) and write to up to
  `2 × P` partitions through one shared byte budget (`buffer_bytes`): when it is full, the
  largest partition is written out, so memory does not grow with `P`.
- `join` holds one org partition in memory (deduplicated last-wins) and streams the edges;
  `assemble` holds one partition's joined rows (grouped by person, keeping only each person's
  newest record's rows) and reads the person segments twice: once to find each winner, once to
  build its document. Both are the two halves of a partitioned hash join: no node ever needs the
  whole organization table.
- Both bound what they hold by `task_memory_bytes` (default 512 MiB) with a grace-hash-join
  multi-pass. Before loading, a task estimates the side it holds from the per-partition sizes
  the earlier stages committed (`report.partitions`, below): `join` from the `orgs/p` rows of
  `extract-orgs`, `assemble` from the `joined/q` rows of `join` and the `persons/q` rows of
  `extract-persons`. The estimate is the rows' encoded size before compression plus a fixed
  overhead per row for the hash maps and vectors (128 bytes per organization, 96 per joined
  row, 192 per person record; `crates/pi-stages/src/passes.rs` explains them). It then runs
  `k = ceil(estimate / task_memory_bytes)` passes (at least 1, at most 1024, with a warning at
  the cap): pass `i` holds only the ids with `xxh3_64(id LE, seed 1) % k == i` (organization
  ids in `join`, person ids in `assemble`) and streams the other side filtered the same way,
  re-reading the partition's segments once per pass. Every id falls in exactly one pass, so
  the joined rows and the documents are the same for any `k`; only their order inside the
  output segments differs, and nothing downstream depends on that order. The counter `passes`
  records `k` per task. The estimate is a model, not a measurement: the overheads assume maps
  and vectors up to half empty after growing.
- `load` sends byte-bounded bulk requests (`bulk_max_bytes`, default 8 MiB) with
  `senders_per_worker` in flight per task (default 2), retries 429/502/503/504 and transport
  errors with jittered exponential backoff, resends only the failed items, and checks every
  item's status. It then checks rows read = sent = acknowledged = the documents `assemble`
  committed for that partition. Total pressure on the cluster is
  `concurrent load tasks × senders_per_worker`, the knob to size against the data nodes.
  Decoded documents wait for the bulk indexer within a byte budget (`load_buffer_bytes`,
  default `2 × bulk_max_bytes`): each takes its body length from the budget and gives it back
  once it is in a bulk request buffer, and one larger than the whole budget passes alone. A
  load task's memory is about `(senders_per_worker + 1) × bulk_max_bytes + load_buffer_bytes`.
- `prepare-index` creates `<alias>-<run_id>` (lowercased) with `number_of_replicas: 0`,
  `refresh_interval: -1`, `translog.durability: async`, the codec, and `shards` primaries
  (default: 2 per data node, read from `_nodes`); an existing index counts as created, so a
  retry succeeds.
- `publish` refreshes and flushes while replicas are still 0 (so the complete-shard check covers
  every copy), checks that the index count equals the sum of `assemble`'s `documents` counters,
  then restores replicas (default 1), `refresh_interval: 1s` and request durability, waits for
  the cluster health (`green` by default), records the indexes the alias points at in
  `publish.json` (create-if-absent; a later attempt reads the first attempt's list back), moves
  the alias in one `_aliases` request, and deletes the recorded indexes and any others the alias
  pointed at (never the run's own; a 404 counts as deleted). The removes carry
  `"must_exist": false`, so a request resent after a lost answer is harmless, and a failed
  request after which the alias points at the run's index alone counts as done. A publish
  interrupted after the alias moved therefore still deletes the old indexes when it resumes.
  Any failure before the alias request leaves the alias untouched; a count mismatch is a
  permanent failure of the run. A failed delete of an old index is reported in `summary.json`,
  not a failure, since the alias has moved by then.
  Publish then writes `summary.json` and deletes orphaned files (data and quarantine files no
  commit lists).

## Storage layout

All paths are under one prefix in the object store (`PI_STORE_URL`: `s3://bucket/prefix`,
`file:///path`, a bare local path, or `memory://` for tests):

```
raw/organizations/*.ndjson.gz          inputs (or PI_INPUT_URL, a separate prefix)
raw/persons/*.ndjson.gz
raw/expected.json                      written by datagen, read by verify
runs/<run_id>/plan.json
runs/<run_id>/state.json               RunState: running | published | failed, current stage
runs/<run_id>/failed.json              the failure marker (create-if-absent)
runs/<run_id>/stages/<stage>.json      the stage's task list and run_by, written by the coordinator
runs/<run_id>/<stage>/_commit/<task>.json
runs/<run_id>/<stage>/_lease/<task>.json
runs/<run_id>/<stage>/data/<dataset>/<p|q>=<n>/<task>-<attempt>-<n>.seg
runs/<run_id>/quarantine/<task>-<attempt>.ndjson
runs/<run_id>/publish.json             the indexes the alias served before (create-if-absent)
runs/<run_id>/summary.json
snapshots/                             reserved for Elasticsearch snapshots (k8s component)
```

- Datasets: `orgs` and `edges` use the partition key `p`; `persons`, `joined` and `docs` use
  `q`. The partition number is not zero-padded.
- Task ids: `run` (single-task stages), `file-NNNNN` (extract), `part-NNNNN` (partitioned).
  Run, stage, task and attempt ids are `[A-Za-z0-9._=-]`, at most 128 bytes. Generated run ids
  are `20261015T120000Z-a1b2c3` (UTC start time first, so the greatest id is the newest run).
- `plan.json`: `{run_id, partitions, alias, index, organizations: [{key, size, version}],
  persons: [{key, size, version}], total_bytes, created_at}`, written create-if-absent; a retry
  reads it back. `version` is `{e_tag, version}` as the input listing reported it (the etag
  always; a version id only where the store reports one). Extract reads each file with
  `If-Match` on that etag (and the version id, if any), so a run reads exactly the bytes it
  planned. The local filesystem's etag is its inode, modification time and size; it checks it
  only on open and then reads the open file, so extract heads the file again once it has read
  it and fails the same way if the etag moved (an in-place overwrite during the read).
- `stages/<stage>.json`: `{stage, run_by: coordinator|workers, tasks, written_at}`. A stage
  with one task or fewer is run by the coordinator, including an extract stage with one file.
- `state.json`: `{run_id, status, stage, completed, error, started_at, updated_at}`.
- `failed.json`: `{stage, task_id, worker, error, failed_at}`. Whoever fails the run first
  creates it; workers and the coordinator stop when they see it; resuming a failed run clears it.
- Quarantine: one NDJSON line per entry, `{feed, file, line, kind, reason, sample}` with a
  sample of at most 1 KiB; kinds `syntax | envelope | record | too_long | document_too_large`.
- `publish.json`: `{alias, index, previous}`, the indexes the alias pointed at (the run's own
  excluded) when the first publish attempt was about to move it; every attempt deletes
  these after the swap.
- `summary.json`: `{run_id, alias, index, partitions, started_at, finished_at, wall_ms,
  documents, persons_per_second, input_bytes, stages: [{stage, tasks, wall_ms, task_ms,
  bytes_in, bytes_out, counters}], publish: {docs, previous, deleted, not_deleted},
  orphans_deleted, orphans_left}`.

### Segment files

A segment is one zstd stream (level 3 by default, frames checksummed) of records, each a LEB128
varint length followed by the `postcard` encoding of the row. Files start with the 8-byte magic
`PISEG001` outside the zstd stream. Writers roll to a new file once an upper-bound estimate of
the compressed size reaches `segment_target_bytes` (default 64 MiB), so a segment ends at most
about one row past the target. Rows (`crates/pi-model`):

- `OrgRow { org_id: u64, seq: Seq, body: Vec<u8> }`: `body` is the raw JSON of the record, its
  repeated keys, at any depth, reduced to the last occurrence.
- `PersonRow { person_id: u64, seq: Seq, body: Vec<u8>, org_ids: Vec<u64> }`: `body` as for
  `OrgRow`; `org_ids` the distinct
  non-null organization ids in role order.
- `EdgeRow { org_id: u64, person_id: u64, seq: Seq }`
- `JoinedRow { person_id: u64, org_id: u64, seq: Seq, body: Option<Vec<u8>> }`: `None` means
  unresolved.
- `DocRow { person_id: u64, body: Vec<u8> }`: the final document JSON.
- `Seq { file: u32, line: u64 }`: position in feed order, for last-wins deduplication; an equal
  `Seq` is the same record offered twice.

### Commits

A task writes its outputs under a fresh attempt id (a UUID v7) and returns a report; the
executor then commits by creating `_commit/<task>.json` with a create-if-absent put
(`PutMode::Create`): `{stage, task_id, attempt, worker, committed_at, report: {outputs,
counters, bytes_in, bytes_out, duration_ms, partitions}}`, where `partitions` (omitted when
empty) maps each output partition, `orgs/p=3`, to `{rows, raw_bytes}`: the rows written there
and their encoded size before compression, which `join` and `assemble` sum to size their passes. **Readers enumerate commits, never data
prefixes**, so a crashed or duplicate attempt's files are invisible; the first commit wins and
the loser's files are orphans that `publish` removes. A task that finds its commit already
present is skipped. This makes every task safe to retry and safe to run twice at the same time.

### Leases (distributed executor)

A worker claims a task by creating `_lease/<task>.json` with `PutMode::Create`:
`{worker, holder, expires_at, acquired_at, renewals, released}`. It renews every
`lease_ttl / 3` with `PutMode::Update` on the etag it holds, and a worker that finds a lease
past `expires_at + clock_skew` (default 5 s), or released, takes it over with `PutMode::Update`
on the etag it observed. Release is an update-if-match that sets `released: true`. A refused
renewal re-reads the lease: if it still carries the holder's own `holder` token (an earlier
renewal landed but its response was lost), the holder goes on with the etag it read. A holder
whose renewals fail for a whole ttl, or whose lease was taken or deleted, has its task
cancelled. Leases avoid duplicate work; commits are what keep a duplicate harmless.

A polling worker lists `_lease/` once per poll (next to `_commit/`) and tries to claim only the
tasks whose lease is absent, released or past `expires_at + clock_skew`. It reads a lease body
only when its listed etag changed since the last poll and judges an unchanged lease's expiry from
the `expires_at` it already read, so a live lease costs no request on most polls (a renewal, every
`lease_ttl / 3`, costs one read).

Conditional updates need an S3-compatible store that answers 412 to a failed `If-None-Match: *`
or a stale `If-Match`; the deployments use SeaweedFS 4.48, which does (see
[ADR 0003](adr/0003-object-store-commits-and-leases.md)). The local filesystem store supports
create-if-absent but not update-if-match, so it serves only the local executor; the distributed
executor refuses it.

## Components

| crate | what it owns |
|---|---|
| `pi-model` | the row types, envelope parsing and validation, ids, `Partitioner`, `LastWins`, `JoinedGroups`, `DocumentBuilder` (the join's output rules). Pure: no I/O. |
| `pi-segment` | `SegmentWriter` / `SegmentReader` for the segment format, rolling, magic, varint framing, `MemorySink` / `FileSink`. |
| `pi-store` | `Store`: a thin wrapper over `object_store` (S3-compatible / local / memory) with put, get, stream, list, create-if-absent and update-if-match; `RunLayout`: every path above, built in one place. |
| `pi-coord` | `TaskSpec`, `StageSpec`, `CommitLog`, `LeaseManager`, the `Stage` trait, the `Executor` trait with `LocalExecutor` (a bounded pool in one process) and `DistributedExecutor` (coordinator) plus `Worker` (lease-claiming workers), `RunDriver`, `RunState`, retries. |
| `pi-es` | `EsClient` (reqwest, rustls, basic or API-key auth, custom CA), `IndexAdmin`, `BulkIndexer`, `Publisher`, `Verifier` (expected counts, content digest), the mapping. |
| `pi-stages` | one type per stage implementing `Stage`, and `build_registry`. |
| `pi-datagen` | a seeded generator of both feeds with realistic skew (Zipf organization popularity, a third null organization ids, configurable unresolved, malformed and duplicate rates, duplicate ids across files) that also writes `expected.json`: exact counts for the acceptance check. |
| `people-index` | the binary: `run`, `worker`, `datagen`, `verify`, `stage` (one task, for debugging). |

The seams are traits so each piece is tested alone: a stage is tested against a local or
in-memory `Store` without Elasticsearch, the bulk indexer against a mock HTTP server, the
executors with fake stages.

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

`RunContext` carries the store, the layout and a cancellation token. An error marked
`permanent` (bad gzip, malformed limit, count mismatch, an input changed or removed since the
plan) is not retried; any other error is retried up to `task_attempts`. A changed input fails
the run with the file and both versions, planned and current; reading the new bytes would mix
inputs (`P` was sized from the old ones, and `Seq` positions would point at different
records), and no retry can bring the planned bytes back.

CPU-bound work (gzip, JSON, zstd, the join) runs on `spawn_blocking` or rayon, never on the
async reactor threads.

### Malformed limit

A single file with more than `max_malformed` malformed lines fails its extract task at once. The
feed total is checked from the commits when the next stage lists its tasks: `extract-persons`
checks the organization feed, `join` checks the person feed.

## Configuration

Every option is a CLI flag with a `PI_*` environment variable (clap `env`). Byte sizes accept
suffixes (`256MiB`), durations humantime (`60s`, `30m`).

| variable | meaning | default |
|---|---|---|
| `PI_STORE_URL` | object store prefix | `file://./store` |
| `PI_INPUT_URL` | input prefix, if not `<store>/raw` | unset |
| `PI_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3-compatible store; an endpoint switches to path-style requests and an `http://` one allows plain HTTP | unset; region `us-east-1` |
| `PI_ES_URL` | Elasticsearch | `http://localhost:9200` |
| `PI_ES_USERNAME`, `PI_ES_PASSWORD` or `PI_ES_API_KEY` | auth | unset |
| `PI_ES_CA_CERT` | PEM file to trust | unset |
| `PI_ES_REQUEST_TIMEOUT`, `PI_ES_GZIP` | one request's timeout, gzip bulk bodies | `120s`, false |
| `PI_ALIAS` | the published alias | `persons` |
| `PI_RUN_ID` | run id (`run`, `worker`, `stage`) | generated; `worker` follows the newest run |
| `PI_EXECUTOR` | `local` or `distributed` | `local` |
| `PI_CONCURRENCY` | tasks at once in this process | number of CPUs |
| `PI_WORKER_ID` | worker name in leases and commits | `HOSTNAME` |
| `PI_LEASE_TTL` | lease lifetime | `60s` |
| `PI_TASK_ATTEMPTS` | attempts per task, the first included | 3 |
| `PI_STAGE_TIMEOUT` | coordinator gives up on a distributed stage | never |
| `PI_WORKER_IDLE_TIMEOUT`, `PI_POLL_INTERVAL` | worker exits after idling; how often it looks for work | never, `2s` |
| `PI_PARTITION_TARGET_BYTES`, `PI_MIN_PARTITIONS` | partitioning | 256 MiB, 16 |
| `PI_MAX_MALFORMED`, `PI_MAX_LINE_BYTES` | per feed; longest input line | 1000, 8 MiB |
| `PI_SEGMENT_TARGET_BYTES`, `PI_BUFFER_BYTES`, `PI_UPLOAD_CONCURRENCY` | segment size, partitioned-writer budget per task, uploads in flight per task | 64 MiB, 256 MiB, 2 |
| `PI_TASK_MEMORY_BYTES` | what a `join` or `assemble` task holds per pass; a partition estimated larger runs in passes | 512 MiB |
| `PI_MAX_DOCUMENT_BYTES` | larger documents are quarantined | 8 MiB (clamped) |
| `PI_BULK_MAX_BYTES`, `PI_SENDERS_PER_WORKER` | load pressure | 8 MiB, 2 |
| `PI_LOAD_BUFFER_BYTES` | decoded documents a load task holds waiting for the bulk indexer | 2 × `PI_BULK_MAX_BYTES` |
| `PI_INDEX_SHARDS`, `PI_INDEX_REPLICAS`, `PI_INDEX_CODEC` | the index | 2 per data node, 1, `best_compression` |
| `PI_PUBLISH_HEALTH`, `PI_HEALTH_TIMEOUT` | health publish waits for | `green`, `30m` |
| `PI_METRICS_ADDR` | Prometheus endpoint, `off` to disable | `0.0.0.0:9464` |
| `PI_LOG_FORMAT` | `json` or `pretty` (filter with `RUST_LOG`) | `json` |
| `PI_DATAGEN_PERSONS`, `_ORGANIZATIONS`, `_FILES`, `_SEED`, `_CONFIG`, `_OUT` | `datagen` | 10000, 2000, 4, 1, unset, `<inputs>` |
| `PI_EXPECTED` | `verify`: local path or store URL | `expected.json` beside the inputs |

## Commands

- `people-index run`: the coordinator. With `PI_EXECUTOR=local` it runs every stage itself;
  with `distributed` it writes each stage's task list, runs the single-task stages, waits for
  the workers' commits, and moves on. An existing `PI_RUN_ID` is resumed. Exits 0 once
  published, 1 on failure.
- `people-index worker`: claims and runs tasks of the current stage of `--run-id` (or of the
  newest run) until the run is published or failed, or the idle timeout passes, then exits 0.
- `people-index datagen --persons N --organizations M --files F --seed S --out <url>`: writes
  `organizations/`, `persons/` and `expected.json` under `--out`.
- `people-index verify --expected <file|url>`: runs the acceptance queries against the alias
  (count, `persons_with_unresolved`, title and organization-name term counts) and exits 1 when
  a check fails; `--digest` prints an order-independent (additive) SHA-256 digest of every
  `_id` and `_source`, read through a point in time. The digest is an integrity check for
  comparing two indexes, not a defence against crafted collisions.
- `people-index stage <name> --run-id R [--task T]`: one task (or every task) of one stage,
  through the local executor, for debugging. It rewrites `stages/<stage>.json` with the tasks it
  ran, so never point it at a run that workers are following.

`run` and `worker` exit 143 on SIGTERM or Ctrl-C (a second signal exits at once).

## Metrics

Served on `PI_METRICS_ADDR` at `/metrics`:

- tasks: `pi_tasks_committed_total`, `pi_tasks_failed_total`, `pi_task_retries_total`,
  `pi_task_duration_seconds{stage,outcome}`, `pi_task_bytes_in_total{stage}`,
  `pi_task_bytes_out_total{stage}`, `pi_task_counter_total{stage,counter}`;
- stages: `pi_stage_duration_seconds{stage}`, `pi_stage_tasks{stage}`;
- leases: `pi_lease_takeovers_total`, `pi_lease_lost_total`;
- data: `pi_malformed_lines_total{feed}`, `pi_documents_indexed_total`;
- bulk: `pi_es_bulk_requests_total`, `pi_es_bulk_retries_total`,
  `pi_es_bulk_items_retried_total`, `pi_es_bulk_docs_indexed_total`,
  `pi_es_bulk_wire_bytes_total`.

## Deployment

- **Kubernetes** (`deploy/k8s`, Kustomize): ECK 3.5.0 runs Elasticsearch 9.5.5 with three
  dedicated master nodes and a data node set spread across zones (`topology.kubernetes.io/zone`,
  shard allocation awareness); SeaweedFS 4.48 is the S3-compatible store locally (the
  production example uses external S3); the coordinator is a Job and the workers a Job with
  `parallelism: N`, spread across zones and nodes. `deploy/k3d` creates a local cluster of one
  server and three agents labelled with three zones, so the multi-node layout is exercised on
  one machine. Components add monitoring (Elasticsearch exporter, ServiceMonitors, a PodMonitor
  for the pipeline, alert rules; no dashboards yet) and snapshots to the object store
  (`snapshots/`, SLM policy `nightly`).
- **Compose** (`deploy/compose`): the same topology on one Docker host: three Elasticsearch
  nodes (master and data, one zone each), SeaweedFS, the coordinator and `--scale worker=N`.

Failure model: a worker that dies loses its lease and another worker takes the task over; a
coordinator that dies is restarted by its Job and resumes from the commits already in the store
(`run` with an existing `PI_RUN_ID` skips committed stages and tasks). Elasticsearch loss of a
data node during the load is absorbed by retries; the count check in `publish` refuses to move
the alias if documents are missing. Only one coordinator per run is assumed (the Job guarantees
it); there is no coordinator lease.
