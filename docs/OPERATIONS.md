# Operations

How to run people-index on one machine (Docker Compose or a local k3d cluster), scale it, resume
a failed run, read what it reports, size it, and what happens when something breaks.
[ARCHITECTURE.md](ARCHITECTURE.md) is the contract this page follows: stages, storage layout,
commits and leases, configuration. Every command below is a `make` target; `make help` lists
them all.

## The pieces

| piece | compose | Kubernetes (k3d) |
|---|---|---|
| image | `people-index:local`, built from `Dockerfile` (`make image`) | the same image, loaded with `make image-import` |
| Elasticsearch 9.5.5 | `es01`, `es02`, `es03`: three master+data nodes, zones a/b/c | ECK resource `es`: 3 dedicated masters + a data node set across zones; HTTP service `es-es-http:9200` |
| TLS | CA and node certificates from `elasticsearch-certutil` (service `certs`) | ECK's self-signed CA, secret `es-es-http-certs-public` key `ca.crt` |
| pipeline user | `pipeline`, role `people-index-writer`, created by `es-users` | the same user in ECK's file realm; credentials in secret `pipeline-es-credentials` (`username`, `password`) |
| reader user | `reader`, role `people-index-reader`, created by `es-users` | role `people-index-reader` is available for an operator to assign to a user |
| object store | SeaweedFS on `s3:8333`, buckets `people-index` and `people-index-snapshots`, created and probed by `s3-init` | SeaweedFS service `s3:8333`, buckets created by Job `s3-bucket-init`; credentials in secrets `s3-credentials` and `snapshot-s3-credentials` |
| a run | services `coordinator` (`people-index run`) and `worker` (`people-index worker`, `--scale worker=N`) | Job `pi-run-<run_id>` and Job `pi-worker-<run_id>` (`parallelism: N`, spread over zones) |
| synthetic data | service `datagen` (profile `datagen`) | Job `pi-datagen-<id>` |
| metrics | Prometheus (profile `metrics`) | `deploy/k8s/components/monitoring` (Prometheus Operator) |

Role `people-index-writer` grants cluster `monitor` and every privilege on `persons` (the alias)
and `persons-*` (its `persons-<run_id>` indexes) and nothing else; `roles.yml` lists the calls
this covers. If you change `PI_ALIAS`, the role's index names follow it in compose (`es-users`
reads `PI_ALIAS`); on Kubernetes change `deploy/k8s/base/elasticsearch/roles.yml` too.
Both deployments block auto-creation of run indexes while allowing other indexes. Compose
sets `action.auto_create_index` from `PI_ALIAS` (default `-persons-*,+*`); on Kubernetes,
for another `PI_ALIAS`, edit the excluded pattern to match its lowercased `<alias>-*` in
every Elasticsearch node set.

The store must refuse a conditional write that does not hold (HTTP 412 for `If-None-Match: *` on
an existing key and for a stale `If-Match`): commits and leases depend on it. SeaweedFS 4.48
passes; `s3-init` checks it on every start and stops the stack if a store does not.

## Compose

Prerequisites: Docker with Compose v2, GNU make and bash. On Linux, Elasticsearch needs
`sudo sysctl -w vm.max_map_count=262144` (Docker Desktop already sets it).

```bash
make image                 # build people-index:local
make compose-run           # start everything, generate data, run, verify
make compose-down          # stop; keeps Elasticsearch data and the store
make compose-clean         # stop and delete every volume
```

`make compose-run` is three steps you can also run alone:

1. `make compose-datagen`: starts the infrastructure (`make compose-up`), then writes both feeds
   and `expected.json` to `s3://people-index/raw/`. Size it with `DATAGEN_PERSONS`,
   `DATAGEN_ORGANIZATIONS`, `DATAGEN_FILES`, `DATAGEN_SEED`.
2. `make compose-pipeline`: starts the coordinator and `WORKERS` workers (default 3) for a new
   `RUN_ID` (UTC time, e.g. `20261015-140501`), waits for the coordinator and exits with its
   status: 0 means the run published.
3. `make compose-verify`: the acceptance queries and the content digest against the alias.
   `VERIFY_SOURCE=summary make compose-verify` checks it against a run's `summary.json`
   instead (see "Verifying a published index" below; `VERIFY_RUN_ID` picks the run).

`make compose-e2e` (CI's end-to-end) starts with `make compose-datagen` and a run that must
fail in `publish` (id a minute older than `RUN_ID`; `PI_INDEX_REPLICAS=3` on 3 data nodes never
reaches green within `PI_HEALTH_TIMEOUT=10s`, `PI_TASK_ATTEMPTS=1`), which leaves its loaded
index behind. Then `RUN_ID` runs and is verified, the restore drill snapshots, loses and
restores its index and the restored index is verified again
([Rollback and restore](#rollback-and-restore)), a second run (`RUN_ID` with `-2` appended)
over the same feeds is verified again in both modes (the summary one finds the second run
from the alias), and `deploy/compose/scripts/check-republish.sh` checks:
the failed run's index existed off the alias before the good runs, the alias points only at
the second run's index, the first run's and the failed run's indexes are gone, no other
`persons-*` index is left, the second run's `summary.json` lists the first index under
`previous` and `deleted`, the first run's lists the failed run's index under `deleted` (not
`previous`), and both have `not_deleted` empty. Those publishes are the only place the pipeline
user lists the `persons-*` indexes, moves the alias off an index and deletes indexes against a
real cluster, the calls role `people-index-writer` must allow.

`make compose-up` alone starts Elasticsearch, the store and the one-shot setup services, and
waits until `certs`, `es-users` and `s3-init` have succeeded:

- `certs` creates the CA and one certificate per node once (they persist in the `certs` volume)
  and copies only the public CA into the `es-ca` volume, which the pipeline mounts read-only at
  `/etc/people-index/es-ca/ca.crt` (`PI_ES_CA_CERT`). Private keys never leave the `certs`
  volume.
- `es-users` waits for a formed three-node cluster, then creates the writer and reader roles and users.
- `s3-init` creates the pipeline and snapshot buckets and runs the conditional-write probe.
  Compose provisions the snapshot identity and gives Elasticsearch the S3 client settings for
  it, but puts no keys in the keystore and registers no repository; `make compose-restore-drill`
  does both for its run (see [Rollback and restore](#rollback-and-restore)).

Configuration: copy `deploy/compose/.env.example` to `deploy/compose/.env` (git-ignored) and
change what you need: passwords, published ports, heap and memory limits, worker shape, the
`PI_*` knobs. Every value has a development default, and every published port binds to
127.0.0.1 only. The pipeline containers run as uid 65532 with a read-only root filesystem, no
capabilities and `no-new-privileges`.

From the host:

```bash
make compose-ca                                    # writes target/es-ca.crt
curl --cacert target/es-ca.crt -u pipeline:<password> https://localhost:9200/_cat/indices/persons*?v
aws --endpoint-url http://localhost:8333 s3 ls s3://people-index/runs/   # with the S3 keys from .env
```

`make compose-runs` lists the runs in the store and `make compose-runs RUN_ID=<id>` prints that
run's `summary.json`, without installing anything on the host.

## Kubernetes on k3d

Prerequisites: Docker, [k3d](https://k3d.io) 5, kubectl, GNU make, bash. Every target passes
`--context k3d-people-index` explicitly and never uses the current context.

```bash
make k3d-up          # cluster "people-index": 1 server + 3 agents, zones zone-a/b/c
make eck             # ECK 3.5.0 CRDs and operator: downloaded, checked against SHA256SUMS, applied
make image           # build people-index:local
make image-import    # load it into every k3d node (imagePullPolicy: Never)
make deploy-local    # credentials if missing, then Elasticsearch, store, RBAC, config; waits for green
make datagen         # Job pi-datagen-<id>: feeds into s3://people-index/raw/
make run             # Jobs pi-run-<id> + pi-worker-<id>; waits until the coordinator completes
make verify          # Job pi-verify-<id>: acceptance queries and digest against the alias
make k3d-down        # delete the cluster
```

- `make secrets` (run by `deploy-local`) creates `deploy/k8s/overlays/local/secrets/*.env` with
  random values when they are missing (`deploy/k8s/gen-secrets.sh`). Those files are the only
  place the credentials live; the overlay turns them into the secrets `pipeline-es-credentials`
  `s3-credentials` and `snapshot-s3-credentials`.
- Run Jobs are rendered by `deploy/k8s/jobs/render.sh` from the templates in `deploy/k8s/jobs/`
  and the defaults in `deploy/k8s/overlays/local/jobs.vars`; an environment variable of the same
  name wins (`WORKERS=6 make run`). The rendered files stay in `target/jobs/` for inspection.
- `make logs` lists the Jobs; `make logs RUN_ID=<id>` follows the coordinator;
  `make worker-logs RUN_ID=<id>` shows every worker.
- Persistent things (Elasticsearch, the store, RBAC, config) are applied once with kustomize;
  each run is a pair of Jobs, kept for a day after they finish (`ttlSecondsAfterFinished`).
- `make eck` downloads `crds.yaml` and `operator.yaml` into `target/eck/` with
  `deploy/k8s/eck/fetch.sh`, checks them with `sha256sum -c` against
  `deploy/k8s/eck/SHA256SUMS`, pins the operator image to the digest in
  `deploy/k8s/eck/operator-image` (`pin-operator.sh`, into `target/eck/operator-pinned.yaml`)
  and applies those local files (server-side). On a mismatch it keeps nothing and stops before
  applying; `scripts/check-deploy.sh` validates the same files, pinned the same way.
- `make verify` renders `deploy/k8s/jobs/verify.yaml` (pod security, credentials and CA as the
  run Jobs), runs `people-index verify --digest` against the alias, waits for the Job and prints
  its output; a failed check fails the target. `VERIFY_SOURCE` in `jobs.vars` picks the mode:
  `expected` (the local overlay: `s3://people-index/raw/expected.json` from `make datagen`) or
  `summary` (the production example: the summary of the run the alias serves, no datagen).
  `VERIFY_SOURCE=summary make verify` overrides it. Size it with `VERIFY_CPU`, `VERIFY_MEMORY`
  and `VERIFY_DEADLINE_SECONDS` in `jobs.vars`.

## Verifying a published index

`people-index verify` has two modes (`--source`, `PI_VERIFY_SOURCE`):

- `expected` (the default): datagen's `expected.json` (`--expected`, `PI_EXPECTED`) holds exact
  counts for the synthetic feeds: the document count, `persons_with_unresolved`, and term
  counts per role title and organization name. Only `datagen` writes it.
- `summary`: for real feeds, where no expected file exists. The run's own
  `runs/<id>/summary.json` (written by `publish`) is the reference. The run is `--run-id`
  (`PI_VERIFY_RUN_ID`), by default the run in the store whose index the alias points at (index
  names are lowercased run ids, so it is found by listing `runs/`); a summary that is missing
  (the run never published) or names another alias is an error. Every query reads the run's
  index itself, not the alias, so a publish in the middle cannot answer for another run. The
  checks:

  | Check | Passes when | What it shows |
  |---|---|---|
  | `alias` | the alias points at the run's `index` and nothing else, before and after the queries | the swap completed, nothing else is served, nothing moved meanwhile |
  | `count` | documents in the index = `publish.docs` | nothing was lost or added since publish counted |
  | `sample:ids` | one document, searched by its `_id`, is found exactly once | ids are unique and retrievable |
  | `sample:full_name` | a match on the sample's first and last name (all terms) finds at least one document | full-text search on `full_name` works |
  | `sample:organization_id` | a term query on the sample's first joined organization id finds at least one document | the joined organizations are searchable |

  The sample is a document with a last name and a joined organization when the index has one;
  the last two checks run only when the sample has text names, and an organization id up to
  `i64::MAX` (the `long` mapping drops larger ones), so persons without names or without
  resolved organizations never fail them. A non-empty index that returns no sample fails
  `sample:ids`; an empty index gets only `alias` and `count`. A
  failure means the index or the alias is wrong, not the data. Checking an older run with
  `--run-id` fails `alias` once a newer run has replaced it.

Both print one JSON object (`passed`, `report`, `digest`, and `run_id` in summary mode) and exit
1 when a check fails; `--digest` adds the content digest in either mode (of the run's index in
summary mode).

## Querying the index

`people-index-reader` grants `read` and `view_index_metadata` on the published alias `persons`
only, with no cluster privileges. Compose creates the `reader` user with `READER_ES_PASSWORD`
from its environment (a development default is provided); Kubernetes defines the role for an
operator to assign to a user. Compose follows `PI_ALIAS` when creating the role. On Kubernetes,
change the reader's alias in `deploy/k8s/base/elasticsearch/roles.yml` when changing `PI_ALIAS`.
Use the alias in the API key descriptor and search URL below as well.

On Elasticsearch 9.5.5 this reader can open a point in time (PIT) on the alias, but
searching with its PIT id returns 403: PIT search requires `read` on the underlying
`persons-*` indexes, which this role does not grant. Page through the alias with
`search_after` without a PIT, or use `from` and `size`. Without a PIT, a publish or
other index changes between requests can change the results.

Create a key as an administrator with `manage_api_key` and access to the alias. API keys are
limited by both their descriptor and the creating user's privileges; the descriptor below gives
the key the reader role's privileges without granting access to run indexes by name. For example,
using a securely supplied administrator credential and a trusted CA:

```bash
curl --cacert "$ES_CA" -u "$ES_ADMIN_USER:$ES_ADMIN_PASSWORD" \
  -H 'Content-Type: application/json' -X POST "$ES_URL/_security/api_key" -d '{
  "name": "people-index-reader",
  "expiration": "90d",
  "role_descriptors": {
    "people-index-reader": {
      "cluster": [],
      "indices": [{"names": ["persons"], "privileges": ["read", "view_index_metadata"]}]
    }
  }
}'
```

Save the returned `id` for revocation and `encoded` value as a secret; the latter is the
credential supplied to search clients. Do not commit either credential. Search the alias:

```bash
curl --cacert "$ES_CA" -H "Authorization: ApiKey $ES_API_KEY" \
  -H 'Content-Type: application/json' -X POST "$ES_URL/persons/_search" \
  -d '{"query":{"match_all":{}},"size":10}'
```

Rotate keys by creating a new one and revoking the old key with an administrator credential that can manage it:

```bash
curl --cacert "$ES_CA" -u "$ES_ADMIN_USER:$ES_ADMIN_PASSWORD" \
  -H 'Content-Type: application/json' -X DELETE "$ES_URL/_security/api_key" \
  -d '{"ids":["<key-id>"]}'
```

For Compose, `make compose-ca` writes the public CA to `target/es-ca.crt`; set `ES_CA` to that
file and `ES_URL` to `https://localhost:9200`. For ECK's default certificate, obtain the public
CA with `kubectl -n people-index get secret es-es-http-certs-public -o jsonpath='{.data.ca\.crt}' | base64 -d > es-ca.crt`.
For a supplied certificate, use its trusted CA chain instead. Set `ES_CA` to the CA file and
`ES_URL` to the HTTPS endpoint whose hostname is covered by the certificate. The production
overlay describes certificate and network exposure options.

## Scaling

**Workers.** Workers are interchangeable: each one claims tasks of the current stage through
leases until the run is published. Adding workers adds throughput up to the number of tasks in a
stage (one per input file for the extract stages, `P` for `join`, `assemble` and `load`).

- compose: `make compose-run WORKERS=6`, or for a run in progress
  `make compose-scale RUN_ID=<id> WORKERS=6`. Each worker is limited by `WORKER_CPUS` and
  `WORKER_MEMORY`; keep `WORKER_CONCURRENCY` (tasks at once per worker) equal to its CPUs.
- k3d: `WORKERS=6 make run`, or for a run in progress
  `kubectl --context k3d-people-index -n people-index patch job pi-worker-<id> -p '{"spec":{"parallelism":6}}'`.
  Workers spread evenly across zones (hard) and nodes (soft) and prefer nodes without an
  Elasticsearch data node.
- Removing workers is safe at any time: a task left behind is taken over once its lease
  expires (`PI_LEASE_TTL`, 60 s by default).

**Elasticsearch data nodes.**

- k3d: raise `count` of the `data` node set of the `es` resource (`deploy/k8s`) and apply
  with `make deploy-local`; ECK adds the nodes and Elasticsearch rebalances shards across zones.
- compose: the three nodes are a fixed dev topology. To add `es04`, copy the `es03` service with
  its own name, zone and volume, add `es04` to `nodes` in `deploy/compose/scripts/certs.sh`
  (it issues the missing certificates from the existing CA on the next start), add it to the
  nodes' `discovery.seed_hosts` (and to `cluster.initial_master_nodes` only when creating a new
  cluster), and set `ES_NODES` of the `es-users` service.
- The shard count of a run is fixed when `prepare-index` creates its index: 2 primaries per data
  node by default (`PI_INDEX_SHARDS` overrides). New data nodes take effect on the next run;
  the current index keeps its primaries and only gains replica placement.

## Resuming a failed run

Every task commits with a create-if-absent write, and a coordinator started with an existing
`PI_RUN_ID` skips committed stages and tasks. Resuming therefore redoes only what had not
committed; a failed run never moves the alias, so the previous index keeps serving meanwhile.
Resume before a newer run publishes: a successful publish deletes the indexes of older runs
that never published, and an older run's `publish` refuses to take the alias from a newer run's
index (a permanent failure naming both), so a run resumed after that can never publish; start a
new run instead.

- compose: the coordinator has no restart policy, so its exit code is the verdict.
  `make compose-pipeline` prints the run id on failure; fix the cause and run
  `make compose-resume RUN_ID=<id>`. Workers restart on their own after a crash
  (`restart: on-failure`).
- k3d: the coordinator Job restarts a crashed coordinator by itself (`backoffLimit`), and the
  worker Job replaces failed workers; evictions and node drains do not count against either. If
  a Job does fail for good, `make resume RUN_ID=<id>` deletes both Jobs of that run and creates
  them again with the same id.
- To find a run id: `make compose-runs` (compose), `make logs` (k3d), or the `runs/` prefix of
  the store.

A run reads exactly the input files it planned: `plan.json` records each file's version, and
an extract task whose file was replaced or removed since then fails the run for good, naming the
file and both versions. To run on new inputs, start a new run. Rewriting a file with identical
content counts as a change too on stores whose etag is not a content hash (the local
filesystem's includes the modification time), so do not rerun `datagen` under a run that is
being resumed.

## Metrics, logs and the run summary

`fields_not_indexed` counts unknown top-level keys in kept records, summed in stage summaries.

- Every pipeline process serves Prometheus metrics on `PI_METRICS_ADDR` (`:9464`, path
  `/metrics`): tasks committed, failed and retried, task and stage durations, lease takeovers
  and losses, malformed lines, and bulk requests, retries, retried items, indexed documents and
  bytes on the wire. [ARCHITECTURE.md](ARCHITECTURE.md#metrics) lists the names;
  `curl <pod>:9464/metrics` shows the live set.
- compose: `make compose-metrics` starts Prometheus on http://localhost:9090, scraping the
  coordinator, every worker (Docker DNS finds each replica) and the store (`s3:9327`).
- k3d: the `monitoring` component adds the Elasticsearch exporter, a PodMonitor for the pipeline
  pods, ServiceMonitors and alert rules, for a cluster that runs the Prometheus Operator. Without
  it, find a pod with
  `kubectl --context k3d-people-index -n people-index get pods -l people-index.io/run-id=<id>`
  and `kubectl --context k3d-people-index -n people-index port-forward pod/<pod> 9464`.
- Logs are JSON lines on stdout (`PI_LOG_FORMAT=json`; `pretty` for reading by eye), filtered by
  `RUST_LOG`.
- `runs/<run_id>/summary.json` is written by `publish`: what the run read, joined, quarantined
  and indexed, and the count check that allowed the alias to move. Malformed lines are in
  `runs/<run_id>/quarantine/` with their origin.
- `values_ignored_by_index` (extract stages, in `summary.json` and
  `pi_task_counter_total{counter="values_ignored_by_index"}`) counts the values of kept records
  that Elasticsearch skips under `ignore_malformed` (`"n/a"` in `followers`, `"yes"` in
  `open_to_work`), or above a keyword's `ignore_above` (a 600-character `skills` element), following the rules Elasticsearch 9.5.5 was observed to apply (they and the
  one known divergence are in [ARCHITECTURE.md](ARCHITECTURE.md#the-data)): the malformed
  values are not indexed and the field is listed in the document's `_ignored`; the record stays
  searchable by its other fields. An `exists` query on `_ignored` finds those documents; it
  counts documents, while the counter counts values per extracted line (before person
  deduplication, an organization's value once however many persons embed it), so the two
  numbers differ.
- `records_with_repeated_keys` (extract stages, same places) counts the kept records that
  repeated a key in some object, at any depth; each was stored with the key's last occurrence
  only, since Elasticsearch fails a document with a duplicate key anywhere
  ([ARCHITECTURE.md](ARCHITECTURE.md#the-data)).

### Pipeline alerts

The monitoring component requires kube-state-metrics to expose Job labels, for example with
`--metric-labels-allowlist=jobs=[app.kubernetes.io/name,app.kubernetes.io/component]`, and
Prometheus to scrape it. Job metrics persist only until the finished Job is deleted; ensure the
Job TTL and alert routing leave enough time to respond. The exporter uses `--es.indices_settings`
for index creation timestamps. Exporter or kube-state-metrics scrape failures can make these
alerts blind; monitor both targets and collector errors independently.

These thresholds are proposed defaults: active for 3 hours, index age 26 hours, and the
`persons` alias. Set the stalled threshold below the Job deadline if an early warning is
needed: the local overlay deadline is 2 hours, while the production example permits a longer
run. A Job hitting `activeDeadlineSeconds` is reported by PeopleIndexJobFailed. Set the index
age threshold to the publication cadence plus the longest creation-to-publish time plus margin.
The 26-hour default assumes a daily cadence and runs under about 2 hours. In an overlay, use a Kustomize `patches` entry
that targets `PrometheusRule` named `people-index-elasticsearch` and replaces the relevant
rule's `expr` and `annotations.summary`; change the alias in both alias rules. For example, the overlay can add the following JSON patch (adjust the alias, seconds, and
summary together; the rule positions refer to the pipeline group in the component):

```yaml
patches:
  - target:
      group: monitoring.coreos.com
      version: v1
      kind: PrometheusRule
      name: people-index-elasticsearch
    patch: |-
      - op: replace
        path: /spec/groups/1/rules/2/expr
        value: 'time() - max(elasticsearch_indices_settings_creation_timestamp_seconds * on(index) group_left() elasticsearch_indices_aliases{alias="my-alias"}) > 93600'
      - op: replace
        path: /spec/groups/1/rules/2/annotations/summary
        value: The index behind my-alias is older than 26 hours.
      - op: replace
        path: /spec/groups/1/rules/3/expr
        value: 'absent(elasticsearch_indices_settings_creation_timestamp_seconds * on(index) group_left() elasticsearch_indices_aliases{alias="my-alias"})'
      - op: replace
        path: /spec/groups/1/rules/3/annotations/summary
        value: my-alias has no index, or alias or creation timestamp metrics are unavailable.
```

Index creation
age approximates publication age, not the actual alias move time. Multiple backing indices use
the newest creation timestamp. The missing alert also fires during initial deployment or when
exporter alias or creation timestamp metrics are unavailable; verify the exporter before concluding the alias is missing.

- **PeopleIndexJobFailed:** a coordinator or worker Job failed. Run `kubectl --context
  k3d-people-index -n people-index describe job/<job>` and inspect its pod logs and run summary.
- **PeopleIndexJobStalled:** a Job has remained active past the expected duration. Run `kubectl
  --context k3d-people-index -n people-index describe job/<job>` and inspect coordinator and
  worker logs, leases and task progress.
- **PeopleIndexAliasOld:** no recent index is behind the alias. Check the scheduler and recent
  Jobs with `kubectl --context k3d-people-index -n people-index get jobs`, then inspect run logs,
  the publish summary and Elasticsearch alias and index creation dates.
- **PeopleIndexJobLabelsMissing:** pipeline Jobs (named `pi-*`) exist in a namespace but kube-state-metrics exports none of them with the `app.kubernetes.io/name` label, so the two Job alerts above cannot match. Add the label allowlist to kube-state-metrics (see the requirement above). Jobs outside the pipeline are ignored.
  Check the kube-state-metrics Job label allowlist and scrape configuration.
- **PeopleIndexAliasMissing:** no matching alias and creation timestamp metrics are present. Check the exporter target and logs,
  then query Elasticsearch `GET /_alias/persons` and inspect the latest publish run.

What to watch during a load: a rising rate of bulk retries means the cluster is pushing back
(HTTP 429); task retries and lease takeovers mean workers are dying or stalling.

## Sizing

**Run store.** Budget for roughly `PI_RETAIN_RUNS` (default 3) published runs of segments,
plus current and unfinished runs and runs sharing a boundary timestamp. Set it to 0 to disable
cleanup. Retention is store-wide: aliases or deployments sharing a bucket prefix share one
window. A current run ID without a timestamp also skips cleanup. Failed and unstamped runs
are never cleaned; remove their `runs/<id>/<stage>/data/` by hand once abandoned. Run history
and committed quarantine are retained. Failed run inspections and failed deletes are logged and counted as
`retention_failures` in the publishing run summary; subsequent publishes retry.

**Partitions.** `plan` picks `P` from the total input bytes divided by
`PI_PARTITION_TARGET_BYTES` (256 MiB), at least `PI_MIN_PARTITIONS` (16), rounded up to a power
of two. The input bytes are the gzipped sizes, so a partition decoded is several times its
target, and organization popularity decides how large the joined rows of a partition get. That
no longer decides memory: it decides how many passes `join` and `assemble` need. More data gives
more partitions, not bigger workers; raise `PI_MIN_PARTITIONS` when there are more worker slots
than tasks, or when the `passes` counter of `join` or `assemble` in `summary.json` (summed over
the stage's tasks) is well above the stage's task count, since every extra pass re-reads the
partition.

**Memory per task.** Each part is bounded by a setting:

- `join` and `assemble` hold at most about `PI_TASK_MEMORY_BYTES` (512 MiB) of the partition
  side they keep in memory. Each estimates that side from the sizes the earlier stages committed
  (the rows' encoded bytes before compression, plus a fixed overhead per row for the hash maps:
  128 bytes per organization, 96 per joined row, 192 per person record) and runs
  `ceil(estimate / PI_TASK_MEMORY_BYTES)` passes, each holding one share of the ids. The
  overheads are modelled, not measured.
- extract tasks and `join` buffer up to `PI_BUFFER_BYTES` (256 MiB) of output rows before
  writing out a partition.
- Every task has up to 4 input segments (2 prefetched, 1 queued, 1 decoding) and
  `2 + PI_UPLOAD_CONCURRENCY` output segments (1 being written, 1 queued, the uploads) in
  memory, each up to about `PI_SEGMENT_TARGET_BYTES` (64 MiB): 8 segments at the default
  upload concurrency of 2.
- Plus about 32 MiB of read buffers, zstd and gzip state, and the runtime.

So one task needs about `PI_TASK_MEMORY_BYTES + PI_BUFFER_BYTES + 8 × PI_SEGMENT_TARGET_BYTES +
32 MiB` (the defaults: 512 + 256 + 512 + 32 MiB, about 1.3 GiB), a worker
`PI_CONCURRENCY` times that, and the coordinator one task (it runs every stage with one task or
fewer itself, including an extract stage with one input file). The deployments set the three
knobs to fit: the k3d base ConfigMap 192 MiB / 64 MiB / 16 MiB (about 416 MiB per task, for
1 GiB workers running two tasks and a 512 MiB coordinator), the production example 1 GiB /
256 MiB / 64 MiB (about 1.8 GiB, for 4 GiB workers running two), compose 384 MiB / 128 MiB /
32 MiB (about 800 MiB, for 2 GiB workers running two and a 1 GiB coordinator). Change
`WORKER_MEMORY`, `WORKER_CONCURRENCY` or `COORDINATOR_MEMORY` and these together. The rule is a
model built from the code, not a profile: watch the first runs' memory and lower
`PI_TASK_MEMORY_BYTES` or the segment size if workers come close to their limit.

**Shards.** The default, 2 primaries per data node, lets every data node index in parallel
without oversharding a small cluster. Aim for primaries of 10 to 50 GB at the final size; set
`PI_INDEX_SHARDS` when the data outgrows that. Replicas are 0 during the load and restored to
`PI_INDEX_REPLICAS` (1) by `publish`, so the replica copy is made once, by Elasticsearch, from
finished segments.

**Bulk pressure.** The load in flight against the cluster is
`workers × PI_SENDERS_PER_WORKER` requests of up to `PI_BULK_MAX_BYTES` (8 MiB) each. Start at
2 to 4 requests in flight per data node and raise it while the indexing rate grows and bulk
retries stay rare; back off when 429s become steady or the data nodes' write thread pools queue.
When workers are added for the CPU-heavy stages, lower `PI_SENDERS_PER_WORKER` so the total
stays where the cluster is comfortable.

**Load task memory.** A `load` task holds the bulk request it is filling and up to
`PI_SENDERS_PER_WORKER` requests in flight, plus decoded documents waiting for the indexer
within `PI_LOAD_BUFFER_BYTES` (default twice `PI_BULK_MAX_BYTES`): about
`(PI_SENDERS_PER_WORKER + 1) × PI_BULK_MAX_BYTES + PI_LOAD_BUFFER_BYTES`, 40 MiB with the
defaults, plus the document being decoded and a couple of segments being read. A document larger
than the load buffer still passes, alone, so a small buffer slows the load but never stops it.

**Elasticsearch.** Heap at most half of the node's memory (compose: `ES_HEAP` and
`ES_MEM_LIMIT`), the rest is page cache for segments. On a nearly full disk the watermarks keep
shards unassigned; free space, or for a throwaway dev stack set
`ES_DISK_THRESHOLD_ENABLED=false`.

## Failure model

| what fails | what happens | what to do |
|---|---|---|
| a worker dies | its lease expires after `PI_LEASE_TTL`; another worker takes the task over and redoes it; the dead attempt's files are orphans that no commit lists | nothing; compose and the worker Job replace the worker |
| two workers run the same task | both may write; only the first commit wins, readers list commits, the loser's files are removed by `publish` | nothing |
| the coordinator dies | k3d: the Job restarts it and it resumes from the commits; compose: the run stops | compose: `make compose-resume RUN_ID=<id>` |
| an existing run index has a different or missing mapping `_meta.people_index.run_id` | `prepare-index` fails permanently before loading, including when run ids differ only in case; an index left by a run interrupted inside `prepare-index` before this release has no owner and is refused after upgrade | delete that index if the run is abandoned, or start a new run |
| an Elasticsearch data node is lost during the load | bulk requests are retried with backoff and only failed items are resent; `publish` refuses to move the alias while shards are missing or the count differs | restore the node and resume; a count mismatch fails the run for good (its load tasks are already committed), so start a new run |
| the run index is missing during load | the task fails permanently before sending bulk requests; auto-creation is also blocked for run indexes | start a new run; resume cannot recreate the index or restore already committed load tasks |
| too many malformed lines (`PI_MAX_MALFORMED`) | one file over the limit fails its extract task at once; a feed over the limit in total fails the next stage (`extract-persons` for organizations, `join` for persons); quarantined lines are in `runs/<run_id>/quarantine/` | fix the input, start a new run |
| a Windows `file://` run fails a write with "Access is denied" | the local store renames a staging file over an object another handle holds for a moment (observed in the store's own conformance test, about 1 run in 100 to 300; a retry on a 5 s budget did not prevent it, and the cause is unknown) | rerun or `make compose-resume`; use the S3 store (compose, k3d) for anything that matters; the local store is for one-machine tries and tests, and Linux is unaffected (#43) |
| the store refuses conditional writes | `s3-init` (compose) stops the stack before any run; never run the distributed executor on such a store | use a conforming S3 server |
| publish refuses a feed bound | the alias stays on its current index and the new index is kept for inspection until a newer run publishes, which deletes it; resuming after that fails because the index is gone, so start a new run | inspect the feed and counts; correct the feed and start a new run, or relax the appropriate bounds and resume (see [Resuming a failed run](#resuming-a-failed-run)). `PI_PUBLISH_ALLOW_EMPTY_ORGANIZATIONS=true` alone does not allow an empty organizations feed when references are unresolved: the unresolved-share bound still refuses unless `PI_PUBLISH_MAX_UNRESOLVED=1` |
| a run fails at any stage | the alias still points at the previous index, which keeps serving | resume or start a new run |

Things outside the model: losing the object store loses the run's intermediate state (inputs
and stage outputs), so the run starts over; losing a majority of Elasticsearch masters stops the
cluster until they return. Rolling back a published index and recovering a lost cluster or
bucket are in [Rollback and restore](#rollback-and-restore).

## CI

`.github/workflows/ci.yml` runs on GitHub-hosted runners: `rust` (fmt, clippy with
`-D warnings`, tests, all `--locked`), `msrv` (check at `rust-version`), `deploy`
(`scripts/check-deploy.sh` and the scrub check), `lint` (actionlint, shellcheck, hadolint, the
scrub-check, republish-check, restore-drill, S3-endpoint and pin-resolve tests, hooks and scripts stored as 100755), `image`
(docker build, no push), `audit` (cargo-deny: advisories, bans, licenses, sources; also weekly),
`pins` weekly and on manual dispatch (`scripts/check-pins-resolve.sh`: every pinned image digest
still exists in its registry, since a registry can delete one when a tag is re-pushed), and
`e2e` on pushes to main and manual dispatch: the compose stack with a small data set through
`make compose-e2e` (a run that fails in publish, then two runs with the restore drill between
them, so the second publish replaces a restored index and the failed run's index is deleted),
logs uploaded on failure. `make check` runs the same Rust, deploy, scrub and file-mode checks locally.

## Releasing

1. Bump `[workspace.package]` version in `Cargo.toml` and update `Cargo.lock`.
2. Move the `CHANGELOG.md` Unreleased entries into a non-empty `## [X.Y.Z]` section,
   leaving a new Unreleased section for future changes. Merge these changes through review and CI.
3. Create and push the `vX.Y.Z` tag at that commit. The release workflow checks that the tag
   matches the workspace version and the changelog section exists. A manual workflow dispatch
   builds and scans both platforms without pushing an image or creating a release.

The tag run scans fixable HIGH and CRITICAL vulnerabilities on linux/amd64 and linux/arm64,
then publishes `ghcr.io/cristiangirlea/people-index:X.Y.Z` with SBOM and maximum provenance
attestations, signs the image digest using GitHub OIDC, and creates a GitHub release with the
version's changelog notes. The workflow log prints the digest. The scan builds and the published
build are separate builds from the same commit; the published digest is scanned again for both
platforms before signing or creating the release. The version tag is added only after that
digest passes both scans and is signed. The release notes include the digest-pinned image reference.

After the first release, GHCR creates the package private. In GitHub, open the package settings
and change its visibility to public; verify the package page shows Public and an unauthenticated
pull of the pinned reference succeeds.

Verify the digest with cosign (replace the digest and tag with the release values):

```bash
cosign verify ghcr.io/cristiangirlea/people-index@sha256:DIGEST \
  --certificate-identity 'https://github.com/cristiangirlea/people-index/.github/workflows/release.yml@refs/tags/vX.Y.Z' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
```

## Snapshot storage

The Kubernetes snapshots component registers the repository `people-index` on the bucket
`people-index-snapshots` (prefix `snapshots/`) with the separate `snapshot-s3-credentials`
Secret, and installs the SLM policy `nightly` (01:30 UTC, `persons*`, kept 14 days, at least 3
and at most 30 snapshots). The S3 client endpoint carries its scheme (`http://s3:8333`; the
production example `https://s3.eu-west-1.amazonaws.com`) instead of the `protocol` setting,
which Elasticsearch 9 deprecates; `scripts/check-deploy.sh` refuses a rendered endpoint without
a scheme or a `protocol` key (`scripts/check-s3-endpoints.py`, tested by
`scripts/test-check-s3-endpoints.sh`). Applying that change to a running cluster changes every
node set's config, so ECK restarts every Elasticsearch node, one at a time.

Change `SNAPSHOT_BUCKET` in the bootstrap Job per overlay,
and configure the corresponding identity and endpoint. For local SeaweedFS, also update the
matching bucket environment variables in the SeaweedFS identity init and bucket-init Job (or compose);
local keys must contain only letters, digits, underscores or hyphens. Local SeaweedFS scopes each identity
to its own bucket; existing shared-bucket snapshots are not migrated automatically.
If an existing local store created with the previous global admin identity denies access after
an upgrade, back up anything you need, then recreate the store: `docker compose -f deploy/compose/compose.yaml down -v`
removes all Compose volumes, including Elasticsearch data; on Kubernetes delete the SeaweedFS
data PVC after stopping the StatefulSet. This discards the old store and its data.

For production, provision a separate external bucket with versioning. If using object lock,
prefer governance mode and test deletion and repository updates with your provider and policy.
Elasticsearch deletes unreferenced blobs and updates repository metadata; even retention at or
below the SLM `expire_after` of 14 days does not guarantee these operations can succeed sooner.
Follow [Elastic's S3 repository guidance](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/s3-repository)
and grant the snapshot identity only the permissions Elasticsearch needs on that bucket. Do not grant the pipeline
identity access to it. Provisioning and retention incur provider costs; the example does not
create or enable them. Verify snapshot and restore behavior with the chosen retention policy.

## Rollback and restore

`publish` deletes the index the alias served before, so rolling back means restoring
`persons-<run_id>` from a snapshot and moving the alias onto it. The nightly snapshot at 01:30
UTC holds the index that was serving then; a run published later in the day is in no snapshot
until the next night. Restoring needs nothing from the pipeline's store.
`make compose-restore-drill` runs these steps against the compose stack (below).

### Access

```bash
# Compose: the CA from make compose-ca, the elastic password from deploy/compose/.env if it
# sets one, else compose.yaml's development default.
make compose-ca
set -a; if [ -f deploy/compose/.env ]; then . deploy/compose/.env; fi; set +a
ES_ADMIN_PASSWORD="${ELASTIC_PASSWORD:-elastic-dev-password}"
export ES_URL="https://localhost:${ES_PORT:-9200}" ES_CA=target/es-ca.crt
# Kubernetes: ECK's self-signed certificate names the service (es-es-http), not localhost.
kubectl --context <context> -n people-index port-forward svc/es-es-http 9200 &
es_forward=$!   # stop it when done: kill "$es_forward"
kubectl --context <context> -n people-index get secret es-es-http-certs-public \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > es-ca.crt
export ES_URL=https://es-es-http:9200 ES_CA=es-ca.crt ES_RESOLVE=es-es-http:9200:127.0.0.1
ES_ADMIN_PASSWORD="$(kubectl --context <context> -n people-index get secret es-es-elastic-user \
  -o jsonpath='{.data.elastic}' | base64 -d)"

# Every call below, as the elastic superuser; the password goes to curl on stdin.
es() {
  printf 'user = "elastic:%s"\n' "$ES_ADMIN_PASSWORD" |
    curl -K - -sS --fail-with-body --cacert "$ES_CA" ${ES_RESOLVE:+--resolve "$ES_RESOLVE"} \
      -H 'Content-Type: application/json' "$@"
  echo
}
```

On Kubernetes the snapshots component has registered the repository. On compose,
Elasticsearch has the client settings for the snapshot bucket but no keys and no repository:
put the snapshot identity's keys into every node's keystore (piped, never on a command line),
reload them, and register the repository as the component does:

```bash
dc() { docker compose -f deploy/compose/compose.yaml "$@"; }
for node in es01 es02 es03; do
  dc exec -T s3 printenv SNAPSHOT_KEY |
    dc exec -T "$node" bin/elasticsearch-keystore add --stdin --force s3.client.default.access_key
  dc exec -T s3 printenv SNAPSHOT_SECRET |
    dc exec -T "$node" bin/elasticsearch-keystore add --stdin --force s3.client.default.secret_key
done
es -X POST "$ES_URL/_nodes/reload_secure_settings"
es -X PUT "$ES_URL/_snapshot/people-index" \
  -d '{"type":"s3","settings":{"bucket":"people-index-snapshots","base_path":"snapshots","client":"default"}}'
```

The keystore lives in each container, so a recreated container needs the keys again.

### Before you start

1. Stop new runs. Suspend the schedule
   (`kubectl --context <context> -n people-index patch cronjob pi-schedule -p '{"spec":{"suspend":true}}'`)
   and keep it suspended until the cause of the rollback is fixed: the next run reads the same
   feed (`raw/` or the configured `PI_DELIVERY`) and would publish it again.
2. Let a running run finish, or stop it. A run that publishes during the restore moves the
   alias, and its cleanup deletes any `persons-*` index that no alias points at and whose run
   is older: the restored index, until step 4 below puts the alias on it. To stop a run on
   Kubernetes, delete its Jobs (`kubectl --context <context> -n people-index delete job
   pi-run-<run_id> pi-worker-<run_id>`, or the scheduled run's `pi-schedule-<n>`): a killed
   coordinator pod is restarted by its Job and resumes. On compose,
   `docker compose -f deploy/compose/compose.yaml stop coordinator worker`.
   Then check that nothing holds the alias run lease: `_run-leases/persons.json` in the pipeline
   bucket is absent, has `"released": true`, or an `expires_at` (plus a few seconds of clock
   skew) in the past. The restore itself takes no lease.

   ```bash
   # compose
   docker compose -f deploy/compose/compose.yaml run --rm --no-deps --entrypoint aws s3-init \
     --endpoint-url http://s3:8333 s3 cp s3://people-index/_run-leases/persons.json -
   # Kubernetes (the in-cluster store; elsewhere, the pipeline bucket with the pipeline identity)
   kubectl --context <context> -n people-index port-forward svc/s3 8333 &
   s3_forward=$!
   AWS_ACCESS_KEY_ID="$(kubectl --context <context> -n people-index get secret s3-credentials \
     -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)" \
   AWS_SECRET_ACCESS_KEY="$(kubectl --context <context> -n people-index get secret s3-credentials \
     -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)" AWS_REGION=us-east-1 \
     aws --endpoint-url http://localhost:8333 s3 cp s3://people-index/_run-leases/persons.json -
   kill "$s3_forward"
   ```

   A run on the local filesystem store takes no lease, so also list the runs' Jobs:
   `kubectl --context <context> -n people-index get jobs -l app.kubernetes.io/name=people-index`
   (compose: `docker compose -f deploy/compose/compose.yaml ps coordinator worker`).

### Restore and re-point the alias

1. Find the snapshot and the index in it, and what the alias serves now:

   ```bash
   es "$ES_URL/_cat/snapshots/people-index?v&s=end_epoch"
   es "$ES_URL/_snapshot/people-index/<snapshot>?filter_path=snapshots.indices"
   es "$ES_URL/_alias/persons"
   ```

   The nightly policy snapshots every `persons*` index, so a snapshot can also hold indexes
   that never published (a run that failed after loading, or one still loading at 01:30).
   Restore the index whose run has a `runs/<run_id>/summary.json` in the pipeline bucket: only
   a published run writes one (`make compose-runs RUN_ID=<run_id>` on compose).

2. Restore the index under its own name. Publish deleted it, so the name is free:

   ```bash
   es -X POST "$ES_URL/_snapshot/people-index/<snapshot>/_restore?wait_for_completion=true" \
     -d '{"indices":"persons-<run_id>","include_global_state":false,"include_aliases":false}'
   ```

   `include_aliases: false` matters: the snapshot recorded the alias on that index, and
   restoring it would put `persons` on two indexes at once. If the name still exists (an index
   kept for inspection, a damaged copy), restore beside it under a new name by adding
   `"rename_pattern":"(.+)","rename_replacement":"$1-restored"`, which gives
   `persons-<run_id>-restored`. Publish orders runs by the time stamp at the start of the name,
   so the renamed copy still counts as that run. Never restore or rename an index to `persons`
   itself: publish refuses an alias name taken by an index. On a cluster with fewer data nodes
   than replicas + 1, add `"index_settings":{"index.number_of_replicas":0}` and raise it later.

3. Wait for green and check the count against the run's summary (`documents` in
   `runs/<run_id>/summary.json`; `make compose-runs RUN_ID=<run_id>` on compose):

   ```bash
   es "$ES_URL/_cluster/health/persons-<run_id>?wait_for_status=green&timeout=10m"
   es "$ES_URL/persons-<run_id>/_count"
   ```

4. Move the alias in one request: off whatever `persons-*` index it points at, onto the restored
   one. Searches see either the old index or the new one, never both or neither.

   ```bash
   es -X POST "$ES_URL/_aliases" -d '{"actions":[
     {"remove":{"index":"persons-*","alias":"persons","must_exist":false}},
     {"add":{"index":"persons-<run_id>","alias":"persons"}}]}'
   es "$ES_URL/_alias/persons"
   ```

5. Verify: `VERIFY_SOURCE=summary make compose-verify` on compose, or the verify Job with
   `VERIFY_SOURCE=summary` on Kubernetes, finds the run from the alias and checks the restored
   index against that run's `summary.json`. A renamed copy matches no run's index name; check it
   by its count.

The index you rolled back from is now off the alias. The next publish deletes it as an older
run's index; take a snapshot of it first if you need it, or delete it now
(`es -X DELETE "$ES_URL/persons-<bad_run_id>"`).

### What this means for the pipeline

- The next run is newer than the restored one and publishes normally: it compares its count with
  the restored index (`PI_PUBLISH_MIN_SHARE`), moves the alias, deletes the restored index and
  lists it under `previous` and `deleted` in its `summary.json`. `make compose-e2e` publishes over
  a restored index this way.
- Of run ordering, publish refuses only when the alias serves a newer run than its own. After a rollback the alias
  serves an older run, so resuming any failed run newer than the restored one would publish it
  over the rollback: don't, unless that is the intent.
- The store needs nothing. `runs/` keeps every run's history as it was, the rolled-back run
  included (its `state.json` still says `published`); don't edit it, note the rollback in the
  incident record. Segment retention (`PI_RETAIN_RUNS`, default 3) counts runs by their
  `published` state, the rolled-back run included, and removes only the stage data of older
  ones after a publish; the restored run's segments may already be gone, and a restore needs
  none of them: it reads only the snapshot.

### Keeping the last indexes for instant rollback

Proposed default, for the owner to confirm: no. Every kept index is a full copy (primaries and
replicas) on the data nodes, so keeping one previous index doubles the disk the alias needs,
for a rollback that snapshots already provide in minutes. When the previous index does still
exist (a publish that could not delete it lists it under `not_deleted` in its `summary.json`),
rolling back is step 4 alone, with that index's name. Keeping N indexes on purpose would need a
publish option that skips deleting them; none exists.

### Disaster recovery

- **Elasticsearch cluster lost.** Stop runs first ([Before you start](#before-you-start),
  steps 1 and 2): a publish in the gap deletes the restored index while no alias points at it,
  and applying an overlay that includes the schedule component recreates `pi-schedule`
  unsuspended, so the apply below suspends it. Create the new cluster from the overlay without
  the snapshot bootstrap Job: the rest of the snapshots component (the S3 client settings, the
  keys in the keystore through `secureSettings`, the NetworkPolicy to the store) is what a
  restore needs, but the Job would register the repository writable and install the `nightly` policy. Two
  clusters writing one repository corrupt it, so while the old cluster might still be writing,
  the new one only reads it:

  ```bash
  kubectl kustomize deploy/k8s/overlays/<overlay> |
    python3 -c 'import sys, yaml; docs = [d for d in yaml.safe_load_all(sys.stdin) if d and not (d["kind"] == "Job" and d["metadata"]["name"] == "snapshot-bootstrap")]; [d["spec"].update(suspend=True) for d in docs if d["kind"] == "CronJob"]; yaml.safe_dump_all(docs, sys.stdout)' |
    kubectl --context <context> apply -f -
  es -X PUT "$ES_URL/_snapshot/people-index" \
    -d '{"type":"s3","settings":{"bucket":"people-index-snapshots","base_path":"snapshots","client":"default","readonly":true}}'
  ```

  Restore the newest snapshot's published index and point the alias at it (steps 1 to 4 above;
  the step 4 request also clears an alias a run may have created on the new cluster).
  Snapshots carry no global state: users and roles come from the deployment (ECK's file realm,
  compose's `es-users`). Once the old cluster is gone for good, apply the whole overlay as
  usual, the Job included: its PUT of the full definition makes the repository writable again
  and installs `nightly`. That apply resumes the schedule too; patch it suspended again
  (Before you start, step 1) until runs may publish. A run published after the newest
  snapshot is lost from Elasticsearch: start a new run on the same feed.
- **Pipeline bucket lost.** The alias keeps serving; only the store's contents are gone: the
  feeds in `raw/`, every run's history in `runs/` and the run lease. Recreate the bucket
  (`s3-bucket-init`, or `s3-init` on compose), deliver the feeds again from their source and start
  a new run; it publishes over the serving index as usual. The history of earlier runs cannot be
  recovered, so `verify --source summary` works only for runs from then on.
- **Snapshot bucket lost.** Nothing serving is affected, but there is nothing to roll back to
  until a new snapshot exists. Recreate the bucket (with versioning, see Snapshot storage),
  register the repository again (`es -X DELETE "$ES_URL/_snapshot/people-index"`, then
  `kubectl --context <context> -n people-index delete job snapshot-bootstrap --ignore-not-found`
  and apply the overlay again) and take a snapshot at once:
  `es -X POST "$ES_URL/_slm/policy/nightly/_execute"`.

### The drill

`make compose-restore-drill` (`deploy/compose/scripts/restore-drill.sh`, run by
`make compose-e2e` after the first run publishes) works on the index the alias serves:

1. On the host, it pipes the snapshot identity's keys from the `s3` service's environment into
   every node's keystore (`elasticsearch-keystore add --stdin`), so they never appear on a
   command line, and calls `_nodes/reload_secure_settings` (failing on a node's
   `reload_exception`): the S3 keys are reloadable secure settings, so no node restarts. A node
   that already has both keys keeps them; one with only one of them is refused. The client
   settings (`s3.client.default.endpoint`, path-style access, region) are static, in
   compose.yaml.
2. It registers the repository `people-index` as the Kubernetes component does; the
   registration verifies every node (a node without the keys fails it, naming the node).
3. It snapshots the index, deletes it (the alias goes with it: the reader gets 404), restores it
   under its own name with `include_aliases: false`, waits for green, compares the count and
   moves the alias with the request in step 4, and checks the reader user's search through the
   alias.
4. It restores it again beside itself as `<index>-restored`, moves the alias there and back,
   checks that the reader cannot search the index by name, and deletes the copy.
5. It deletes its snapshot, and the repository registration only when it created it, removes
   the keys it added and reloads. Keys or a repository that were there before are left as they
   were. A failure keeps the snapshot so the index can still be restored by hand, and still
   removes the keys: put them back as in Access above before restoring from it.

`make compose-e2e` then verifies the restored index against `expected.json` (the content digest
included) and publishes a second run over it. `scripts/test-restore-drill.sh` (CI's lint job,
`make check`) runs the drill against a stub docker and a fake Elasticsearch: its refusals and
failures leave no keys behind, a failed restore keeps the snapshot, and keys or a repository
that were there before stay.

Exercised on 2026-10-28 against the compose stack (Elasticsearch 9.5.5, three nodes, SeaweedFS
4.48; Docker Desktop on Windows, `make compose-e2e` with CI's sizes), three times. The last run:
index `persons-20261028-221205`, 20000 documents; snapshot 0.9 s; restore under its own name to
green 3.0 s, the renamed copy 2.2 s; the counts, the reader's search through the alias and the
content digest (`verify`) matched, and the second run then published over the restored index
and deleted it. The whole `make compose-e2e` took 4 min 4 s (the earlier runs: snapshot 1.1 s,
restores 2.6 to 3.0 s and 2.4 to 2.5 s, about 4 min 50 s). Not yet exercised on k3d or against
an external S3 store.

## Scheduled runs

A scheduled run reads whatever `PI_DELIVERY` the ConfigMap sets. With it unset, each run
re-indexes `raw/`.

Add `../../components/schedule` to an overlay's `components` and apply that overlay.
The proposed defaults run at 02:00 UTC daily; patch the CronJob's `schedule`, `timeZone`,
image and resources for the cluster. Its Indexed Job has one coordinator and three workers;
change `completions` and `parallelism` together to change the worker count. Suspend it with
`kubectl --context k3d-people-index -n people-index patch cronjob pi-schedule -p '{"spec":{"suspend":true}}'`.

`Forbid` prevents overlapping Jobs from this CronJob. A store lease also excludes manual runs
for the same alias: a different run fails with `a run is already in progress`. If a manual
coordinator is refused before it creates run state, stop its separately started worker Job. The same run id
may resume, taking over its lease; do not deliberately start two coordinators for that id.
A crashed coordinator's lease expires after `PI_LEASE_TTL` plus clock skew. If it is still
live, stop the holder and wait for expiry; only remove `_run-leases/<alias>.json` from the
store after ensuring no coordinator is running. The run lock needs conditional updates (S3 or memory); local filesystem runs proceed without it. Hand-picked run ids without
the UTC stamp are not ordered by publish.

Start a scheduled-style run by hand using a current UTC minute in its Job name:

```sh
kubectl --context k3d-people-index -n people-index create job --from=cronjob/pi-schedule pi-schedule-$(( $(date -u +%s) / 60 ))
```

Use your own context in production.

The run stamp is the scheduled minute, so a late Job within `startingDeadlineSeconds` can
lose to a manual run started in the meantime. Scheduled worker failures exit 2 and count
against the Job backoff limit; coordinator failures exit 1 and fail the Job.

The schedule component defaults to 1 GiB and a 7200 second deadline. For the production
example, patch the CronJob to match its task sizing and 43200 second deadline. For example,
add this strategic merge patch to the overlay after enabling the component:

```yaml
patches:
  - target:
      kind: CronJob
      name: pi-schedule
    patch: |-
      apiVersion: batch/v1
      kind: CronJob
      metadata:
        name: pi-schedule
      spec:
        jobTemplate:
          spec:
            activeDeadlineSeconds: 43200
            template:
              spec:
                containers:
                  - name: people-index
                    image: ghcr.io/cristiangirlea/people-index:0.1.0@sha256:<digest>
                    imagePullPolicy: IfNotPresent
                    resources:
                      requests:
                        cpu: "2"
                        memory: 4Gi
                      limits:
                        cpu: "2"
                        memory: 4Gi
                        ephemeral-storage: 8Gi
                volumes:
                  - name: tmp
                    emptyDir:
                      sizeLimit: 8Gi
```

Also configure zone spread and node anti-affinity for the cluster; the shared indexed pod
shape deliberately has no hard worker-only spread rule.
## A feed delivery

For a new delivery `batch-20261028`, place gzipped NDJSON files under
`raw/batch-20261028/persons/` and `raw/batch-20261028/organizations/`. Place an optional
`expected.json` beside those directories for synthetic verification. After all files are
written, create the empty `raw/batch-20261028/_COMPLETE` object. Then run with
`PI_DELIVERY=batch-20261028 PI_REQUIRE_COMPLETE_MARKER=true`; use the same `PI_DELIVERY`
for `verify --source expected`. Never modify a completed delivery; use a new prefix.

For a synthetic delivery, `people-index datagen --delivery batch-20261028` writes the feeds
and `expected.json` there; create `_COMPLETE` separately after it succeeds. An explicit
`datagen --out` names the exact destination instead. With `PI_INPUT_URL`, these paths are
relative to that input prefix rather than `raw/`.

## Feed schema

The pipeline reads a few names in the feeds: the envelope keys (`id`, `record`), the person's
list of references to organizations (`roles`) with each reference's organization id
(`organization_id`), and the mapped fields whose JSON kinds extraction checks
([ARCHITECTURE.md](ARCHITECTURE.md), "The data"). Those are the built-in schema. A feed that
names them differently is described by two files, read once at startup by `run`, `worker` and
`stage`:

- a **schema file**, `PI_SCHEMA_FILE` (`--schema-file`): the names, below;
- a **mapping file**, `PI_MAPPING_FILE` (`--mapping-file`): the index mapping with the same
  names, the `mappings` body of a create-index request, as `crates/pi-es/src/mapping.json` is.

Both unset (or empty, as compose passes an unset variable) means the built-in schema and
mapping: a deployment that sets neither changes nothing.

### The schema file

A JSON object; every key is optional, and a key left out keeps its built-in value. A table
that is given replaces the built-in table whole (it is not merged).

| key | meaning | built-in |
|---|---|---|
| `envelope.id` | the envelope key of the person or organization id | `id` |
| `envelope.record` | the envelope key of the record | `record` |
| `person.references` | the person field listing references to organizations | `roles` |
| `person.reference_id` | the field of a reference holding the organization id | `organization_id` |
| `person.fields` | the checked top-level person fields: `[{"name", "kind"}]` | `PERSON_FIELDS` |
| `person.reference_fields` | the checked fields of each reference | `ROLE_FIELDS` |
| `person.disabled` | person fields the mapping disables (`enabled: false`) | `memberships`, `educations`, `certifications`, `languages` |
| `organization.fields` | the checked organization fields | `ORGANIZATION_FIELDS` |
| `organization.disabled` | organization fields the mapping disables | `addresses`, `locations`, `funding_rounds` |

The tables are in `crates/pi-model/src/shape.rs`. A `kind` is the JSON a field takes, and must
fit the field's type in the mapping:

| kind | takes | mapping type |
|---|---|---|
| `number` | a number, or a string of decimal digits within `u64` (strict: anything else quarantines the record) | `long` or `integer` with `ignore_malformed: true` |
| `boolean` | `true`, `false`, `"true"`, `"false"`, `""` (strict) | `boolean` |
| `lenient_long`, `lenient_integer`, `lenient_boolean` | any scalar or array of scalars; values the index skips are counted, not quarantined | `long`, `integer`, `boolean` with `ignore_malformed: true` |
| `string`, `string_array` | any scalar or array of scalars | `text`, `keyword`, or `date` with `ignore_malformed: true` |
| `object_array` | an array of objects: only the references field | `object` |

Keyword fields with an index or doc values, including multi-fields, need an integer
`ignore_above` at most 8191; keywords with `index: false` and `doc_values: false`, and fields inside disabled objects,
are exempt. Elasticsearch measures UTF-16 code units (an emoji counts two); each unit takes
at most three UTF-8 bytes, so 8191 units fit below Lucene's 32,766-byte limit. Values above
the limit remain in `_source` and appear in `_ignored`. Extraction counts each input value
once in `values_ignored_by_index`, even if several keyword fields ignore it; array elements
count separately, before deduplication and assembly.

Every `long`, `integer` and `date` field needs `ignore_malformed: true`, whatever its kind: an
id above `i64::MAX` passes `number`, a value past `i32` fits no `integer`, and a `string` takes
text no date format reads; without it Elasticsearch fails the document and the load with it.

The file is refused at startup, with the file and the field named (`PI_SCHEMA_FILE
/etc/people-index/feed/schema.json: person.fields[2].name: "x" is listed twice (also at
person.fields[1].name)`), for: an unknown key (anywhere), a section or field entry that is not
an object, an empty name, a field name with a `.` (Elasticsearch reads it as an object path),
the same envelope key for `id` and `record`, a name listed twice in one object's tables
(`fields` and `disabled` together), a references field missing from `person.fields` or not an
`object_array` (or another `object_array` field), the organization id (`person.reference_id`)
listed in `person.reference_fields` with a kind other than `number` or `string` (the join reads
it by the id rule anyway), and a name the document builder writes:
`organizations` and `unresolved_organization_ids` on the person, `organization_resolved` in a
reference. Those three output names are fixed whatever the feed calls its references: the
document gets `organizations[]`, `unresolved_organization_ids[]` and
`<references>[].organization_resolved` ("The output document").

A complete example, for a feed whose lines look like
`{"key": 7, "changed": "2026-10-28T12:00:00Z", "payload": {"given_name": "Ada", "positions":
[{"company_ref": 3, "position_title": "Engineer"}]}}` and
`{"key": 3, "payload": {"label": "Acme", "offices": [...]}}` (a test reads this block back and
runs a renamed feed through it, `crates/people-index/tests/feed_schema.rs`):

<!-- feed-schema-example -->
```json
{
  "envelope": {"id": "key", "record": "payload"},
  "person": {
    "references": "positions",
    "reference_id": "company_ref",
    "fields": [
      {"name": "id", "kind": "number"},
      {"name": "given_name", "kind": "string"},
      {"name": "family_name", "kind": "string"},
      {"name": "full_name", "kind": "string"},
      {"name": "headline", "kind": "string"},
      {"name": "summary", "kind": "string"},
      {"name": "country", "kind": "string"},
      {"name": "region", "kind": "string"},
      {"name": "city", "kind": "string"},
      {"name": "industry", "kind": "string"},
      {"name": "profile_url", "kind": "string"},
      {"name": "skills", "kind": "string_array"},
      {"name": "connections", "kind": "lenient_long"},
      {"name": "followers", "kind": "lenient_long"},
      {"name": "open_to_work", "kind": "lenient_boolean"},
      {"name": "positions", "kind": "object_array"}
    ],
    "reference_fields": [
      {"name": "id", "kind": "number"},
      {"name": "position_title", "kind": "string"},
      {"name": "company_ref", "kind": "number"},
      {"name": "organization_name", "kind": "string"},
      {"name": "organization_domain", "kind": "string"},
      {"name": "employment_type", "kind": "string"},
      {"name": "start_date", "kind": "string"},
      {"name": "end_date", "kind": "string"},
      {"name": "location", "kind": "string"},
      {"name": "description", "kind": "string"}
    ],
    "disabled": ["memberships", "educations", "certifications", "languages"]
  },
  "organization": {
    "fields": [
      {"name": "id", "kind": "number"},
      {"name": "label", "kind": "string"},
      {"name": "domain", "kind": "string"},
      {"name": "industry", "kind": "string"},
      {"name": "country", "kind": "string"},
      {"name": "city", "kind": "string"},
      {"name": "size_range", "kind": "string"},
      {"name": "employee_count", "kind": "lenient_long"},
      {"name": "founded_year", "kind": "lenient_integer"},
      {"name": "keywords", "kind": "string_array"},
      {"name": "technologies", "kind": "string_array"},
      {"name": "description", "kind": "string"}
    ],
    "disabled": ["offices", "locations", "funding_rounds"]
  }
}
```

### The mapping file

Start from a copy of `crates/pi-es/src/mapping.json` (its reasons are in `mapping.md` next to
it) and rename the same fields. For the example above: `first_name` and `last_name` become
`given_name` and `family_name` (keep their `copy_to: full_name`), `roles` becomes `positions`
with `title` and `organization_id` renamed inside it (and `positions.organization_resolved`, a
`boolean`, added by the builder, kept), `organizations.name` becomes `organizations.label` and
`organizations.addresses` becomes `organizations.offices`.

Your own fields can be added to the mapping, each with an entry in the matching schema table
(`person.fields`, `person.reference_fields` or `organization.fields`) of a kind its type takes,
as long as they hold scalars or arrays of scalars. No kind takes an object other than the
references, so a field of your own that holds one is never indexed: on the person or the
organization, map it `{"type": "object", "enabled": false}` and list it under
`person.disabled` or `organization.disabled`; inside a reference, leave it out of the mapping
(there is no disabled list for references). Either way it stays in `_source`.

At startup the mapping and the schema are checked against each other, and a run does not start
when they disagree; the error lists every problem with its mapping path and both sources
(`PI_SCHEMA_FILE: ..., PI_MAPPING_FILE: built-in`):

- `"dynamic": false` at the top, and no other `dynamic` value anywhere: unknown fields stay in
  `_source`, unindexed;
- every field Elasticsearch parses (not disabled, not written by the builder) is in a schema
  table with a kind its type takes (the table above), and every table entry is mapped; every
  `long`, `integer` and `date` field has `ignore_malformed: true`; a type without a rule here
  (`nested`, `float`, `wildcard`, ...) is refused;
- every keyword with an index or doc values, including multi-fields, has integer `ignore_above` at most 8191;
  keywords with `index: false` and `doc_values: false`, and fields inside disabled objects, are exempt;
- text fields and text multi-fields may omit `analyzer`, `search_analyzer`, and
  `search_quote_analyzer`, or set each to `standard`, `simple`, `whitespace`, or `english`;
  other names can emit immense terms that fail indexing or queries, and are refused even on unindexed text;
  fields inside disabled objects are exempt. Do not use index templates to override these built-in
  analyzers or the default analyzer: the mapping check cannot inspect index settings;
- every multi-field (under `"fields"`) takes what its field lets through: `text` and `keyword`
  always, `long`, `integer`, `date` and `boolean` with `ignore_malformed: true`, anything else
  only if it fits the field's kind;
- every `copy_to` target is a `text` or `keyword` field;
- the fields with `"enabled": false` are exactly the schema's `disabled` lists;
- `organizations` is an `object`, `unresolved_organization_ids` a `long` with
  `ignore_malformed: true` and `<references>.organization_resolved` a `boolean`.

### Running with them

Every process of a run reads the same files: the coordinator, every worker and `people-index
stage`. `plan.json` records the SHA-256 of both, as canonical JSON
(`"schema": {"feed_schema_sha256": ..., "mapping_sha256": ...}`; key order and whitespace do not
matter, and a schema file that spells out the built-in names in the built-in order is the
built-in schema: tables are compared in order), and every
task checks them before it starts: a resumed run, or a worker, with another schema or mapping
fails the run for good (`run 20261028T120000Z-a1b2c3 was planned with another feed schema or
index mapping: the feed schema (PI_SCHEMA_FILE) has sha256 ..., the plan ...`). Resume it with the
files it started with; a changed schema is a new run. A plan written before this field existed
counts as the built-in schema and mapping. `summary.json` repeats the pin.

**Compose** passes `PI_SCHEMA_FILE` and `PI_MAPPING_FILE` from `deploy/compose/.env` to every
pipeline container, empty by default. The files must be inside the containers, for instance with
a `deploy/compose/compose.override.yaml` next to `compose.yaml` (Compose reads it by itself):

```yaml
services:
  coordinator:
    volumes: [./feed:/etc/people-index/feed:ro]
  worker:
    volumes: [./feed:/etc/people-index/feed:ro]
```

and in `.env`: `PI_SCHEMA_FILE=/etc/people-index/feed/schema.json` and
`PI_MAPPING_FILE=/etc/people-index/feed/mapping.json`. Compose merges volumes by their path in
the container, so these add to the pipeline's own (`es-ca`, `/tmp`).

**Kubernetes**: neither variable is set in `people-index-config` (base); an overlay that uses
files puts them in a ConfigMap, sets the variables and mounts the ConfigMap into every pipeline
pod. Not enabled anywhere by default; for an overlay with `schema.json` and `mapping.json` next
to its `kustomization.yaml`:

```yaml
configMapGenerator:
  - name: people-index-feed-schema
    files:
      - schema.json
      - mapping.json
  - name: people-index-config
    behavior: merge
    literals:
      - PI_SCHEMA_FILE=/etc/people-index/feed/schema.json
      - PI_MAPPING_FILE=/etc/people-index/feed/mapping.json
```

(with the overlay's `generatorOptions: disableNameSuffixHash: true`, as the local overlay has,
so the Jobs find the fixed name), and in both Jobs of `deploy/k8s/jobs/run.yaml` (and the
CronJob of the schedule component, if used) one more volume and mount:

```yaml
          volumeMounts:
            - name: feed-schema
              mountPath: /etc/people-index/feed
              readOnly: true
      volumes:
        - name: feed-schema
          configMap:
            name: people-index-feed-schema
```

A changed ConfigMap reaches the next run's pods; a run already planned refuses it, as above.

### What stays built-in

- `people-index datagen` writes feeds in the built-in schema only, and its `expected.json`
  counts role titles on `roles.title.keyword` and organization names on
  `organizations.name.keyword`: `verify --source expected` is for those synthetic feeds.
- `verify --source summary` reads the run's pin from `summary.json`: for a run with another
  schema or mapping it checks `alias`, `count` and `sample:ids`, and leaves out
  `sample:full_name` and `sample:organization_id`, which query the built-in mapping's
  `full_name` and `organizations.id`.
- Counter and summary names keep the built-in words whatever the feed calls its references
  (`roles`, `roles_resolved`, `roles_unresolved` in `assemble`'s counters, `resolved_roles` and
  `unresolved_roles` in `summary.json`), and `PI_PUBLISH_MAX_UNRESOLVED` bounds the share of
  references whose organization did not resolve.
