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
| object store | SeaweedFS on `s3:8333`, bucket `people-index`, created and probed by `s3-init` | SeaweedFS service `s3:8333`, bucket created by Job `s3-bucket-init`; credentials in secret `s3-credentials` |
| a run | services `coordinator` (`people-index run`) and `worker` (`people-index worker`, `--scale worker=N`) | Job `pi-run-<run_id>` and Job `pi-worker-<run_id>` (`parallelism: N`, spread over zones) |
| synthetic data | service `datagen` (profile `datagen`) | Job `pi-datagen-<id>` |
| metrics | Prometheus (profile `metrics`) | `deploy/k8s/components/monitoring` (Prometheus Operator) |

Role `people-index-writer` grants cluster `monitor` and every privilege on `persons` (the alias)
and `persons-*` (its `persons-<run_id>` indexes) and nothing else; `roles.yml` lists the calls
this covers. If you change `PI_ALIAS`, the role's index names follow it in compose (`es-users`
reads `PI_ALIAS`); on Kubernetes change `deploy/k8s/base/elasticsearch/roles.yml` too.

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

`make compose-e2e` (CI's end-to-end) is `make compose-run`, then a second run (`RUN_ID` with
`-2` appended) over the same feeds, `make compose-verify` again, and
`deploy/compose/scripts/check-republish.sh`: the alias points only at the second run's index,
the first run's index is gone, no other `persons-*` index is left, and the second run's
`summary.json` lists the first index under `previous` and `deleted` with `not_deleted` empty.
That second publish is the only place the pipeline user moves the alias off an index and
deletes it against a real cluster, the calls role `people-index-writer` must allow.

`make compose-up` alone starts Elasticsearch, the store and the one-shot setup services, and
waits until `certs`, `es-users` and `s3-init` have succeeded:

- `certs` creates the CA and one certificate per node once (they persist in the `certs` volume)
  and copies only the public CA into the `es-ca` volume, which the pipeline mounts read-only at
  `/etc/people-index/es-ca/ca.crt` (`PI_ES_CA_CERT`). Private keys never leave the `certs`
  volume.
- `es-users` waits for a formed three-node cluster, then creates the role and the user.
- `s3-init` creates the bucket and runs the conditional-write probe.

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
  and `s3-credentials`.
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
  run Jobs), runs `people-index verify --expected s3://people-index/raw/expected.json --digest`
  against the alias, waits for the Job and prints its output; a failed check fails the target.
  Size it with `VERIFY_CPU`, `VERIFY_MEMORY` and `VERIFY_DEADLINE_SECONDS` in `jobs.vars`.

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
  `open_to_work`), following the rules Elasticsearch 9.5.5 was observed to apply (they and the
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

What to watch during a load: a rising rate of bulk retries means the cluster is pushing back
(HTTP 429); task retries and lease takeovers mean workers are dying or stalling.

## Sizing

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
| an Elasticsearch data node is lost during the load | bulk requests are retried with backoff and only failed items are resent; `publish` refuses to move the alias while shards are missing or the count differs | restore the node and resume; a count mismatch fails the run for good (its load tasks are already committed), so start a new run |
| too many malformed lines (`PI_MAX_MALFORMED`) | one file over the limit fails its extract task at once; a feed over the limit in total fails the next stage (`extract-persons` for organizations, `join` for persons); quarantined lines are in `runs/<run_id>/quarantine/` | fix the input, start a new run |
| the store refuses conditional writes | `s3-init` (compose) stops the stack before any run; never run the distributed executor on such a store | use a conforming S3 server |
| a run fails at any stage | the alias still points at the previous index, which keeps serving | resume or start a new run |

Things outside the model: losing the object store loses the run's intermediate state (inputs
and stage outputs), so the run starts over; losing a majority of Elasticsearch masters stops the
cluster until they return.

## CI

`.github/workflows/ci.yml` runs on GitHub-hosted runners: `rust` (fmt, clippy with
`-D warnings`, tests, all `--locked`), `msrv` (check at `rust-version`), `deploy`
(`scripts/check-deploy.sh` and the scrub check), `lint` (actionlint, shellcheck, hadolint, the
scrub-check and republish-check tests, hooks and scripts stored as 100755), `image` (docker
build, no push), `audit` (cargo-deny: advisories, bans, licenses, sources; also weekly), and
`e2e` on pushes to main and manual dispatch: the compose stack with a small data set through
`make compose-e2e` (two runs, so the second publish replaces the first), logs uploaded on
failure. `make check` runs the same Rust, deploy, scrub and file-mode checks locally.
