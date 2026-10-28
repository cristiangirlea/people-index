# Kubernetes deployment

The multi-node deployment of people-index: Elasticsearch run by ECK with dedicated masters and
zone-aware data nodes, an S3-compatible object store, and each pipeline run as a coordinator
Job plus a worker Job. [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) is the contract for
the stages, the storage layout and the `PI_*` settings used here.

## Layout

| path | what it holds |
|---|---|
| `../k3d/cluster.yaml` | a local cluster: 1 server (control plane only) + 3 agents in zones `zone-a`, `zone-b`, `zone-c` |
| `eck/` | `fetch.sh`: downloads the ECK 3.5.0 operator and CRDs from Elastic's release URLs and keeps them only when they match `SHA256SUMS`; `pin-operator.sh`: pins the operator image to the digest in `operator-image` |
| `base/` | namespace, Elasticsearch (`es`: 3 masters, 3 data nodes), SeaweedFS (`s3:8333`) and its bucket Job, the pipeline ServiceAccount, the `people-index-config` ConfigMap, NetworkPolicies |
| `components/snapshots/` | S3 snapshot repository `people-index` (prefix `snapshots/`) and the SLM policy `nightly` |
| `components/schedule/` | optional CronJob for indexed coordinator and worker runs |
| `components/monitoring/` | elasticsearch-exporter, ServiceMonitors, a PodMonitor for the pipeline's `:9464`, alert rules; needs the prometheus-operator CRDs and kube-state-metrics with Job labels allowlisted (see below) |
| `overlays/local/` | k3d sizing (about 7.5 GiB for Elasticsearch), snapshots, `imagePullPolicy: Never` for `people-index:local` |
| `overlays/production-example/` | 6 data nodes, external S3 instead of SeaweedFS, monitoring and snapshots, sizing notes |
| `jobs/` | `run.yaml` (coordinator + workers), `datagen.yaml` and `verify.yaml`, rendered by `render.sh` from an overlay's `jobs.vars` |
| `gen-secrets.sh` | writes an overlay's git-ignored credential env files from its `*.env.example` |

Names other parts rely on: namespace `people-index`; Elasticsearch HTTP service `es-es-http:9200`
(TLS, CA in Secret `es-es-http-certs-public` key `ca.crt`); file-realm user `pipeline` with role
`people-index-writer`, credentials in Secret `pipeline-es-credentials` (`username`, `password`);
object store service `s3:8333`, bucket `people-index`, credentials in Secret `s3-credentials`
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

## Local cluster

Every command names the context. The k3d config adds `k3d-people-index` to the kubeconfig
without switching to it, so no command here can reach another cluster by accident.

```bash
k3d cluster create --config deploy/k3d/cluster.yaml
docker build -t people-index:local .
k3d image import people-index:local -c people-index

deploy/k8s/gen-secrets.sh local
deploy/k8s/eck/fetch.sh target/eck      # fails, keeping nothing, on a checksum mismatch
bash deploy/k8s/eck/pin-operator.sh target/eck/operator.yaml >target/eck/operator-pinned.yaml
kubectl --context k3d-people-index apply --server-side -f target/eck/crds.yaml -f target/eck/operator-pinned.yaml
kubectl --context k3d-people-index -n elastic-system rollout status statefulset/elastic-operator
kubectl --context k3d-people-index apply --server-side -k deploy/k8s/overlays/local
kubectl --context k3d-people-index -n people-index wait elasticsearch/es \
  --for=jsonpath='{.status.health}'=green --timeout=15m
```

Server-side apply because the ECK CRDs are too large for the annotation client-side apply keeps.
Docker needs at least 10 GiB of memory for the local overlay.

Generate inputs, then run the pipeline:

```bash
deploy/k8s/jobs/render.sh datagen | kubectl --context k3d-people-index apply -f -
RUN_ID=demo-1 deploy/k8s/jobs/render.sh run | kubectl --context k3d-people-index apply -f -
kubectl --context k3d-people-index -n people-index wait job/pi-run-demo-1 --for=condition=complete --timeout=2h
kubectl --context k3d-people-index -n people-index logs -f job/pi-run-demo-1
JOB_ID=demo-1 deploy/k8s/jobs/render.sh verify | kubectl --context k3d-people-index apply -f -
kubectl --context k3d-people-index -n people-index wait job/pi-verify-demo-1 --for=condition=complete --timeout=30m
kubectl --context k3d-people-index -n people-index logs job/pi-verify-demo-1
```

`render.sh` reads `overlays/<overlay>/jobs.vars` (default overlay `local`); any variable in the
environment wins (`WORKERS=6`, `IMAGE=...`). `RUN_ID` defaults to the UTC time plus a random
suffix; re-applying a run with the same `RUN_ID` after its Jobs are gone resumes it from the
commits in the store.

Reach the services from the host with port-forwards:

```bash
kubectl --context k3d-people-index -n people-index port-forward svc/es-es-http 9200
kubectl --context k3d-people-index -n people-index get secret pipeline-es-credentials \
  -o jsonpath='{.data.password}' | base64 -d     # user `pipeline`
```

Failure drills: `docker stop k3d-people-index-agent-1` takes a whole zone away (one master, one
data node, that zone's workers). The masters keep a quorum; the published index stays
searchable because its replicas sit in other zones. Workers on the lost node lose their leases
and the other workers take their tasks over. The index being loaded has no replicas until
`publish`, so its shards on the lost node are unavailable until `docker start` brings the node
(and its volume) back; the loader retries meanwhile, and `publish` refuses to move the alias if
documents are missing.

The monitoring component requires Prometheus to scrape kube-state-metrics configured with
`--metric-labels-allowlist=jobs=[app.kubernetes.io/name,app.kubernetes.io/component]`.

## Security

- Pods run as non-root with a read-only root filesystem, no capabilities, the RuntimeDefault
  seccomp profile and no service-account token. The namespace enforces the `baseline` Pod
  Security level and warns on `restricted`.
- NetworkPolicies deny all ingress by default. Clients opt in by label:
  `people-index.io/es-client: "true"` reaches Elasticsearch on 9200 and
  `people-index.io/s3-client: "true"` reaches the store on 8333. The pipeline's own egress is
  limited to Elasticsearch, the store and DNS (production: HTTPS outside private ranges, the
  metadata service excluded).
- Elasticsearch users are least-privilege file-realm users: `pipeline` (cluster `monitor`, all
  on `persons` and `persons-*`) and, with monitoring, `exporter` (read-only).
  Role definitions are in `base/elasticsearch/roles.yml` and `components/monitoring/monitor-role.yml`;
  both were exercised against Elasticsearch 9.5.5.
- Credentials never enter git: overlays generate Secrets from `secrets/*.env` files that
  `.gitignore` excludes; `gen-secrets.sh` fills them with random values. Scripts pass secrets to
  curl on stdin, never on a command line.

## The object store

SeaweedFS 4.48 (`chrislusf/seaweedfs:4.48`, `weed server -s3`) stands in for S3 locally; MinIO no
longer publishes container images. The commit and lease protocol depends on conditional writes,
probed against this image with the same flags and security context as the StatefulSet:

| request | status |
|---|---|
| PUT `If-None-Match: *`, new key | 200 |
| PUT `If-None-Match: *`, existing key | 412 |
| PUT `If-Match: <current etag>` | 200 |
| PUT `If-Match: <stale etag>` | 412 |
| PUT `If-Match` on a missing key | 412 |
| 16 concurrent `If-None-Match: *` PUTs on one new key (3 rounds) | one 200, fifteen 412, each round |
| 16 concurrent `If-Match` PUTs with the same etag (3 rounds) | one 200, fifteen 412, each round |
| unsigned request / wrong secret | 403 |

It runs as one replica: its data (generated inputs, run intermediates, snapshots of a rebuildable
index) can be recreated, and production uses a managed object store.

## Checks

`scripts/check-deploy.sh` (CI) checks that every image (Dockerfile, compose, k3d, the rendered
manifests, the ECK operator, the Job templates, CI's lint images) is pinned as
`name:tag@sha256:<digest>` and that the Dockerfile's Rust is CI's `RUST_TOOLCHAIN`; fetches the
ECK files with `eck/fetch.sh`, which keeps them only when they match `eck/SHA256SUMS` (and
checks that it refuses a mismatch), and pins the operator image in `operator.yaml` to
`eck/operator-image` with `eck/pin-operator.sh`, as `make eck` does; renders `base`, every
overlay and the Job templates; validates all of it with kubeconform `-strict` (Kubernetes
schemas plus the datreeio CRDs-catalog, at a pinned commit, for the custom resources);
validates the k3d config against k3d's schema; parses the shell scripts (and runs shellcheck
when installed); and runs `docker compose config -q` on `deploy/compose/compose.yaml`. It needs
no cluster.
