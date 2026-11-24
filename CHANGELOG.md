# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows the compatibility policy in [docs/INTEGRATING.md](docs/INTEGRATING.md#compatibility-policy).

## [Unreleased]

## [0.2.1] - 2026-11-24

### Security

- Update `url` to 2.5.9 (RUSTSEC-2026-0592: a stack overflow in `Url::origin()` on nested `blob:`
  URLs). people-index never calls `origin()`; its dependencies `object_store` and `reqwest` use
  `url` on configured endpoints.

### Changed

- Release images are signed with cosign v3 (was v2.6.0), in the Sigstore bundle format stored as
  an OCI referrer, and the release workflow verifies the signature before it adds the version
  tag. Verify this release with cosign v3. cosign v2 needs `--new-bundle-format` added (checked
  on a test image signed by cosign v3); without it, it reports no signatures. 0.1.0 and 0.2.0
  still verify with v2 and v3.
- The production example overlay runs the signed 0.2.0 image, pinned by digest.
- Compose gives each Elasticsearch node 1536m (`ES_MEM_LIMIT`, was 1g) with the same 512m heap:
  measured at 1536m under load, a node holds about 1.2 GiB of anonymous memory, and at 1g the
  kernel killed nodes for memory, which failed loads or left a short index for the publish guard
  to refuse.
- The k3d local overlay's Elasticsearch masters get 1280Mi (was 1Gi) with the same 512m heap: in
  a measured 200,000-person run they came within 51-87 MiB of 1Gi, and at 1280Mi they keep
  259-328 MiB. The data nodes, measured 190-203 MiB below 1536Mi, keep it. Give Docker at least
  15 GiB (was 14).

## [0.2.0] - 2026-11-20

### Added

- `PI_DELIVERY=latest`: a run plans the newest delivery that has its `_COMPLETE` marker (by
  name), records it in `plan.json`, and is skipped (`runs/<id>/skipped.json`, exit 0, its
  workers too) when the alias already serves that delivery or a newer one, unless
  `PI_REPUBLISH_UNCHANGED_DELIVERY=true`; a delivery can no longer be named `latest`. A skipped
  `run` prints `{"run_id", "status": "skipped", "delivery", "serving_run", "reason"}` instead of
  the stage lists and counts `pi_runs_skipped_total`.
- A security policy (`SECURITY.md`) for reporting vulnerabilities privately.

### Changed

- **Breaking** (the feed contract and settings, (a) and (c) of the
  [compatibility policy](https://github.com/cristiangirlea/people-index/blob/v0.2.0/docs/INTEGRATING.md#compatibility-policy)):
  a delivery can no longer be named `latest`. `PI_DELIVERY=latest` used to read `raw/latest/`; it
  now plans the newest complete delivery, and a directory named `latest` fails such a run.
  Remedy: rename the delivery and set `PI_DELIVERY` to the new name.
- Pin the production example release image by digest and check rendered Job images.
- The in-cluster SeaweedFS requests 1.5 GiB and is limited to 2 GiB of memory (was 512 MiB / 1 GiB
  in the base, 256 / 512 MiB in the local overlay) after it was OOM-killed on k3d. Docker needs at
  least 14 GiB of memory for the local overlay (was 10).
- Elasticsearch 9.5.6 (compose, the ECK spec and the snapshot bootstrap Job). The Elasticsearch
  rules extraction predicts (numbers, number length, repeated keys, keyword limits, analyzer
  tokens, the date table, the mapping read-back and the built-in templates) were re-verified on
  9.5.6 and are unchanged from 9.5.5.
- SeaweedFS 4.54 (compose and Kubernetes), aws-cli 2.40.4 and Prometheus v3.16.0 in compose.
  SeaweedFS 4.52 reworked how conditional writes are checked; the conformance suite with its
  create-if-absent and update-if-match races, and the lease test, pass against 4.53 and 4.54.
- Refresh the pinned Rust build image and the distroless runtime image; the runtime image no
  longer carries the zlib vulnerabilities CVE-2026-85091 (high) and CVE-2026-27171 (medium)
  found in 0.1.0's.

### Fixed

- Remove the CLI's direct dependency on the unmaintained `humantime` (RUSTSEC-2026-0542).
  Duration settings accept the same forms.
- Identifier strings longer than 1000 characters and ids above Elasticsearch's numeric range
  are counted once in `values_ignored_by_index`; verify never samples them.
- Retry transient S3 failures through brief store outages with the configurable
  `PI_STORE_RETRY_TIMEOUT` window.
- `values_ignored_by_index` counts a string longer than 1000 characters in a `long` or `integer`
  field: Elasticsearch skips it whatever it holds (observed on 9.5.5 and 9.5.6); one that held a
  readable number used to go uncounted.

## [0.1.0] - 2026-10-29

### Added

- A staged Rust pipeline that joins gzipped person and organization feeds and publishes the
  documents to Elasticsearch behind an alias, switched atomically per run.
- Local and distributed execution coordinated only through an S3-compatible store
  (create-if-absent commits and leases), plus synthetic data generation and verification.
- Configurable feed schema and index mapping files (`PI_SCHEMA_FILE`, `PI_MAPPING_FILE`),
  checked against each other at startup and pinned in each run's plan.
- Named deliveries (`PI_DELIVERY` reads `raw/<name>/`; `PI_REQUIRE_COMPLETE_MARKER=true` refuses
  to plan one without its `_COMPLETE` marker) and a `fields_not_indexed` count of unknown
  top-level record fields.
- Counts of values Elasticsearch ignores (lenient measures, over-long keywords, malformed
  built-in dates) as `values_ignored_by_index`, a lower bound whose gaps are listed in
  `crates/pi-es/src/mapping.md`.
- Publish guards: feed bounds against the serving index, run-index ownership, no recreation of
  a missing run index, newer-run ordering, and a read-back of the index's effective settings and
  mapping before loading.
- Mapping checks that keep one bad record from failing a load (keyword `ignore_above`, text
  analyzers) and refusal of aliases that would match Elasticsearch's built-in index templates.
- An alias run lease for `run` and `stage`, and an optional in-cluster schedule (CronJob).
- Retention of run segments (`PI_RETAIN_RUNS`) with run history kept.
- Read-only alias access for applications (`people-index-reader` role, API keys).
- Snapshots in a separate bucket with scoped identities, and a rollback and restore runbook
  with a drill in the compose end-to-end run.
- Prometheus alerts for Elasticsearch health (exporter, cluster status, disk, heap, bulk
  rejections, snapshot age) and for failed, stalled and missing pipeline runs.
- Docker Compose and Kubernetes (Kustomize, ECK) deployments on a Debian 13 distroless image.
- A release workflow: linux/amd64 and linux/arm64 images, Trivy scan, SBOM and provenance,
  cosign keyless signing.
- An integration guide with a compatibility policy, a contributing guide and the MIT license.
