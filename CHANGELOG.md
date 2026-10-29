# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows the compatibility policy in [docs/INTEGRATING.md](docs/INTEGRATING.md#compatibility-policy).

## [Unreleased]

### Changed

- Pin the production example release image by digest and check rendered Job images.

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
