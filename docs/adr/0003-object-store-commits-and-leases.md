# ADR 0003: Commits and leases as conditional writes in the object store

Status: accepted, 2026-10-15

## Context

A distributed run needs four things: each task's outputs become visible exactly once, even when
a task runs twice; idle workers find work; a dead worker's task is picked up by someone else;
and a restarted coordinator resumes where the last one stopped. The usual tools are a
coordination database (PostgreSQL, etcd, Redis), a message queue, or Kubernetes Indexed Jobs.

The run already depends on an object store for its inputs and every stage's outputs.

## Decision

Keep all coordination state in the same object store, written only with conditional puts:

- **Commits.** A task writes its outputs under a fresh attempt id, then the executor creates
  `_commit/<task>.json` with create-if-absent (`If-None-Match: *`). The commit lists the exact
  output keys. Readers list commits, never data prefixes, so a crashed or losing attempt's files
  are invisible; the first commit wins and `publish` deletes the orphans.
- **Leases.** A worker claims a task by creating `_lease/<task>.json`, renews it every
  `ttl / 3` with update-if-match (`If-Match: <etag>`), and takes over a lease past
  `expires_at + clock_skew` (or released) with update-if-match on the etag it read. A holder
  whose lease is taken, or whose renewals fail for a whole ttl, has its task cancelled.
- **Run state.** `state.json` records the current and completed stages, `stages/<stage>.json`
  the task list and who runs it, and `failed.json` (create-if-absent) stops every process when
  any task fails for good.

Leases only prevent wasted work. Correctness rests on the commit: a worker that keeps running
after losing its lease can finish and try to commit, and loses harmlessly. Loading into
Elasticsearch is idempotent because the document `_id` is the person id.

**Why no coordinator database.** The state is small (one object per task, one renewal per
running task every 20 s by default) and belongs next to the data it describes: resuming a run
is reading its prefix. A database would be one more stateful service to deploy, back up, secure
and keep consistent with the store, and a commit in it would still have to agree with objects
written elsewhere.

**The store must refuse conditional writes that do not hold**: HTTP 412 for
`If-None-Match: *` on an existing key and for a stale or missing `If-Match` key, with exactly one
winner under concurrency. AWS S3 documents both conditions. Other S3-compatible servers differ
and must be checked; the compose stack's `s3-init` probes every start and stops the stack if
the store fails.

**Store choice: SeaweedFS 4.48** for compose and the local k3d cluster. Probed with the
deployment's own flags: 412 in every case above, and in 16-way races of create-if-absent and of
update-if-match on one key, exactly one 200 in each of three rounds; anonymous or wrongly signed
requests get 403. The lease tests and the store conformance suite pass against it. MinIO is not
used because its project stopped publishing community container images, so there was no
maintained image to pin; it was not tested. The local filesystem backend supports
create-if-absent (an atomic hard link) but not update-if-match, so it serves only the local
executor, and the distributed executor refuses it.

## Consequences

- One fewer service to operate; the store is the single source of truth for a run.
- Deployments are tied to stores with correct conditional writes; a non-conforming server would
  break leases and commits silently, hence the probe.
- Lease expiry uses wall-clock time with a skew allowance (5 s). Large clock drift causes early
  takeovers: repeated work, never a wrong result.
- Work discovery is by polling (`PI_POLL_INTERVAL`, 2 s), and every stage lists its commits:
  latency of seconds per stage and list requests that grow with the task count, both small
  next to task run times.
- Orphaned files accumulate until `publish` removes them; a run that never publishes keeps them.
- Only one coordinator per run is assumed (its Job or compose service ensures it); there is no
  coordinator lease.
