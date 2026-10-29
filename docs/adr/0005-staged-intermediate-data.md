# ADR 0005: Persisted intermediate data between stages

Status: accepted, 2026-10-15

## Context

Between extract, join, assemble and load, rows could flow through memory and the network (a
streaming dataflow with an in-memory shuffle), or each stage could write its outputs to durable
storage for the next one to read. With streaming, a failure anywhere restarts the whole
pipeline or needs checkpointing; every stage has to be scaled together; and Elasticsearch has to
be up from the first record.

When the data is persisted, its format matters: it is written and read once per stage, and most
of each row is an opaque JSON body.

## Decision

Every stage writes its outputs to the object store and the next stage starts only when the
previous one has committed every task (a barrier). Outputs are **segment files**:

- the 8-byte magic `PISEG001`, then one zstd stream (level 3, frame checksums) of records, each
  a LEB128 varint length followed by the `postcard` encoding of one row;
- rolled at `segment_target_bytes` (64 MiB compressed by default, estimated so a segment ends at
  most about one row past it);
- named `<task>-<attempt>-<n>.seg` under `data/<dataset>/<p|q>=<n>/`, and listed in the task's
  commit ([ADR 0003](0003-object-store-commits-and-leases.md)).

**postcard** because the rows are Rust structs shared across crates (`OrgRow`, `PersonRow`,
`EdgeRow`, `JoinedRow`, `DocRow`) with a few integers and a byte body: serde derives the
encoding, there is no schema file to keep in sync, and the encoding is compact. **zstd**
because JSON compresses well at a fast level, and frame checksums turn storage damage into an
error instead of wrong rows. Columnar formats (Parquet, Arrow) were not chosen: rows are read
whole, the bulk of each is an opaque JSON blob, and column statistics would buy nothing.
Re-encoding as JSON lines was not chosen: larger, and every reader would parse again.

## Consequences

- **Retries are per task.** A failed task reruns from its committed inputs; nothing upstream
  repeats. A crashed coordinator resumes from the commits.
- **Replays.** `people-index stage <name> --run-id R --task T` reruns one task against real
  inputs, which makes a stage debuggable in isolation.
- **Independent scaling.** Each stage's parallelism is its own task count (input files for
  extract, `P` for the rest); workers can join or leave between and during stages. The
  CPU-heavy stages run while Elasticsearch is idle; only `load` puts pressure on it.
- **Extra I/O and storage.** The data is written several times (persons, edges, joined, docs)
  and read back; publish removes a run's segments once `PI_RETAIN_RUNS` newer published
  runs exist (#63).
- **Barriers cost latency.** A slow task holds back the next stage; the remedy is more, smaller
  partitions.
- **The format is versioned by its magic.** Readers reject other magics, truncation, bad
  checksums and oversized lengths; a change to a row type is a new magic, not a silent
  reinterpretation.
