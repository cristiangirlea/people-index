# ADR 0002: A partitioned hash join instead of a shared lookup store

Status: accepted, 2026-10-15

## Context

Every person has zero or more roles, each pointing at an organization by id. A document needs
the full organization record for each distinct id, so the pipeline must join two feeds whose
sizes grow independently. Duplicate ids in either feed resolve to the last record in feed
order, whatever the number of workers.

Three ways to do the join:

1. **A node-local table.** Load every organization into an in-memory map on each worker, then
   stream the persons past it. Simple and fast on one machine, but every worker needs the whole
   table: memory per node grows with the organization feed, adding nodes adds no capacity for
   it, and each new worker repeats the full load before doing useful work. It cannot scale out;
   it can only scale up until the table no longer fits.
2. **A shared lookup store.** Put the organizations in a key-value service (Redis, a database,
   or an Elasticsearch index) and look ids up while building documents. Memory per worker is
   small, but every reference becomes a network lookup (batched at best), the store's
   throughput caps the whole pipeline, and it is another stateful service to size, secure,
   make highly available, and clear between runs. Last-wins also has to be enforced at write
   time in that store, across workers writing concurrently.
3. **A partitioned hash join.** Hash both sides on the join key so each task needs only one
   partition of the organization table.

## Decision

Use a partitioned hash join over the object store, in two halves:

- `extract-persons` writes, for every person, one `EdgeRow {org_id, person_id, seq}` per
  distinct non-null organization id, partitioned by `p = xxh3_64(org_id) % P`, and the person
  rows by `q = xxh3_64(person_id) % P`. `extract-orgs` partitions organizations by `p`.
- `join` (one task per `p`) loads one organization partition into a last-wins map and streams
  that partition's edges past it, emitting `JoinedRow`s (with the organization body, or `None`
  when unresolved) re-keyed by `q`.
- `assemble` (one task per `q`) groups the joined rows by person, keeps only the rows of each
  person's winning record, and builds the documents.

**Partition sizing.** `plan` sets `P = next_power_of_two(max(input_bytes /
partition_target_bytes, min_partitions))` with defaults 256 MiB and 16. One `P` serves both
keys. A power of two makes the modulo a mask and means doubling `P` splits every partition in
two (a property test checks this). The hash function and its seed are part of the contract,
pinned by a known test value, because two components that disagree on it lose rows silently.

## Consequences

- **Memory per task is bounded by the partition, not the input.** More data gives more
  partitions and more tasks; a worker's size does not change.
- **The shuffle has a cost.** Person records are written once more (`persons/`), each
  reference is written twice (`edges/`, then `joined/`), and `joined/` carries a copy of the
  organization body per reference. With Zipf-like popularity a few organizations are
  referenced very often, so `joined/` can be much larger than the organization feed. This is
  the price of never holding the whole table; a per-partition dictionary of organization
  bodies could reduce it later, and it has not been measured yet.
- **Skew.** Hashing spreads ids evenly, but one very popular organization lands in a single
  `p`. The join holds organizations and streams edges, so that costs streaming time in one
  task, not memory. On the person side, a person's joined rows are bounded by their roles.
- **The partition target counts gzipped bytes**, so a decoded partition is several times the
  target. `join` and `assemble` do not rely on it for memory: a partition estimated larger
  than `PI_TASK_MEMORY_BYTES` runs in several passes (a grace hash join keyed by a second hash
  of the id), each holding one share of it, at the cost of re-reading the partition per pass.
- **More stages and more intermediate data** than a single-node join, which
  [ADR 0005](0005-staged-intermediate-data.md) accepts for retries and scaling.
