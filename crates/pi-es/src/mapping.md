# The index mapping

`mapping.json` is the explicit mapping of the output document (docs/ARCHITECTURE.md, "The output
document"). JSON has no comments, so the reasons live here. `crate::mapping` embeds the file at
compile time and its tests check the choices below, so this note and the file cannot drift apart
silently.

## `dynamic: false`

Records carry unknown fields through untouched. With dynamic mapping every new key in the feed
would become a mapped field: an unbounded mapping, a cluster-state update in the middle of the
load and a type guessed from whichever document came first. With `dynamic: false` unknown fields
stay in `_source` (returned, digested, never indexed) and the mapping is exactly this file.
Making a new field searchable is a mapping change and a new run, which is how it should be.

## Text plus keyword where queries need both

- `first_name`, `last_name`: `text` for matching, `.keyword` for exact match, sorting and
  aggregations; both are copied into `full_name` (`text`, not stored in `_source`) so a name
  query is one field.
- `roles.title`, `roles.organization_name`, `organizations.name`: `text` for search,
  `.keyword` for the exact term queries of the acceptance check (`roles.title.keyword`,
  `organizations.name.keyword`) and for facets. `ignore_above: 1024` keeps an absurdly long
  value from failing anything: it is simply not indexed as a keyword. The data generator must
  pick expected titles and names shorter than that, or those counts cannot match.

## Keyword for codes and enumerations

`country`, `region`, `city`, `industry`, `skills`, `employment_type`, `organization_domain`,
`size_range`, `keywords`, `technologies`: exact values used in filters and facets, never
analysed.

## Not indexed: large or display-only fields

- `summary`, `roles.description`, `organizations.description`: `text` with `index: false`.
  They are the bulk of a document's bytes; indexing them would multiply the index size and the
  load time for queries nobody runs. They stay in `_source` for display. If free-text search on
  them is wanted later, flip `index` and reload.
- `profile_url`, `roles.id`: `index: false` and `doc_values: false`, kept for display only.

## `enabled: false` objects

`memberships`, `educations`, `certifications`, `languages`, `organizations.addresses`,
`organizations.locations`, `organizations.funding_rounds`: nested structures with fields of
their own that no query needs. `enabled: false` skips parsing their contents entirely, so their
shape (or a malformed value) can never fail a bulk item, and they cost nothing but `_source`.

## Robust scalar types

Numbers and dates carry `ignore_malformed: true`: the record is raw JSON from the feed, and a
`"n/a"` in `followers` must not turn into a permanent bulk failure for the whole run. The
malformed value stays in `_source`; only that field is not indexed for that document.
`roles.start_date` and `end_date` accept full dates, `yyyy-MM` and `yyyy`, so range queries
work on partial dates.

## `roles` and `organizations` are `object`, not `nested`

A `nested` field indexes every array element as a hidden Lucene document. With dozens of roles
per person that multiplies the document count of the index, its size and its load time, and
every query on those fields has to be a `nested` query. The acceptance check and the queries
this index serves ask "does this person have at least one role with title X" and "is this
person linked to organization Y": per-field questions that a flattened `object` answers exactly.
The one thing `object` cannot answer is a correlated question about a single role ("title X
*at* organization Y in the same role"). If that becomes a requirement, the fix is a `nested`
copy of only the fields it needs (`roles.title`, `roles.organization_id`), not of whole roles.
