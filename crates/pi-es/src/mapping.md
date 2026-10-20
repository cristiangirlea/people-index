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
their own that no query needs. `enabled: false` skips mapping their contents entirely, so their
shape (or a malformed value) cannot fail a bulk item, and they cost nothing but `_source`.
The JSON parser still reads their tokens, and its own rules apply before any mapping, so two
things fail the item in any field, disabled, `text` or `ignore_malformed` alike (both observed
on 9.5.5):

- a number token longer than 1000 characters: extraction quarantines such a record as `record`,
  naming the path (`pi_model`'s `MAX_NUMBER_CHARS`);
- a key repeated in one object ("Duplicate field"), at any depth: extraction rewrites such a
  record to each key's last occurrence and counts it in `records_with_repeated_keys`.

docs/ARCHITECTURE.md, "The data", has both rules.

## Robust scalar types

A value Elasticsearch cannot read as its mapped type fails the bulk item with a mapper error,
and a failed item fails the load task and with it the run. Two layers keep one bad record from
doing that:

- Extraction checks the JSON kind of every field this mapping parses (`pi_model::PERSON_FIELDS`,
  `ROLE_FIELDS`, `ORGANIZATION_FIELDS`; docs/ARCHITECTURE.md, "The data") and quarantines a
  record with the wrong kind (`"abc"` in an `id`, a string where `roles` expects objects, an
  object in any field but an `object` one) as `record`, naming the field. `text`, `keyword` and
  `date` fields take any scalar or an array of scalars: Elasticsearch reads a number or a
  boolean as text. The test `extract_shapes_agree_with_the_mapping` in `mapping.rs` fails when a
  field is added here without a table entry, or a type changes under one.
- Numbers and dates, the top-level `id` included, carry `ignore_malformed: true` (checked by
  `numbers_and_dates_ignore_malformed_values`), and so does `open_to_work`. Elasticsearch then
  skips a scalar that does not parse (a number past the range of `long`, a date string in
  another format, `"n/a"` in a `long`, `"yes"` in a `boolean`) and keeps the document: the
  malformed values are not indexed and the field is listed in the document's `_ignored`; the
  record stays searchable by its other fields, and the value stays in `_source`. An object
  still fails the item, `ignore_malformed` or not.

Identifiers (`id`, `roles.id`, `roles.organization_id`, `organizations.id`) are strict at
extract time (`JsonKind::Number`): a value that is not a number, or a string holding one, is
quarantined, and `ignore_malformed` stays on them as a second line. Measures (`connections`,
`followers`, `open_to_work`, `organizations.employee_count`, `organizations.founded_year`) are
lenient (`JsonKind::LenientLong`, `LenientInteger` for `founded_year`, `LenientBoolean`): any
scalar or array of scalars passes, and only an object is quarantined. `values_ignored_by_index`
counts the values Elasticsearch skips (lists in `_ignored`), following the rules observed on
9.5.5 and pinned in `pi-model`'s tests (`pi_model` `es_number`): in either number type a
boolean; in a `long` a string other than `""` that is not an exact decimal (no surrounding
whitespace) or whose integer part is outside `i64`, or whose exponent is outside `int` or
scale (fraction digits as written minus the exponent) outside `int` or below -19, whatever its
value (`BigDecimal`'s limits: `"0e20"` is skipped, `"0e19"` is not), a JSON integer
outside `i64`, or a JSON float beyond `±2^63` compared as a double (so values that round to
2^63 pass); in an `integer` a value Java's `Double.parseDouble` rejects (it trims, so a string
of spaces only is rejected, and takes `NaN`, `Infinity`, `d`/`f` suffixes and hexadecimal;
those forms were observed, the rest of the grammar is derived from Java's) or one outside
`i32` before truncation; in a `boolean` anything but `true`, `false`, `"true"`, `"false"` and `""`. One known divergence: a
non-ASCII decimal digit in a `long` string (`"٥"`) is counted, though Elasticsearch indexes it.
The counter counts per extracted line, before person deduplication, and an organization's
value once however many person documents embed it. A lenient field must be mapped as its own
type with `ignore_malformed: true`; the agreement test fails otherwise. `roles.start_date` and `end_date` accept full dates,
`yyyy-MM` and `yyyy` (a JSON number such as `2020` is read as text, so as a year), so range
queries work on partial dates.

## `roles` and `organizations` are `object`, not `nested`

A `nested` field indexes every array element as a hidden Lucene document. With dozens of roles
per person that multiplies the document count of the index, its size and its load time, and
every query on those fields has to be a `nested` query. The acceptance check and the queries
this index serves ask "does this person have at least one role with title X" and "is this
person linked to organization Y": per-field questions that a flattened `object` answers exactly.
The one thing `object` cannot answer is a correlated question about a single role ("title X
*at* organization Y in the same role"). If that becomes a requirement, the fix is a `nested`
copy of only the fields it needs (`roles.title`, `roles.organization_id`), not of whole roles.
