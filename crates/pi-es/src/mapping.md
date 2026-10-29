# The index mapping

`mapping.json` is the explicit mapping of the output document (docs/ARCHITECTURE.md, "The output
document"). JSON has no comments, so the reasons live here. `crate::mapping` embeds the file at
compile time and its tests check the choices below, so this note and the file cannot drift apart
silently. A feed with other field names brings its own copy (`PI_MAPPING_FILE`,
docs/OPERATIONS.md "Feed schema"); `check_mapping` holds either against the feed schema at
startup.

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

## Text analyzer limits

`check_mapping` allows only `standard`, `simple`, `whitespace`, and `english` when
`analyzer`, `search_analyzer`, or `search_quote_analyzer` is specified on a `text` field
or text multi-field, including on an unindexed parent. Omitting them uses the default. Other
names (including custom analyzers and `keyword`) are refused because a single immense term
can fail indexing or queries. Fields inside disabled objects are not parsed and are exempt.

On a single-node Elasticsearch 9.5.5 container, `_analyze` on 40,000 ASCII `a` characters
produced 157 tokens of at most 255 bytes for `standard`, `simple`, `whitespace`, and
`english`; indexing each succeeded. Further probes found that these analyzers cap tokens at
255 characters for ASCII, CJK, U+023A and four-byte input. Multibyte tokens reached about
765 bytes (CJK and U+023A), far below the 32,766-byte term limit. `french` also indexed
successfully (157 one-byte tokens for the ASCII input), but is not on the deliberately small
allow-list. `keyword` and `pattern` (`\W+`) failed indexing with HTTP 400; `fingerprint`
emitted nothing for a 40,000-character word (safe for that input, but still refused). A
`search_analyzer` of `keyword` did not fail indexing, but a match query with a 40,000-character
value failed with HTTP 400. The startup check validates names; prepare-index also reads the effective index settings and
mapping, refusing analysis definitions or mapping changes introduced by templates.

A 9.5.5 single-node probe created an index with the built-in mapping and run metadata.
GET `_mapping` returned `dynamic` as the string `"false"`, omitted `type: object` on
objects with properties, and returned scalar `copy_to` as a one-element array; the rest
matched. A matching index template defining a default keyword-tokenizer analyzer and an
extra keyword field put both in the created index: GET `_settings` exposed the analyzer
and GET `_mapping` exposed the field. The read-back check normalizes those observed mapping
forms and compares the entire mapping including run metadata.

A 9.5.5 single-node probe with a basic licence applied templates to `test-1` with this
mapping. Index-level `index.mapping.ignore_above: 10` appeared in GET `_settings`, not
GET `_mapping`; a longer `profile_url` remained in `_source` and was absent from `_ignored`.
With `index.mapping.source.mode: synthetic`, creation and indexing succeeded, but GET
`_settings` reported `STORED` and the URL remained in `_source`: basic did not activate
synthetic source. With `disabled`, creation and indexing succeeded, GET `_settings` reported
`disabled`, and GET the document returned no `_source`. Neither source mode appeared in GET
`_mapping`. The effective settings check refuses the index-level limit and source modes other
than stored, including when a template supplies them.

## Keyword for codes and enumerations

`country`, `region`, `city`, `industry`, `skills`, `employment_type`, `organization_domain`,
`size_range`, `keywords`, `technologies`: exact values used in filters and facets, never
analysed.

Every keyword with an index or doc values, including multi-fields, has `ignore_above`:
8191 for codes without a smaller limit, 512 or 1024 for the others. Elasticsearch measures
Java string length in UTF-16 code units (an emoji is two). A unit takes at most three UTF-8
bytes, so 8191 units take at most 24,573 bytes, below Lucene's 32,766-byte term limit;
the safe ceiling is 10,922 units. On Elasticsearch 9.5.5, 4,095 emoji were indexed and
found by a term query; 5,000 emoji were ignored and listed in `_ignored`; 8191 three-byte
BMP characters were indexed. A further probe indexed a document containing 8192 emoji in
`country`, `first_name`, `roles.location`, `organizations.domain` and `skills`; the original
`_source` remained and `_ignored` listed each affected keyword path.

`values_ignored_by_index` counts each input value over the smallest applicable keyword limit,
once even if multiple fields ignore it, and counts each array element separately. It counts
before deduplication and assembly, so it does not equal the number of `_ignored` entries.

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
  naming the path (`pi_model`'s `MAX_NUMBER_CHARS`), unless the number is in an occurrence of a
  repeated key that the rewrite below drops;
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
extract time (`JsonKind::Number`): a value that is not a number, or a string of decimal
digits within `u64` (`pi_model::parse_decimal_id`, so `"007"` passes), is quarantined, and `ignore_malformed` stays on them as a second line. Measures (`connections`,
`followers`, `open_to_work`, `organizations.employee_count`, `organizations.founded_year`) are
lenient (`JsonKind::LenientLong`, `LenientInteger` for `founded_year`, `LenientBoolean`): any
scalar or array of scalars passes, and only an object is quarantined. `values_ignored_by_index`
counts the values Elasticsearch skips (listed in `_ignored`), following the rules observed on
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
value once however many person documents embed it. Every number and date field must be mapped
with `ignore_malformed: true` (and a lenient one as its own type), a multi-field under `fields`
included: `check_mapping` refuses the mapping at startup otherwise. `roles.start_date` and `end_date` accept full dates,
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
