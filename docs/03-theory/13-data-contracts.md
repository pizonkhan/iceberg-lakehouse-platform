# Data contracts, and where schema-on-read breaks down

A data contract, precisely defined, is a machine-checked agreement on a model's column names,
types, and nullability that fails the build if violated. That definition is doing real work:
it excludes a lot of things that get called "contracts" informally but are not. A naming
convention documented in a markdown file is not a contract, because nothing stops a column from
drifting away from it. A comment above a SQL query describing what a column is supposed to mean
is not a contract, because nothing reads the comment and enforces it. A test that runs after a
model has already built and checks its output is closer, but it is still not what dbt itself
means by the word `contract`, and the difference between those two things is the subject of this
document, including an honest look at which one this project actually has.

## What a contract guarantees, mechanically

dbt's contract mechanism (`contract: {enforced: true}` in a model's config, paired with
declared `data_type` and, optionally, `constraints` on each column in that model's schema file)
changes what dbt does before it ever runs the model's query. With contract enforcement on, dbt
generates a `CREATE TABLE` statement with an explicit column list and explicit types from the
declared schema, and it compares that declared schema against what the model's compiled SQL
would actually produce, before submission to the warehouse. If the model's SELECT list does not
match the declared columns, in name, type, or nullability, the build fails at that comparison,
before a single row is materialized and before any downstream query ever runs against a
mismatched table.

This is a build-time, schema-shape guarantee, not a data-quality guarantee. A contract does not
know or care whether a value is sensible, whether a foreign key resolves, or whether a business
rule holds. What it guarantees is narrower and different in kind: that the physical shape of the
table, its column names, their types, and which columns are nullable, cannot silently drift out
from under whatever consumed the model's earlier, contracted shape. If someone renames a column,
drops one, or changes an `integer` to a `varchar` in a model's SQL without updating the
declared contract to match, the build stops there. The failure happens at compile time, before
`CREATE TABLE` or `INSERT` ever executes, which means it happens before the change has any
chance to reach a consumer, incrementally or otherwise.

## What a contract cannot catch

A contract's scope stops exactly at the schema boundary it enforces. It says nothing about the
values inside a correctly-shaped column. A `not_null` constraint declared as part of a contract
is enforced (dbt compiles it into the `CREATE TABLE`'s column constraints where the adapter
supports it), but a `unique` test, an `accepted_values` check, a referential integrity check
against another table, a check that a number falls inside a sane range: none of that is part of
what a contract mechanically verifies. Those remain the job of `dbt test`, which runs after the
model has already built, against data that already exists in the table. A contract and a test
suite are complementary, not substitutes for each other: the contract protects the shape, the
tests protect the content, and a project can have either without the other.

## What this project actually has, checked directly

This is the honest finding this document exists to state plainly, not gloss over, in keeping
with this project's own stated principle that evidence beats assertion. A direct search of
every dbt model and schema file under `transform/lakehouse/models/` for dbt's literal contract
enforcement key found zero matches:

```
grep -rln "contract:" transform/lakehouse/models/
```

returns exactly one file, `dim_title.sql`, and that file's only match is a comment referencing
"Full column-level contract: .notes/modeling.md" as prose, not a `contract:` config block. A
further check for `enforced` anywhere under `models/` (excluding installed packages and build
artifacts) finds no `contract: {enforced: true}` block anywhere in the project. `dbt_project.yml`
sets no project-wide contract default either. The word "contract" appears constantly across this
project's model files, schema YAML, and code comments (`dim_device.sql`, `_dim_device.yml`,
`fct_playback_events.sql`, `_fct_playback_events.yml`, `fct_billing_transactions.sql`, and
`.notes/modeling.md` itself, which is the actual source of every model's documented column
contract), but every one of those usages is the informal, documented sense of the word: a
written specification of what a model's columns are supposed to be, kept in a markdown file and
in dbt schema YAML descriptions, checked against by a human reviewer and by ordinary `dbt test`
assertions like `not_null` and `unique`. None of it is dbt's `contract: {enforced: true}`
mechanism. This project documents its schema and tests its data; it does not use dbt's literal
contract enforcement anywhere.

### What that gap concretely costs

The honest way to state the difference is with a specific scenario, not an abstraction. Suppose
a future change to `dim_plan.sql` accidentally casts `price_usd` from `decimal` to `varchar`,
say by an unintended string concatenation upstream in the model's CTEs. With this project's
current, test-based approach:

1. `dbt run` compiles and executes the model's SQL. Nothing about the type change stops
   compilation, because there is no declared contract to compare against; dbt has no
   expectation of what type `price_usd` is supposed to be.
2. The `CREATE TABLE` (or `MERGE`) actually runs against Trino and materializes the table with
   `price_usd` now typed `varchar`.
3. Only then does `dbt test` run, and only if some test on `price_usd` happens to exist and
   happens to be sensitive to this specific failure mode (a `dbt_utils.expect_column_values_to_be_of_type`
   check would catch it; a plain `not_null` test would not, since a stringified price is
   still non-null).
4. If no such test exists, or if this model is not part of the currently-selected `dbt test`
   scope, the type change ships all the way to a materialized table with no error at any stage.
   Anything downstream that expects a numeric `price_usd`, a BI tool doing arithmetic on it, a
   fact table joining and aggregating it, a Python job casting it, fails or silently
   misbehaves at the point of use, potentially far removed in time and in the dependency graph
   from where the type actually changed.

With `contract: {enforced: true}` declared and `price_usd: decimal(...)` specified in the
model's schema file, step 1 would instead fail immediately: dbt compares the compiled column
list and types against the declared contract before ever submitting a `CREATE TABLE` to Trino,
and a mismatch stops the build there. The type change never reaches a materialized table at all,
let alone a consumer. This is the precise, mechanical difference between the two approaches: a
contract fails before the query runs; this project's current test-based approach can only fail
after the model has already built and only if a test happens to be written that is sensitive to
that specific kind of violation. The gap is not that this project has no schema discipline,
`.notes/modeling.md` documents a real column-level contract for every model, and every dimension
and fact carries real `not_null`/`unique`/`accepted_values` tests checked against actual data
(this project's own 356/356 passing `make test` run, `.notes/failures.md`, is real evidence that
test coverage is neither theoretical nor absent). The gap is specifically that none of that
enforcement happens at the compile-time boundary dbt's contract mechanism is built for, so
schema-shape violations that ordinary data tests do not happen to cover can reach a materialized
table before anything catches them, if they are caught at all.

## Where schema-on-read breaks down

A contract, even a well-enforced one, operates at the level dbt controls: the model's own
declared output. Underneath that, Iceberg's actual storage model has a second, more subtle
schema boundary worth separating out explicitly, because it is easy to describe imprecisely.

An Iceberg table, at the catalog level, is schema-on-write: it has one well-defined, versioned
schema at any point in its history, tracked in `metadata.json`, and every column carries a
permanent, never-reused field id, not just a name. Changing that schema (adding a column,
widening a type, renaming a field) is itself a tracked, atomic metadata operation, not something
that happens implicitly by writing a file with a different shape. This project's own schema
evolution evidence demonstrates the mechanism directly: adding `discount_pct integer` to
`demo_billing_events` via `ALTER TABLE` assigned it field id 6, one past the table's previous
highest field id, "not reused from any dropped or renamed field"
(`docs/evidence/schema-evolution/01-add-column-notes.txt`). The table's schema, at the catalog
level, is exactly as well-defined and evolving-with-history as any traditional schema-on-write
system.

But underneath that catalog-level schema, the actual Parquet data files backing an Iceberg table
are schema-on-read from the query engine's perspective, once a table's schema has evolved past
what an older file physically contains. A Parquet file written before a column existed simply
does not have that column in its own physical schema. When a query against the table's current
schema asks for that column, the engine has to reconcile the file's older physical schema
against the table's current logical schema, at read time, per file. Iceberg's mechanism for
doing this correctly is exactly the field-id resolution this project's own evidence demonstrates:
each column carries a permanent field id independent of its name or position, and a reader
resolves a requested column by matching field ids against whichever schema version a given data
file was written under. If a file's own schema (whatever field ids it actually wrote) is missing
a field id the table's current schema requests, the correct, specified behavior is to return null
for that field on every row in that file, not to error, not to guess, and not to silently
misalign a different column into that field's position.

### This project's own real example

This project has a real, naturally-occurring instance of exactly this pathology, not a
constructed one: `bronze_playback_sessions.playback_quality`. The source system that produces
this project's raw playback data began emitting the `playback_quality` field only partway
through its own operational history; earlier ingestion batches simply have no such column in
their source files at all, "a different Parquet schema, not just nulls"
(`.notes/decisions.md`, "mid-stream schema drift"). Silver's staging layer documents the correct
reading of this directly:

```yaml
- name: playback_quality
  description: >
    streaming quality tier delivered for the session (sd, hd, fhd, uhd). null on rows ingested
    before the source system started emitting this field; not an error, a genuine mid-stream
    schema addition.
```

(`transform/lakehouse/models/staging/sources.yml`). The correct behavior here, `playback_quality`
reading back as null for every row from a pre-drift file and reading back its real value for
every row from a post-drift file, is exactly what this project's schema evolution scenario 1
demonstrates formally, in isolation, against a purpose-built table: after `ALTER TABLE ... ADD
COLUMN discount_pct integer` followed by a fresh insert, querying old rows by the new column
name reads back null,

```
{"event_id":"evt_000001","amount":17,"status":"pending","discount_pct":null}
{"event_id":"evt_000500","amount":90,"status":"refunded","discount_pct":null}
{"event_id":"evt_001000","amount":80,"status":"refunded","discount_pct":null}
```

while new rows written after the column existed read back populated,

```
{"event_id":"evt_001001","amount":87,"status":"completed","discount_pct":1}
{"event_id":"evt_001050","amount":70,"status":"completed","discount_pct":10}
{"event_id":"evt_001100","amount":60,"status":"completed","discount_pct":0}
```

(`docs/evidence/schema-evolution/01-add-column-old-rows-null.json` and
`01-add-column-new-rows-populated.json`). The evidence directory's own notes state the mechanism
in exactly the terms that matter: "readers resolve columns by field id against whichever schema
version a data file was written under, and any field id absent from a file's schema is treated
as null, no rewrite required." Crucially, the underlying Parquet files are never touched to
backfill the missing column: `01-add-column-notes.txt` confirms the two original data files are
"byte-for-byte untouched: same file_path, same file_size_in_bytes, same record_count in both
before and after." The reconciliation happens entirely at read time, per file, which is the
schema-on-read behavior this section is naming.

### Why this is a real risk, not just a footnote

The reason this is worth stating as a place schema-on-read "breaks down," rather than simply a
feature working as intended, is what would happen if the field-id resolution were done wrong, or
bypassed. A naive schema-on-read implementation, one that resolved columns by name-and-position
against whatever a given file's raw schema happens to contain, rather than by Iceberg's stable
field id against the table's declared logical schema, would have no principled way to know that
a pre-drift file's absence of `playback_quality` means "this field did not exist yet, return
null" rather than, say, silently reading a neighboring physical column's bytes into the
`playback_quality` position, or dropping the row, or raising an unhandled error partway through
a scan. The reason this project's real data does not exhibit that failure is that Trino's
Iceberg connector correctly implements Iceberg's specified field-id-based default-value
resolution, not because the underlying problem does not exist. The contract enforcement
discussed earlier in this document operates one full layer above this: it can only ever
guarantee that a dbt model's own compiled output matches its declared schema. It says nothing
about whether the engine reading the Parquet files beneath that model correctly reconciled an
older file's physical schema against the table's current logical one before that data ever
reached the model's query in the first place. That reconciliation is Iceberg's and the engine's
responsibility, sitting underneath and prior to anything dbt's contract mechanism can see or
enforce.

## How to verify this is actually working

Confirm no model in this project currently declares a dbt contract:

```
grep -rn "contract:" transform/lakehouse/models/ | grep -v "\.sql:"
```

Reproduce the schema drift resolution directly against Iceberg metadata, not just Trino's query
result, to see the field-id mechanism itself rather than only its effect:

```
mc cat local/warehouse/schema_evolution_demo/demo_billing_events_*/metadata/<version>.metadata.json \
  | jq '.schemas, ."current-schema-id", ."last-column-id"'
```

which should show two schema entries (schema-id 0 and schema-id 1 in this project's own captured
evidence), `current-schema-id: 1`, and `last-column-id` advanced from 5 to 6, with the new
field's id one past the table's previous highest, confirming it was assigned fresh rather than
reused. Then query old and new rows by the new column name through Trino and confirm the split
matches the file-level evidence, old rows null, new rows populated, exactly as
`01-add-column-old-rows-null.json` and `01-add-column-new-rows-populated.json` show. On this
project's real `bronze_playback_sessions` table, the same check is:

```sql
select
    _source_file,
    count(*) as row_count,
    count(playback_quality) as non_null_playback_quality
from iceberg.bronze.bronze_playback_sessions
group by _source_file
order by _source_file;
```

Pre-drift source files should show `non_null_playback_quality = 0` for every one of their rows;
post-drift files should show it populated for the overwhelming majority of rows, matching
`.notes/decisions.md`'s own verification of this exact split at ingestion time.
