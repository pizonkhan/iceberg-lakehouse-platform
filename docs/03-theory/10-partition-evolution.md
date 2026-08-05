# Partition evolution

This document goes deep on one specific Iceberg capability: changing a table's partition
scheme without rewriting the data already written under the old one. It assumes the reader
already knows what a partition spec and a field id are in general terms; if `09-iceberg-internals.md`
exists alongside this file, that is the place for the general metadata-tree and hidden-partitioning
background. This document is the worked case: this project's own scenario 5 evidence, reproduced in
full, plus the math for what the equivalent operation would cost under a Hive-style table at this
project's real scale, plus the one place partition evolution's benefit is real but partial rather
than complete.

## The problem it solves, stated precisely

A partition scheme is a bet made at table-design time about which columns future queries will
filter on. Early in a table's life the bet is often wrong or incomplete: a table partitioned by day
turns out to need month-grain retention sweeps, or a table with no partitioning at all turns out to
need one once row counts climb past the point where a full scan is cheap. In a Hive-style table
(schema-on-read, partition columns baked directly into the physical directory layout under the
table's location, `event_at_day=2026-01-01/`, `event_at_day=2026-01-02/`, and so on) that bet cannot
be revised in place. The partition columns are not metadata describing the files, they *are* how the
files are found: every file's physical path encodes the partition values it belongs to, and the
table's `PARTITIONED BY` clause is fixed at `CREATE TABLE` time. Changing it means every existing
file has to move to a new path that encodes the new scheme, which means reading and rewriting every
row the table holds.

The problem this creates in practice is a false choice: live with a partition scheme that no longer
matches how the table is actually queried, or pay for a full rewrite (and, for the migration window,
double the storage) to fix it. Iceberg's partition evolution exists to remove that choice's cost side
entirely for the common case: changing what future writes partition by, without touching a single
byte of what past writes already produced.

## The mechanism, from first principles

Iceberg tables carry two independent identity layers that a Hive table does not: a **field id** per
column, and a **partition spec** as its own versioned, named object in the table's metadata, separate
from the schema. Neither lives in the data files' physical location. Both live in `metadata.json`.

A partition spec is a list of `(field-id, source-id, transform)` triples. `source-id` points at the
schema field the value is derived from; `transform` is a function applied to that source field's
value at write time (`identity`, `day`, `month`, `bucket[N]`, and a handful of others); the spec's own
`field-id` (allocated starting at 1000, distinct from the schema's own column field ids, which start
at 1) identifies that derived partition value itself, independent of the source column's identity.
Every data file records, in its manifest entry, which spec id it was written under and the literal
partition values that spec produced for it. Nothing about this requires the file's physical path to
match the spec that produced it, though engines conventionally lay out paths that mirror the spec for
readability, which is exactly what the evidence below shows: a Hive-style-looking path per file, but
one whose meaning is fully recoverable from metadata rather than load-bearing on its own.

This is the structural fact that makes evolution possible: a partition spec change is an edit to
`metadata.json`'s `partition-specs` array and its `default-spec-id` pointer. It touches no data file,
because no data file's identity or location depends on which spec is currently the default. Every
file keeps carrying its own spec id and partition values forever; the query planner reads whichever
spec each file claims and applies pruning against that spec, per file, not against one table-wide
scheme. Old specs are never deleted from `partition-specs`, only superseded as the default for new
writes. This is also why an Iceberg partition spec change is instantaneous regardless of table size:
its cost is proportional to the size of one small JSON document, not to the number of rows the table
holds.

The bucket transform used in this project's own evidence (`bucket(subscriber_id, 4)`) deserves its
own line: it hashes the source value (Iceberg's spec uses a 32-bit hash, murmur3, of the value's
canonical binary representation) and reduces it modulo the bucket count, `bucket_id = hash(value) mod
N`, giving a fixed number of buckets independent of the cardinality of the underlying column, which
is the reason it exists at all: `day(event_at)` produces a naturally bounded number of partitions
because days are bounded, but `identity(subscriber_id)` or even `bucket` on a column with no natural
bound would not be, without an explicit bucket count capping it.

## This project's real evidence: scenario 5, worked in full

The demonstration lives in `docs/evidence/schema-evolution/`, files prefixed `05-`, run against a
dedicated table, `iceberg.schema_evolution_demo.demo_billing_events_partitioned`, kept separate from
the other schema-evolution scenarios' shared table specifically because partition-spec changes needed
a clean spec history from table creation, not one layered onto a table that started unpartitioned
(`00-setup-notes.txt`).

### Spec 0: the initial definition

```sql
CREATE TABLE demo_billing_events_partitioned (
   event_id varchar, subscriber_id varchar, amount integer,
   event_at timestamp(6), status varchar
)
WITH (format = 'PARQUET', format_version = 2,
      partitioning = ARRAY['day(event_at)']);
```

600 rows were inserted, `event_at` spanning 2026-01-01 through 2026-01-30. The raw `metadata.json`
captured immediately after (`05-partitioned-spec0-metadata.json`) records exactly one partition spec:

```json
"partition-specs": [
  {"fields": [{"field-id": 1000, "name": "event_at_day", "source-id": 4, "transform": "day"}],
   "spec-id": 0}
],
"default-spec-id": 0,
"last-partition-id": 1000
```

`$partitions` after this insert (`05-partition-spec0-partitions.json`) shows exactly what `day()`
produces: 30 distinct partition values, one file each, 20 records each, `2026-01-01` through
`2026-01-30`, every `file_count` equal to 1.

### Spec 1: the evolved definition

```sql
ALTER TABLE demo_billing_events_partitioned
  SET PROPERTIES partitioning = ARRAY['month(event_at)', 'bucket(subscriber_id, 4)'];
```

The command's full result, captured verbatim in `05-partition-evolve-command.txt`, is `SET
PROPERTIES`. Nothing else. No scan, no rewrite, no data-file operation of any kind, a single
metadata-only DDL statement that returns as fast as any other `ALTER TABLE ... SET PROPERTIES`
would. 400 more rows were then inserted, `event_at` spanning 2026-02-01 through 2026-03-02. The final
`metadata.json` (`05-partitioned-final-metadata.json`) now carries both specs, permanently:

```json
"partition-specs": [
  {"fields": [{"field-id": 1000, "name": "event_at_day", "source-id": 4, "transform": "day"}],
   "spec-id": 0},
  {"fields": [
     {"field-id": 1001, "name": "event_at_month", "source-id": 4, "transform": "month"},
     {"field-id": 1002, "name": "subscriber_id_bucket", "source-id": 2, "transform": "bucket[4]"}
   ], "spec-id": 1}
],
"default-spec-id": 1,
"last-partition-id": 1002
```

Spec 0 was not removed or rewritten to match spec 1. It is still entry zero in the array. Only
`default-spec-id` moved, from 0 to 1, meaning: new writes use spec 1; every already-written file keeps
whatever spec it was actually written under.

### The two physical layouts, coexisting under one table

`$files` grouped by `spec_id` (`05-partition-files-by-spec.json`) shows the split cleanly:

```
{"spec_id":0,"file_count":30,"total_records":600}
{"spec_id":1,"file_count":8,"total_records":400}
```

`05-partition-files-detail.json` lists every file's real path. Spec 0's 30 files sit one per day
under `.../data/event_at_day=2026-01-01/...parquet` through `event_at_day=2026-01-30/...parquet`,
each holding exactly 20 records, a flat Hive-style single-level layout. Spec 1's 8 files sit under a
two-level nested layout, `event_at_month=2026-02/subscriber_id_bucket=0/...parquet` through
`subscriber_id_bucket=3/...parquet`, then the same four buckets repeated under
`event_at_month=2026-03/`. The real per-file record counts for spec 1: February's four buckets hold
81, 94, 92, and 107 records (374 total); March's four buckets hold 6, 3, 10, and 7 (26 total),
summing to the full 400. The unevenness inside a month (81 versus 107, roughly a 32% spread across
only 250 distinct subscriber ids feeding four buckets) is consistent with a hash-based assignment
rather than a designed-even split, which is expected: `bucket[4]` guarantees a fixed *count* of
buckets, not that every bucket receives an equal share of any particular batch.

Both directory shapes exist side by side, right now, under the same table location
(`s3://warehouse/schema_evolution_demo/demo_billing_events_partitioned_4bba6c53.../data/`). Nothing
about spec 0's files changed when spec 1 became the default. This is the concrete, file-level meaning
of "evolution, not a rebuild": Iceberg never required the 30 spec-0 files to be moved, renamed, or
reorganized into spec 1's directory scheme, and never will, unless something explicitly rewrites them
later for reasons unrelated to the spec change itself (compaction, for instance).

### Proof that a query spanning both specs returns correct combined results

A query with no partition predicate at all (`05-partition-cross-spec-query.json`):

```json
{"total_rows":1000,"total_amount":54440,"min_event_at":"2026-01-01 00:00:00.000000",
 "max_event_at":"2026-03-02 00:00:00.000000"}
```

1,000 rows, exactly 600 plus 400, correct aggregate sum, correct min/max across the full range. A
second query deliberately straddles the spec-change boundary, January 25 through February 5, a window
containing 6 days written under spec 0 and 5 days written under spec 1
(`05-partition-cross-spec-range-query.json`):

```json
{"month":"2026-01-01 00:00:00.000000","row_count":120}
{"month":"2026-02-01 00:00:00.000000","row_count":69}
```

120 January rows (6 days times 20 rows/day, exact) and 69 February rows, pulled correctly from two
physically different directory layouts, two different partition specs, in one query, with no special
handling required in the SQL. The planner reads each file's own spec id from its manifest entry and
applies the right transform for that file; the query author never has to know two specs exist.

## The math: what a full rewrite would cost at this project's real scale

Iceberg's own evolution cost for the operation above is fixed and small: one `ALTER TABLE ... SET
PROPERTIES` statement, one metadata write, zero files read or written. The honest comparison is what
the equivalent change would cost if `demo_billing_events_partitioned` were a Hive-style table instead,
worked out using this project's own actual measured performance rather than a generic estimate.

This project's largest real table is `fct_playback_events`, 119,640,099 rows
(`.notes/decisions.md`, 2026-08-04). A full-refresh build of that table, one complete read of its
source plus one complete write of its own 119.6 million rows on this project's real (single-node,
memory-capped) Trino instance, was directly measured: 228 seconds to create the table, 231 seconds
total including hooks. That gives a real, derived per-row cost for one full pass over data at this
project's actual scale and hardware:

```
228 s / 119,640,099 rows ≈ 1.9 microseconds/row
```

A Hive-style partition-scheme change is not a metadata edit, it is a full pass exactly like that one.
Every existing file's partition columns are baked into its physical path, so changing the scheme
means reading every row and rewriting it under the new path layout, the same read-then-write shape as
the full-refresh CTAS above, at the same order of per-row cost. For a table `fct_playback_events`'s
size, that floors the operation at roughly the measured 228 seconds of pure compute, before accounting
for the fact that Hive offers no atomic in-place way to swap an existing table's `PARTITIONED BY`
clause: the real operational pattern is create a new table under the new scheme, `INSERT OVERWRITE`
the entire dataset into it (the same full read-write pass again), validate row counts against the old
table, repoint every downstream consumer, and only then drop the original. Until that final drop, both
copies exist on disk simultaneously, meaning the migration's storage footprint is roughly double the
migrated table's own size for the full window, not just its compute cost. This project's whole
generated dataset is about 3.0GB of Parquet on disk (`docs/02-data.md`), dominated by the ~120-million
row playback data; a real production table at meaningfully larger scale than this project's laptop
footprint would pay that same doubling ratio against however many actual terabytes it holds, for
however long the cutover window lasts.

Against that: Iceberg's actual evolution on this project's own demo table read zero of the existing
600 rows, wrote zero new files for the 30 already-committed spec-0 files, and returned in the time a
single `SET PROPERTIES` statement takes to commit a metadata update, independent of whether the table
held 600 rows or 119.6 million. The entire cost asymmetry is structural, not incidental: a Hive table
pays the per-row rewrite cost because partition identity and physical location are the same thing;
Iceberg does not, because they are not.

## When partition evolution does not help: the pruning asymmetry

Partition evolution's zero-rewrite property is real, but it buys correctness and cheap writes, not
uniform query performance across a table's whole history. Pruning is applied per file against
whichever spec that specific file was written under, which means a predicate that only lines up with
one spec's partition fields prunes well against files from that spec and poorly, or not at all,
against files from the other. This project's own two specs make the limitation exact rather than
hypothetical.

**Filtering only on `subscriber_id`.** Spec 0 has no partition field derived from `subscriber_id` at
all; it only ever partitions by `day(event_at)`. A query like `WHERE subscriber_id = 'sub-00042'`
against the table cannot eliminate a single one of the 30 spec-0 files at the partition level: Trino's
planner has no partition value to compare the predicate against for those files, so it falls back to
whatever file-level statistics happen to be available (column min/max, potentially a dictionary or
bloom filter), which is weaker than partition elimination and in the worst case means opening all 30
files regardless of the filter. The identical predicate against spec 1's 8 files gets real partition
pruning: `bucket(subscriber_id, 4)` is directly computable from the literal filter value, so the
planner can eliminate 3 of the 4 buckets in whichever month(s) the rest of the query touches before
opening a single file footer, up to 75% of spec-1's files skipped by partition value alone. Same
predicate, same table, radically different pruning outcome depending purely on which spec produced
the file being considered.

**Filtering on `event_at` at day precision after the spec moved to month grain.** A query for a single
day inside spec 0's range, `WHERE event_at = '2026-01-15'`, prunes to exactly 1 of the 30 files:
`day()` is spec 0's own transform, an exact match for the predicate's own granularity. The same
single-day predicate aimed at a date inside spec 1's range, `WHERE event_at = '2026-02-15'`, can only
be pruned at the partition level down to "the `2026-02` partition," which spans all of February; spec
1 never recorded a day-level value to eliminate against, because `month()` is coarser than `day()` by
construction. Trino still has to apply the day-level predicate as a residual filter within whatever
February files remain, either via each file's own row-group statistics or, failing that, by reading
and filtering rows directly, not through partition elimination for that dimension at all.

The precise statement worth carrying forward: after a spec change, a query that only filters on the
column(s) the *old* spec pruned by keeps getting sharp elimination against old files and degrades to
file-statistics-only (or coarser-granularity) pruning against new files, and a query that only filters
on the column(s) the *new* spec added gets no partition-level help at all against old files, until it
adds the predicate that spec was actually designed for. Partition evolution changes what future writes
are organized by; it does not retroactively reorganize what already exists, and no query gets to
assume every file in a table shares one partition scheme just because the table has one default spec
today.

## How to verify this is actually working

Every number above is reproducible directly against the live stack, and none of it depends on
`$snapshots` or `$history`, both of which this catalog's Nessie REST bridge exposes only for the
current commit (see `docs/03-theory/11-time-travel-snapshot-expiry.md` for why). Partition-spec and
file-level evidence is unaffected by that limitation: it comes from `partition-specs` in the raw
`metadata.json` (never pruned, every historical spec kept permanently) and from the current snapshot's
own live manifest list, not from snapshot lineage.

```sql
-- spec history and default pointer, straight from the catalog's system table
SELECT * FROM iceberg.schema_evolution_demo."demo_billing_events_partitioned$partitions";

-- file-level spec assignment, the direct proof both layouts coexist
SELECT spec_id, count(*) AS file_count, sum(record_count) AS total_records
FROM iceberg.schema_evolution_demo."demo_billing_events_partitioned$files"
GROUP BY spec_id;

-- raw metadata.json, pulled directly from MinIO rather than through Trino's
-- system tables, the same method used to capture 05-partitioned-final-metadata.json
```

Running the cross-spec aggregate and the January 25 through February 5 range query shown above
against the real table reproduces `05-partition-cross-spec-query.json` and
`05-partition-cross-spec-range-query.json` exactly. To see the pruning asymmetry directly rather than
take it on description, run `EXPLAIN` (or Trino's query-stats output) for `WHERE subscriber_id =
'<any real value>'` and compare the number of spec-0 files listed against spec-1 files listed in the
plan's scan node; the split will not be proportional to each spec's share of total files.
