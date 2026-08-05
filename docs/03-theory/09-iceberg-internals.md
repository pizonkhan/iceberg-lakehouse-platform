# Iceberg internals

Every other document in this theory section explains a modeling problem this platform's
dimensional model solves. This one explains the substrate all of it sits on: what an Iceberg
table actually is on disk, how a snapshot commits atomically, what copy-on-write and
merge-on-read really cost, and where this project's specific catalog choice, Project Nessie
(ADR-001), diverges from what a plain Hadoop or Glue-backed Iceberg catalog would do. Every claim
below about this project's own tables, numbers, or behavior is either pulled directly from the raw
evidence captured in `docs/evidence/schema-evolution/` and `docs/evidence/time-travel/`, or from a
live query run against the real stack while writing this document (the running Trino container,
the `iceberg` catalog, the real tables under `dev_facts` and `dev_dimensions`). Nowhere below is
Iceberg's metadata model described in the abstract when a real captured file could be quoted
instead.

## 1. The metadata tree: catalog pointer to data file

### The problem it solves

A table backed by files in object storage has no natural notion of "the current state of the
table." Object storage gives you a flat namespace of immutable blobs; nothing in S3 or MinIO
itself knows which of the thousands of Parquet files under a given prefix belong to the table's
current, correct contents versus a half-finished write, a file from three schema versions ago, or
an orphaned write from a query that failed midway through. A table format has to answer three
questions without a central database doing row-level bookkeeping: which files, right now,
constitute this table; what did the table look like a moment ago, before the write in progress
finished; and how can a query engine find the small number of files relevant to its predicate
without listing and opening every file in the table. Iceberg answers all three with one idea: a
tree of immutable metadata files, rooted at a single pointer that changes atomically, with each
layer of the tree existing specifically to let a reader prune more of the tree it doesn't need to
open.

### The mechanism, walked top to bottom

Four layers, each a real, separately addressable object in storage:

**1. The catalog pointer.** Some external system (a Hive metastore table property, a single
`version-hint.text` file for a bare Hadoop catalog, or, on this project's stack, a Nessie
reference) records where the table's current `metadata.json` lives. This is the one piece of
state that is not itself part of the Iceberg metadata tree; it is what makes the tree reachable
at all.

**2. `metadata.json`.** One JSON file holding the table's entire structural history: every schema
version it has ever had, every partition spec, every snapshot, and which snapshot is current. It
never grows unbounded in the way it might sound: old data is not re-read from it, only consulted
when a query needs history.

**3. The manifest list.** An Avro file, one per snapshot, that inventories the manifest files
belonging to that snapshot, with per-manifest summary statistics (partition value ranges, added
and deleted file counts) an engine can use to skip whole manifests without opening them.

**4. Manifest files.** Avro files that list actual data files (and, for tables using them, delete
files), each entry carrying the file's path, its partition values under whatever spec wrote it,
its row count, its size, and column-level lower and upper bounds used to prune the file entirely
if a query's predicate cannot possibly match anything inside it.

**5. Data files.** Ordinary Parquet (in this project's case; `format = 'PARQUET'` on every table)
files, never rewritten by a schema change, only ever added or, eventually, marked no longer
referenced.

The layer that makes this a *tree* rather than a flat list is that a reader never has to open more
than it needs: it reads one `metadata.json`, follows one pointer to one manifest list, reads that
list's small summary rows to decide which manifests are even worth opening, and only then opens
the manifests whose summaries say they might contain relevant data. A table with a million data
files across a thousand manifests can be queried by reading `metadata.json`, one manifest list,
and, in the best case, a single manifest, never touching the other 999.

### The Nessie wrinkle, and why it matters here

On a bare Hadoop or Glue catalog, step 1 above is a single mutable pointer: one location, one
current value, full stop. On this project's Nessie-backed catalog, the "catalog pointer" is not a
single global value but a value scoped to a Nessie reference (a branch or tag), because Nessie's
own commit graph, not a single overwritten pointer, is what determines "current." The real
evidence for this shape is in `docs/evidence/time-travel/04-nessie-history-before-rollback.json`:
each commit's `content` block carries its own `metadataLocation`, e.g.

```json
"metadataLocation": "s3://warehouse/time_travel_demo/demo_billing_batch_a2a989cc-58ec-43ed-b4f4-b005776ed4af/metadata/00000-91e813ec-bf83-4392-9ef8-4eabdb16bf76.metadata.json"
```

and the exact same table has a *different* `metadataLocation` on the commit immediately before it.
"Ask the catalog for this table's metadata.json" is therefore, on this stack, really "ask Nessie
for this table's metadata.json as of this ref" (main, a WAP branch, a demo branch), and different
refs can genuinely disagree about which `metadata.json` is current for the identical table. This
is a real, load-bearing consequence of the catalog choice, not a simplification for this document:
it is exactly what makes Nessie's catalog-wide branching possible (a whole namespace of tables can
be branched, isolated, and merged as one unit, which is why this project picked Nessie over Polaris
in the first place, ADR-001), and it is also the root of the snapshot-lineage limitation covered in
section 6 below. Keep this in mind through the rest of this section: every `metadata.json` example
quoted below is the real file for one specific ref at one specific commit, not a table-wide
singleton.

### Worked example: a real captured `metadata.json`, field by field

`docs/evidence/schema-evolution/00-setup-metadata.json` is the raw `metadata.json` pulled directly
from MinIO (not narrated, not reconstructed from Trino's system tables) for
`iceberg.schema_evolution_demo.demo_billing_events` immediately after its two seed `INSERT`
batches (500 rows each, 1,000 rows total). Walking it top to bottom:

```json
"format-version": 2,
"table-uuid": "1255de13-9df8-499e-a290-fe4d840131de",
"location": "s3://warehouse/schema_evolution_demo/demo_billing_events_24684717-8629-4f02-aaa0-3e1cbdfcff8a",
"last-column-id": 5,
"current-schema-id": 0,
"current-snapshot-id": 9127784597258499632,
```

Five real columns exist at this point (`event_id`, `subscriber_id`, `amount`, `event_at`,
`status`), so `last-column-id` is 5, and the schemas array carries exactly one entry, `schema-id`
0, with those five fields at field ids 1 through 5:

```json
"schemas": [{"fields": [
  {"id": 1, "name": "event_id", "required": false, "type": "string"},
  {"id": 2, "name": "subscriber_id", "required": false, "type": "string"},
  {"id": 3, "name": "amount", "required": false, "type": "int"},
  {"id": 4, "name": "event_at", "required": false, "type": "timestamp"},
  {"id": 5, "name": "status", "required": false, "type": "string"}
], "schema-id": 0, "type": "struct"}],
```

`partition-specs` has one entry, spec 0, `"fields": []` (the table is unpartitioned at this point
in its history), and, worth noting for section 5 below, `"last-partition-id": 999`: Iceberg
reserves field ids 1000 and up for partition fields by convention, so a `last-partition-id` of 999
means none has been allocated yet, exactly the state right before the partitioned demo table's
first spec allocates field id 1000 (see the worked partition-evolution example later in this
document). The snapshots array has exactly one entry, since the two seed batches both landed as
appends onto the same snapshot lineage entry point being queried here:

```json
"snapshots": [{
  "manifest-list": "s3://warehouse/schema_evolution_demo/demo_billing_events_24684717-8629-4f02-aaa0-3e1cbdfcff8a/metadata/snap-9127784597258499632-1-ec8e7058-1929-4b3b-bd39-8b5a5ee22610.avro",
  "schema-id": 0,
  "sequence-number": 3,
  "snapshot-id": 9127784597258499632,
  "summary": {
    "operation": "append",
    "added-data-files": "1",
    "added-records": "500",
    "total-records": "1000",
    "total-data-files": "2",
    "total-delete-files": "0",
    "iceberg-version": "Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)"
  },
  "timestamp-ms": 1785886423356
}],
```

That `manifest-list` value is a real path this project's own MinIO bucket actually holds today.
The manifest list is what step 3 in the mechanism above describes: an Avro file inventorying every
manifest belonging to this snapshot. Reading through to what those manifests actually list, `$files`
(`docs/evidence/schema-evolution/00-setup-files.json`) is Trino's own system table for exactly
this content, populated by reading the real manifest entries, not a separate bookkeeping layer
Trino invents:

```json
{"content":0,"file_path":"s3://warehouse/schema_evolution_demo/demo_billing_events_24684717-8629-4f02-aaa0-3e1cbdfcff8a/data/20260804_233343_00175_ddgj6-3082a349-22c2-4ef5-9286-252aac37285b.parquet","file_format":"PARQUET","spec_id":0,"record_count":500,"file_size_in_bytes":2852}
{"content":0,"file_path":"s3://warehouse/schema_evolution_demo/demo_billing_events_24684717-8629-4f02-aaa0-3e1cbdfcff8a/data/20260804_233336_00174_ddgj6-ad015423-d3b5-4fa2-be5b-28536b89a0aa.parquet","file_format":"PARQUET","spec_id":0,"record_count":500,"file_size_in_bytes":2848}
```

Two real Parquet files, 500 records apiece, exactly matching `total-data-files: 2` and
`total-records: 1000` in the snapshot summary above. `content: 0` is a manifest-entry field with
three possible integer values (0 data, 1 position delete, 2 equality delete, covered in full in
section 4), so these are ordinary data files.

Beyond the manifest-derived scan-pruning bounds (per-file lower and upper bounds on each column,
kept inside the manifest entries themselves and used to skip a file whose bounds cannot satisfy a
query predicate), this same snapshot also has a separate Puffin statistics file, a genuinely
different mechanism from manifest-level pruning, used for cost-based query planning rather than
file skipping:

```json
"statistics": [{
  "blob-metadata": [
    {"fields": [1], "properties": {"ndv": "1000"}, "type": "apache-datasketches-theta-v1"},
    {"fields": [2], "properties": {"ndv": "250"}, "type": "apache-datasketches-theta-v1"},
    {"fields": [3], "properties": {"ndv": "90"}, "type": "apache-datasketches-theta-v1"},
    {"fields": [4], "properties": {"ndv": "120"}, "type": "apache-datasketches-theta-v1"},
    {"fields": [5], "properties": {"ndv": "3"}, "type": "apache-datasketches-theta-v1"}
  ],
  "snapshot-id": 9127784597258499632,
  "statistics-path": "s3://warehouse/schema_evolution_demo/demo_billing_events_24684717-8629-4f02-aaa0-3e1cbdfcff8a/metadata/20260804_233343_00175_ddgj6-ee5602c0-fa4a-43eb-8358-ce065d383879.stats"
}],
```

These are Theta-sketch distinct-value estimates per field id (`status`, field 5, correctly shows
`ndv: 3`, matching the seed data's real `refunded`/`pending`/`completed` cycle), used by the query
planner to estimate cardinality and choose join strategies. It is a real, separate metadata
artifact from the manifest-level min and max bounds that drive file pruning; the two are easy to
conflate because both are described loosely as "column statistics," but one lives in the manifest
entries and prunes files at scan time, the other lives in a Puffin blob and informs the optimizer
before a scan is even planned.

### How a read resolves, concretely

Putting the layers together as an algorithm, using this table's own real values:

1. Resolve the catalog pointer for the ref being queried (on this stack, ask Nessie for `main`'s
   current content record for `schema_evolution_demo.demo_billing_events`). This yields a
   `metadataLocation`.
2. Fetch that `metadata.json` and read `current-snapshot-id`: `9127784597258499632`.
3. Look that id up in the `snapshots` array to get `manifest-list`:
   `.../metadata/snap-9127784597258499632-1-ec8e7058-1929-4b3b-bd39-8b5a5ee22610.avro`.
4. Read that Avro file. It lists the manifest files belonging to this snapshot, each with a
   partition-value summary the engine checks against the query's predicate before deciding to open
   it at all.
5. For each manifest not pruned in step 4, read its entries: file path, partition values under
   that entry's spec, record count, and per-column lower and upper bounds.
6. Any manifest entry whose bounds cannot satisfy the query's predicate is skipped without ever
   opening the Parquet file. What survives is the real, minimal file set: for an unfiltered scan of
   this table at this point in its history, exactly the two files listed above, 1,000 rows total.

### When it fails

The tree's guarantees are only as strong as its atomic root swap (covered fully in section 2): if
the catalog pointer is ever hand-edited or a data file deleted directly from object storage without
going through the catalog, the tree becomes internally inconsistent, and every subsequent read or
write referencing the missing piece fails hard rather than silently. This is not hypothetical for
this project: a real incident (`.notes/decisions.md`, 2026-08-04) did exactly this, a bulk `mc rm
--recursive` against a bronze table's storage prefix outside the catalog, and the next append to
that table failed every retry with `FileNotFoundError` trying to read its own prior snapshot's
manifest list, because Nessie's catalog record still pointed at a manifest that object storage no
longer had. The lesson recorded there generalizes directly from the tree structure explained above:
never delete Iceberg table files directly from the object store, even test debris; always drop
through the catalog so storage and metadata stay consistent, because nothing below the catalog
pointer knows how to tolerate a layer of the tree going missing out from under it.

### How to verify this is actually working

Reproduce the walk above directly against this project's real stack. Any real table exposes the
tree through Trino's system tables without needing to fetch raw Avro:

```sql
select * from iceberg.dev_facts."fct_billing_transactions$snapshots";
select * from iceberg.dev_facts."fct_billing_transactions$manifests";
select content, file_path, record_count, file_size_in_bytes
from iceberg.dev_facts."fct_billing_transactions$files";
```

or fetch the raw `metadata.json` directly, the same way the schema-evolution evidence was
captured, via `mc cat` against the location recorded in `$properties`:

```sql
select * from iceberg.dev_facts."fct_billing_transactions$properties";
```

which returns `location` as a literal `s3://...` path whose `metadata/` prefix holds every
`metadata.json` this table has ever had.

## 2. Snapshot isolation and optimistic concurrency control

### The problem it solves

A reader and a writer can be active against the same table at the same moment. Without a specific
guarantee about what the reader sees, this is a race: a reader might see half the writer's new
rows and none of the deletes that go with them, or a partially written file, or a table that
briefly has duplicate or missing rows mid-write. Iceberg's guarantee is precise: a reader always
sees a single, complete, self-consistent snapshot, either entirely the state before a concurrent
write, or entirely the state after it, never anything in between. Delivering that guarantee without
locking the whole table for the duration of every write is what the tree structure from section 1
and the atomic pointer swap below exist to make possible.

### The mechanism: why the pointer swap is what makes isolation free

A writer building a new snapshot never modifies anything the current snapshot's tree already
points to. It writes new data files, new manifests, and a new manifest list, all as brand-new
objects in storage, and only as the very last step does it attempt to make that new snapshot
current, by a single atomic operation that swaps the catalog pointer from the old `metadata.json`
to the new one (or, in Nessie's case, commits a new content record on the ref). Every reader that
resolved the pointer before that swap is holding a reference to a `metadata.json` whose entire tree
beneath it, manifests, files, everything, is untouched by the write in progress, because none of it
was ever mutated in place. That reader's query runs to completion against a tree that cannot
change out from under it, not because of a lock, but because nothing it is reading was ever going
to change: the old files stay exactly as they were, referenced by the old manifest list, referenced
by the old `metadata.json`, for as long as anything still points at them. A reader that starts after
the swap resolves the new pointer and gets the new, equally complete tree. There is no state in
between the two that any reader can ever observe, because the pointer itself has no intermediate
value: it is one value, then atomically another.

### This project's own proof: the mid-merge kill test

A red team pass (`.notes/decisions.md`, 2026-08-05, part 3) set out to prove this atomicity
directly rather than take it on faith, by killing a real, live Trino `MERGE` mid-write against
`fct_billing_transactions` and inspecting the table afterward.

Baseline, before any of it: 1,500,100 rows, content checksum (Trino's `checksum()` aggregate over
every column except the wall-clock `loaded_at`, so any drift can only mean real corruption, not an
artifact of the check itself) `BA98E50C4EF99C85`. The exercise used the model's own documented
backfill mechanism with a window covering the whole dataset, which forces the `MERGE` to re-match
and rewrite all 1,500,100 existing rows, a real, substantial write, while being provably
content-neutral (every non-`loaded_at` column is a pure function of unchanged upstream data).

**First kill attempt.** The backfill was launched, `system.runtime.queries` polled every 50
milliseconds for a `RUNNING` query touching the table, and the instant one appeared, both
`system.runtime.kill_query` and a `SIGKILL` on the dbt client process were fired (both, deliberately:
killing only the client does not guarantee the server-side query stops, since Trino can keep
executing after its client disconnects). This landed at t+2.018 seconds into a run that otherwise
completes in about 4.9 seconds, during the scan or plan phase, before any write had begun: zero new
Parquet files appeared with that query's id prefix, and the table's row count and checksum were
unchanged, the cleanest possible outcome, but not yet the strongest demonstration, since nothing had
actually been written to interrupt.

**Second attempt**, deliberately re-run to catch a write in flight: the same kill method, but
polling MinIO's `data/` directory directly for a new file to appear rather than only watching query
state. This landed later, t+6.118 seconds, and caught a real write: the file
`20260805_041256_00236_u3ufz-1da325f7-20a7-4735-a5b4-2be114799254.parquet` had appeared in
`s3://warehouse/dev_facts/fct_billing_transactions_.../data/` before the kill landed. Confirmed
after the kill: the query shows `state=FAILED, error_type=USER_ERROR,
error_code=ADMINISTRATIVELY_KILLED`; the orphaned Parquet file is still physically present in MinIO
(Iceberg does not auto-delete uncommitted files on query failure, that is what the
`remove_orphan_files` maintenance procedure exists for, not something that runs implicitly);
querying `iceberg.dev_facts."fct_billing_transactions$files"` for that exact path returns zero
rows, meaning the current committed snapshot's manifests reference it nowhere. Table state
immediately after: 1,500,100 rows, checksum `BA98E50C4EF99C85`, unchanged, distinct
`billing_transaction_id` count also 1,500,100 (no duplication from the interrupted merge), and a
plain `select count(*)` succeeded throughout, run once deliberately in the same breath as the final
rerun to demonstrate the table was never unavailable or in a locked or partial state at any point.

The backfill was then rerun to completion twice, the model's own documented recovery path, no
special repair step: both reruns completed normally (`MERGE (1_500_100 rows)` in dbt's own output,
4.2 to 4.4 seconds), and the table's final row count and checksum matched the original baseline
exactly after each. `dbt test --select fct_billing_transactions` (17 tests) and the full
`tests/integration` pytest suite (45 passed, 1 skipped) both pass clean against the post-exercise
table. This is the mechanism from the previous paragraph observed directly rather than reasoned
about: a query that wrote a real, orphaned Parquet file to storage still left the table's readable,
committed state completely untouched, because a file being written to object storage and a file
being referenced by a committed manifest are two separate events, and only the second one is what
readers can ever see.

### Optimistic concurrency control: the compare-and-swap

The atomic pointer swap is also what makes concurrent *writers* safe without locking. Two writers
racing to commit the next snapshot of the same table both read the current pointer as their base,
build their respective new trees independently against that base, and then each attempts the same
compare-and-swap: "move the pointer from base to my new metadata.json, but only if the pointer is
still at base." Exactly one of them can win that race, because a compare-and-swap on a single value
is inherently serializable: the first one to land moves the pointer and succeeds; the second one's
precondition (the pointer still being at base) is now false, so its commit is rejected outright. The
losing writer's work (its new manifests and data files) is not lost, but its commit did not apply;
it has to re-read the new current state and retry its logical operation against that new base, not
blindly reapply its stale one.

On this project's stack, that compare-and-swap is Nessie's own conditional commit: a branch commit
or merge is submitted with an expected current hash, and Nessie rejects it with a real,
observable conflict if the branch has moved since. `ops/wap.py`'s own merge call is a direct,
real instance of exactly this pattern already exercised in this codebase (covered in full in
[12-write-audit-publish.md](12-write-audit-publish.md)):

```python
merge_result = nessie.merge_branch(
    config.nessie_uri, "main", current_main_ref.hash, branch_name, built_branch_ref.hash,
)
```

`current_main_ref.hash` is passed as the merge's expected target hash. If `main` moved since it was
last read, the merge is rejected rather than silently clobbering a concurrent write. A real conflict
of exactly this shape was hit during development, not staged: elementary-data's dbt package writes
project-wide bookkeeping tables into an unscoped schema on every invocation, and a WAP run's own
writes to those keys collided with ordinary concurrent dbt activity against `main`, producing a
genuine Nessie `REFERENCE_CONFLICT` at merge time (`.notes/surprises.md`, 2026-08-05). That is real,
observed optimistic-concurrency rejection working exactly as designed, catalog-side, even though it
surfaced from bookkeeping tables rather than the tables this project actually models.

### An honest gap: concurrent writers to the same table were not tested

The mid-merge kill test above proves atomicity under a single writer being interrupted. It does not
prove what happens when two independent writers race to commit conflicting changes to the same
table at the same instant. This project did not build or run that scenario. The compare-and-swap
mechanism described above is Iceberg's documented, general design, and this project's own code does
lean on the identical primitive at the Nessie-branch level (the merge conflict above is real,
observed evidence of the primitive functioning), but "two writers simultaneously appending to
`fct_billing_transactions` and confirming exactly one wins while the other retries cleanly" is not a
scenario this project constructed and verified end to end. Stating that as tested would overstate
what was actually done.

### How to verify this is actually working

Reproduce the mid-merge kill demonstration directly:

```
uv run dbt build --select fct_billing_transactions --target trino \
  --vars '{"backfill_start": "2023-01-01 00:00:00.000000", "backfill_end": "2026-08-04 00:00:00.000000"}'
```

while polling in a second terminal:

```sql
select query_id, state from system.runtime.queries
where query like '%fct_billing_transactions%' and state = 'RUNNING';
```

and killing it mid-flight with `call system.runtime.kill_query(query_id => '...', message => '...')`.
Confirm afterward that row count and a `checksum()` over every non-`loaded_at` column match the
pre-kill baseline, and that
`select * from iceberg.dev_facts."fct_billing_transactions$files" where file_path = '<the orphaned
path>'` returns zero rows.

## 3. Copy-on-write versus merge-on-read

### The problem it solves

Every row-level `UPDATE`, `DELETE`, or `MERGE` against an Iceberg table has to decide how to
represent "this row is no longer part of the table" without mutating the immutable data file that
row physically lives in. There are exactly two structural answers. Copy-on-write rewrites the
entire data file that contained the changed row, minus the removed rows plus any replacements, and
atomically swaps the new file in for the old one, the same pointer-swap mechanism from section 2
applied at file granularity. Merge-on-read leaves the original data file untouched and writes a
small, separate delete file recording which specific rows in which specific data file are no longer
live; a reader reconciles data files against their applicable delete files at scan time. Both
strategies produce the same logical answer to any query. They cost completely different amounts to
write and to read, and that cost difference is the entire reason the choice exists as a table
property rather than a fixed behavior.

### Defining the tradeoff precisely

**Write amplification** is the ratio of physical bytes actually written to storage to the logical
bytes of the change being made. **Read amplification** is the ratio of physical bytes a query
engine must scan to answer a query to the minimum bytes that would contain just that answer, if the
table held only a single, current, fully compacted copy of its live data.

Copy-on-write pushes cost toward every write: even a change touching one row in a large file
requires reading and rewriting that entire file, so write amplification for a small, scattered
change can be enormous, but every read afterward sees a clean, minimal, single-copy table with no
delete reconciliation to do, so read amplification stays at 1x by construction. Merge-on-read
inverts this: a write costs almost exactly what the logical change costs (a small new data file for
the changed rows, a small delete file marking the old ones), so write amplification stays close to
1x, but every read has to reconcile data files against however many delete files have accumulated
since the last compaction, and superseded data files are not reclaimed until something explicitly
rewrites them, so read amplification grows with every uncompacted write.

### Which strategy this project's tables actually use, checked directly

`WITH (...)` in every `CREATE TABLE` this project runs sets only `format`, `format_version`, and
occasionally `partitioning`; none of them sets `write.delete.mode`, `write.update.mode`, or
`write.merge.mode`. Querying the real table properties directly against `fct_billing_transactions`
confirms no override exists:

```
"key","value"
"format","iceberg/PARQUET"
"provider","iceberg"
"format-version","2"
"gc.enabled","false"
"write.format.default","PARQUET"
"nessie.catalog.content-id","fec58f8a-c887-46b7-80f7-0331fac56ab0"
```

(`select * from iceberg.dev_facts."fct_billing_transactions$properties"`, run live against the
real stack.) No delete-mode property appears at all, which means every table in this project runs
on Iceberg's v2 default. That default is not merely assumed here: it is directly, physically
confirmed by the presence of real delete files on disk (walked in full in section 4), something
that could only exist if the engine actually executing these `MERGE` statements is writing
merge-on-read deltas rather than rewriting whole files. This project's tables use merge-on-read,
confirmed by the files themselves, not read off a config flag that happens to be absent.

### The math, worked against this table's real bytes

`fct_billing_transactions` sits at 1,500,100 live rows today. Its `$files` system table, queried
live, shows five real data files and four real position-delete files, an artifact of the mid-merge
kill exercise in section 2 having rerun a whole-table backfill `MERGE` several times over:

```
"content","file_path","record_count","file_size_in_bytes"
"0",".../20260805_041051_00214_...-e00f8689....parquet","1500100","42392037"
"0",".../20260805_040918_00188_...-a03aa61c....parquet","1500100","42319324"
"0",".../20260805_040713_00099_...-48ae0f70....parquet","1500100","42419049"
"0",".../20260805_003308_00325_...-4bedc307....parquet","1500100","41632047"
"0",".../20260805_041334_00263_...-b3240aa9....parquet","1500100","42309406"
"1",".../20260805_040713_00099_...-01055d01....parquet","1500100","1565984"
"1",".../20260805_041334_00263_...-0d8c4bc1....parquet","1500100","1565980"
"1",".../20260805_041051_00214_...-714e2969....parquet","1500100","1565983"
"1",".../20260805_040918_00188_...-44ead239....parquet","1500100","1565980"
```

Every one of the five data files is a full copy of all 1,500,100 rows (each backfill rewrote the
whole table's worth of rows as new data files rather than in place), and each of the four delete
files marks all 1,500,100 rows from the *previous* full copy as superseded. The row-count arithmetic
is exact: five data files at 1,500,100 records each sum to 7,500,500 physical data rows; four delete
files at 1,500,100 positions each sum to 6,000,400 positions marked no longer live;
`7,500,500 - 6,000,400 = 1,500,100`, exactly the live `count(*)` queried at the top of this section.
The byte totals tell the same story in storage terms: data bytes sum to 211,071,863; delete bytes sum
to 6,263,927; total physical bytes on disk for this table right now is 217,335,790, against a live,
logical content of only about 42.2 million bytes (one data file's worth).

**Read amplification, as it stands today.** A full scan has to read all five data files and all
four delete files to determine which of the 7,500,500 physical data rows are actually live, 217.3
million bytes physically scanned to answer a query whose live content would fit in one 42.2 million
byte file. That is a read amplification of `217,335,790 / 42,214,373`, approximately **5.15x**,
right now, on this real table, because no compaction has run since the mid-merge kill exercise
(`gc.enabled` is `false` at the table level, and nothing in this project runs `expire_snapshots` or
`rewrite_data_files` on a schedule).

**Write amplification, for a realistic small correction.** Suppose a routine correction touches
1,000 already-posted transactions, the kind of case the backfill mechanism's own comment describes
("re-pull a batch that was missed or corrected upstream"), scattered arbitrarily across the table
(this table carries no `partitioning` clause, confirmed in its `SHOW CREATE TABLE` output, so a
`MERGE` matching on `billing_transaction_id` has no locality guarantee and will, in the general
case, touch rows in every existing data file). Using this table's own observed bytes per row
(`42,214,373 / 1,500,100`, approximately 28.14 bytes per row, Parquet-compressed) and bytes per
delete position (`1,565,982 / 1,500,100`, approximately 1.04 bytes per position):

Under merge-on-read (what this table actually does): a new data file for the 1,000 corrected rows
(about 28,140 bytes) plus a position-delete file marking the 1,000 old positions (about 1,044
bytes), roughly 29,184 physical bytes written for a 28,140-byte logical change: write amplification
of about **1.04x**.

Under copy-on-write (the counterfactual this project's tables do not run, modeled here from this
table's own real per-row byte ratios rather than measured from an actual copy-on-write execution,
since no such run was performed for direct comparison): every data file containing at least one of
the 1,000 matched rows has to be read and rewritten whole. With no partitioning to localize the
match, all five 42-megabyte-class data files are, in the general case, implicated, 211,071,863
physical bytes rewritten for the same 28,140-byte logical change: write amplification of roughly
`211,071,863 / 28,140`, approximately **7,502x**.

That gap, about 1x against roughly 7,500x for the identical logical correction, is the whole reason
merge-on-read exists as an option, and it is also the whole reason merge-on-read's own cost (the
5.15x read amplification measured above, live, on this real table) is not free: it is deferred write
cost, not eliminated write cost, and it accumulates on every disk read until something pays it back
by compacting.

### When it fails, and the honest cost this project carries right now

Merge-on-read's failure mode is exactly what section 3's numbers show: read amplification grows
without bound as writes accumulate, unless a compaction procedure (Iceberg's `rewrite_data_files`,
which folds delete files back into fresh, clean data files and reclaims the superseded ones) runs
periodically. This project has not run that procedure against `fct_billing_transactions`; the
5.15x figure above is the real, current, uncorrected state of the table, left in place deliberately
after the mid-merge kill exercise rather than cleaned up, since cleaning it up was not necessary to
prove the atomicity claim that exercise existed to demonstrate. Anyone treating this table as a
production workload would need a scheduled compaction pass this project does not yet run.

### How to verify this is actually working

```sql
select * from iceberg.dev_facts."fct_billing_transactions$properties";

select content, count(*) as files, sum(record_count) as records,
       sum(file_size_in_bytes) as bytes
from iceberg.dev_facts."fct_billing_transactions$files"
group by content;
```

`content = 0` rows are data files; `content = 1` rows are position-delete files (full mechanism in
section 4). Comparing `sum(bytes)` across both groups against `select count(*) from
fct_billing_transactions` reproduces the amplification arithmetic above directly against whatever
the live table looks like at the time it is run.

## 4. Position deletes versus equality deletes

### The mechanism of each

Iceberg's v2 spec (the version this project is pinned to, ADR-002) represents a merge-on-read
delete as a separate file with a `content` type distinct from ordinary data:

**Position deletes** (`content = 1`) record a specific `(file_path, position)` pair for every
deleted row: exactly which physical file and which row offset within it is no longer live. The
writer knows precisely where each matched row sits, because it found it via a scan or an index, so
it can point at it directly. A reader reconciles a data file against its position-delete files by a
cheap join on file path and row position, no predicate evaluation needed.

**Equality deletes** (`content = 2`) record a set of column values instead of a physical location:
"delete any row where these columns equal these values." This is what a writer reaches for when it
does not have, or does not want to pay for, a read-before-write step to locate the exact physical
position of the row being replaced, the shape a streaming CDC writer (Flink is the canonical
example) commonly produces, applying upserts without scanning the target table first. The cost is
on the read side: every data file has to be evaluated against the equality predicate to know which
of its rows to exclude, since there is no positional index to join against directly.

### When each is used, and what actually produces which

Trino's Iceberg connector, the only write path any table in this project goes through, always
resolves a `MERGE`, `UPDATE`, or `DELETE` to position deletes: it plans the statement as a real
scan that locates the exact matched rows before writing anything, so it always has a physical
position to record and never needs to fall back to an equality predicate. Equality deletes exist in
the v2 spec specifically for writers that cannot or do not want to pay that scan cost, a category
this project's write path (`dbt-trino`, `incremental_strategy='merge'`) does not belong to.

### Checked directly against this project's real data

Rather than assume Trino's connector never produces equality deletes on this project's tables, this
was checked live against every incremental fact, the bridge table, and the one incrementally
merge-able dimension in the warehouse:

```sql
select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."fct_billing_transactions$files" group by content;
-- content=0: 5 files, 7,500,500 records   content=1: 4 files, 6,000,400 records

select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."fct_daily_subscription_snapshot$files" group by content;
-- content=0: 2 files, 27,160,730 records   content=1: 1 file, 149,384 records

select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."fct_playback_events$files" group by content;
-- content=0: 4 files, 119,640,099 records   (no content=1 or content=2 rows)

select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."fct_watchlist_adds$files" group by content;
-- content=0: 1 file, 750,000 records   (no content=1 or content=2 rows)

select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."fct_signup_funnel$files" group by content;
-- content=0: 1 file, 70,000 records   (no content=1 or content=2 rows)

select content, count(*) as files, sum(record_count) as records
from iceberg.dev_dimensions."dim_subscriber$files" group by content;
-- content=0: 2 files, 125,616 records   (no content=1 or content=2 rows)
```

Every result above is a real, live query result against the running stack, not a projection. Two
facts stand out.

**No `content = 2` (equality delete) file exists anywhere in this project.** Across every table
checked, `content` only ever takes the values 0 and 1. This is exactly what the write-path
reasoning above predicts, confirmed rather than assumed: Trino's `MERGE` never needs the
equality-predicate fallback because it always resolves an exact position first.

**Position deletes exist on exactly two tables, and for two structurally different reasons.**
`fct_playback_events`, `fct_watchlist_adds`, `fct_signup_funnel`, and `dim_subscriber` have never
produced a single delete file, because their `MERGE`'s unique key genuinely never recurs in normal
operation: each incremental run's watermark (bronze `_ingested_at`) selects rows that are new to
this run by construction, so the `MERGE`'s `WHEN MATCHED` branch is never hit and the statement
degenerates to a pure insert (`dim_subscriber` is additionally full-refresh rather than incremental
at all, per its own build entry in `.notes/decisions.md`, so it was never going to produce a delete
file regardless). This confirms, directly against real data rather than by assumption, that a
`MERGE` whose incoming batch is always genuinely new rows can run indefinitely without ever writing
a delete file, exactly the honest finding this section set out to check for. `fct_billing_transactions`
and `fct_daily_subscription_snapshot` are the two counterexamples, and for different reasons worth
distinguishing rather than collapsing into one story: `fct_billing_transactions`'s delete files are
an artifact of the red-team mid-merge kill exercise (section 2) deliberately re-running a
whole-table backfill `MERGE` several times, forcing every existing `billing_transaction_id` to
re-match, not something that happens in this table's ordinary incremental operation. `fct_daily_
subscription_snapshot`'s delete files, by contrast, are organic: its unique key,
`(snapshot_date_key, subscriber_sk)`, can genuinely recur across reruns that overlap the most
recent days already materialized (the model's own header comment describes a horizon recomputed
fresh on every run), so ordinary reruns of this specific fact do produce real, expected position
deletes without any artificial exercise driving them.

### How to verify this is actually working

```sql
select content, count(*) as files, sum(record_count) as records
from iceberg.dev_facts."<any incremental fact>$files"
group by content;
```

`content = 0` is data, `content = 1` is position deletes, `content = 2` would be equality deletes.
Running this against every table in `dev_facts` and `dev_dimensions` reproduces the full table
above directly.

## 5. Hidden partitioning and partition evolution

### The problem it solves

A Hive-style table's partitioning is a directory structure a query has to know about explicitly: the
partition column lives in the file path (`event_date=2026-01-01/`), and a query only benefits from
partition pruning if it filters directly on that literal column, in that literal form. Changing how
such a table is partitioned, switching from daily to monthly buckets, or adding a second bucketing
dimension, means every existing file lives under the wrong directory scheme for the new layout, so
the only way to actually change it is a full rewrite: read every row, recompute its new partition
value, write it out under the new directory structure, and only then can queries benefit from the
new scheme uniformly. Iceberg's hidden partitioning exists specifically to make partitioning
evolvable without that rewrite.

### The mechanism: field ids, not column names or positions

Every partition field in an Iceberg partition spec is defined as a transform applied to a source
column, identified by that column's stable field id, not its name or its ordinal position in the
schema. A spec entry looks like `{"field-id": 1000, "name": "event_at_day", "source-id": 4,
"transform": "day"}`: field id 1000 is the new, synthetic partition field's own identity; `source-id:
4` says it derives from whatever schema field currently holds id 4 (`event_at` in this table,
established back in section 1's worked example); `transform: "day"` says how. Because the source
reference is a field id, not a name, a query never has to write a partition-column predicate at
all: filtering on `event_at` directly (the real underlying column) is enough for the planner to
derive the partition value the filter implies and prune manifests accordingly. That is what "hidden"
means here: the partition column is not a column a user selects, writes to, or even needs to know
exists.

This is the same identity mechanism that makes a plain column rename a metadata-only operation,
demonstrated directly in this project's own evidence: `ALTER TABLE demo_billing_events RENAME
COLUMN status TO txn_status` left field id 5 completely unchanged (`{"id": 5, "name": "status", ...}`
before, `{"id": 5, "name": "txn_status", ...}` after), so every historical data file, written when
the column was still called `status`, stays fully readable under the new name with zero rewrite,
because the manifest entries and the schema both key on field id 5, never on the string `"status"`
or `"txn_status"` (`docs/evidence/schema-evolution/02-rename-column-notes.txt`). A Hive-style table
has no such layer: column identity there is the name (or ordinal position, for older formats) baked
directly into how each file is decoded, so a rename is either a drop-and-recreate that severs the
old data's association with the new name, or a rewrite of every file to update its embedded column
name. Iceberg's rename is a pure metadata edit for the identical structural reason its partition
evolution is a pure metadata edit: both operations only ever touch the id-to-name or id-to-transform
mapping in `metadata.json`, never the data files field ids were assigned to.

### Worked example: this project's own partition-spec evolution, scenario 5

A dedicated table, `demo_billing_events_partitioned`, was built specifically to demonstrate this
from a clean spec history (`docs/evidence/schema-evolution/05-partition-evolution-notes.txt`).
Created with an initial spec:

```sql
CREATE TABLE demo_billing_events_partitioned (event_id varchar, subscriber_id varchar,
  amount integer, event_at timestamp(6), status varchar)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(event_at)']);
```

600 rows were inserted, spanning 2026-01-01 through 2026-01-30, landing under spec 0. Then the spec
itself was changed, on the live table, with no rewrite:

```sql
ALTER TABLE demo_billing_events_partitioned
  SET PROPERTIES partitioning = ARRAY['month(event_at)', 'bucket(subscriber_id, 4)'];
```

400 more rows were inserted, spanning 2026-02-01 through 2026-03-02, landing under the new spec 1.
The raw `metadata.json` afterward (`05-partitioned-final-metadata.json`) carries both specs, kept
permanently side by side, never one overwriting the other:

```json
"partition-specs": [
  {"spec-id": 0, "fields": [
    {"field-id": 1000, "name": "event_at_day", "source-id": 4, "transform": "day"}
  ]},
  {"spec-id": 1, "fields": [
    {"field-id": 1001, "name": "event_at_month", "source-id": 4, "transform": "month"},
    {"field-id": 1002, "name": "subscriber_id_bucket", "source-id": 2, "transform": "bucket[4]"}
  ]}
]
```

`default-spec-id` moved from 0 to 1; spec 0 was never deleted, only stopped being the default for
new writes. `$files`, grouped by which spec each real file was written under, shows the split
exactly:

```
"spec_id":0,"file_count":30,"total_records":600
"spec_id":1,"file_count":8,"total_records":400
```

Thirty spec-0 files, one per day (`event_at_day=2026-01-01/` through `event_at_day=2026-01-30/`,
20 records each), a genuine Hive-style directory layout for that spec:

```
.../data/event_at_day=2026-01-01/20260804_233756_...-94e39465....parquet   (20 records)
```

and eight spec-1 files nested under the new two-level scheme
(`event_at_month=2026-02|03/subscriber_id_bucket=0..3/`):

```
.../data/event_at_month=2026-02/subscriber_id_bucket=0/20260804_233836_...-11291dec....parquet   (81 records)
.../data/event_at_month=2026-03/subscriber_id_bucket=3/20260804_233836_...-81f8eb98....parquet   (7 records)
```

Both physical layouts coexist under the same table location. This is what "evolution" means
concretely, in contrast to a rewrite: none of the thirty January files were moved, renamed, or
reorganized when the spec changed; they simply keep being read under spec 0 forever, while every new
write goes to spec 1. A query with no partition predicate returns the full combined 1,000 rows
correctly (`total_rows: 1000, total_amount: 54440, min_event_at: 2026-01-01, max_event_at:
2026-03-02`), and a range query deliberately straddling the spec boundary, January 25 through
February 5, spanning six days written under spec 0 and five under spec 1, returns correctly grouped
monthly counts pulled transparently from both physical layouts in one query: `{"month":
"2026-01-01", "row_count": 120}` and `{"month": "2026-02-01", "row_count": 69}`
(`05-partition-cross-spec-range-query.json`). The query planner never needed to know two different
physical schemes were involved; it resolved both through the same field-id-keyed mechanism section 1
walked through, once per spec, and unioned the results.

### Why a Hive-style table cannot do this without a rewrite

Under Hive-style partitioning, "the partition column" is a physical directory segment, and there is
exactly one active scheme at a time: every file lives under one interpretation of what the
partition boundaries mean. Changing daily buckets to monthly buckets means every existing daily
directory now disagrees with what the table's own partition metadata says the layout should be;
either old data becomes unreadable under the new scheme, or a full pass rewrites every file into the
new directory structure before the new scheme can be trusted for anything. Iceberg's partition spec
is not a directory-naming convention a reader has to already agree with; it is a per-file record
(inherited from whichever manifest entry lists that file, tagged with its own `spec_id`) of exactly
which transform produced that file's own partition values, so old and new files can disagree about
scheme and still both be read correctly, forever, without ever reconciling to one physical layout.

### When it fails

Hidden partitioning changes what future writes do; it does not retroactively improve pruning on
data already written under the old scheme. A query that would have benefited from the new spec's
finer-grained bucketing still has to fall back to spec 0's coarser day-level pruning for the January
data, exactly as shown in the range-query example above (both months' worth of rows were read
correctly, but the January portion was necessarily pruned only to day-level file boundaries, not the
month-plus-bucket granularity spec 1 offers). Evolving the spec is not a substitute for compacting
old data into the new scheme if uniform pruning granularity across the table's full history actually
matters; it only avoids forcing that rewrite to happen before the new scheme can be used at all.

### How to verify this is actually working

```sql
select spec_id, count(*) as files, sum(record_count) as records
from iceberg.<schema>."<table>$files" group by spec_id;
```

grouped by spec, reproduces the split shown above directly against any table that has undergone a
partition-spec change, and

```sql
select * from iceberg.<schema>."<table>$partitions";
```

against the raw `metadata.json`'s `partition-specs` array confirms every historical spec is still
present, never overwritten, exactly the mechanism this section describes.

## 6. Nessie's snapshot lineage limitation, and how time travel actually works here

### The finding

A red team pass (the same mid-merge kill exercise from section 2, `.notes/decisions.md`, 2026-08-05
part 3) surfaced a real, unplanned discovery while inspecting `fct_billing_transactions`'s history
after two consecutive successful `MERGE`s. Both `"fct_billing_transactions$history"` and
`"...$snapshots"` showed only the single most recent snapshot, `parent_id` empty, even though the
snapshot summary's own `total-data-files` and `total-records` proved a real prior snapshot had
existed and been extended. The raw `metadata.json` files in MinIO confirmed it directly: every
commit writes a fresh `00000-<uuid>.metadata.json`, version number 0, never `00001-`, `00002-`, and
so on, building on the previous file the way a plain Hadoop or Glue catalog's metadata log would.
This project's own committed setup evidence shows the identical pattern from a completely
unrelated table, independently: `docs/evidence/schema-evolution/00-setup-snapshots.json`, captured
after two plain `INSERT ... SELECT` statements with no `MERGE` and no dbt involved at all, records
exactly one row in `$snapshots`, `"parent_id":null`, for a table whose own `total-records: 1000`
summary field proves two real inserts happened.

### Ruling out the wrong explanations

Three hypotheses were on the table before this was root-caused, and all three were checked directly
rather than assumed away. A missing `docker-compose.yml` configuration: ruled out, no config flag
controls this behavior anywhere in Nessie's REST catalog implementation, it is unconditional. Something
specific to `dbt-trino`'s `MERGE` write path: ruled out, the schema-evolution table's plain `INSERT ...
SELECT` produces the identical single-snapshot, no-parent pattern, and the time-travel work
package's own demo table reproduces it again from Nessie's raw commit operations, independent of any
dbt invocation whatsoever. That leaves the third hypothesis, confirmed directly rather than inferred:
this is inherent to how Nessie's own Iceberg REST-catalog bridge materializes `metadata.json` per
commit, a deliberate design choice, not a bug or a gap.

The confirmation is a direct citation, not a guess dressed up as a fact
(`docs/evidence/time-travel/10-root-cause-confirmation.txt`, quoting Nessie's own
`site/docs/guides/iceberg-rest.md`, fetched from the live upstream repository):

> "To retain Nessie's consistency and cross-branch/tag isolation guarantees, we have deliberately
> chosen to only return the state of a table or view as a single snapshot in Iceberg."

This is a real, considered tradeoff for a real property this project's own write-audit-publish
pattern depends on directly: catalog-wide, cross-table atomicity, one branch pointer covering every
table at once (`ADR-001`, `ADR-008`). A catalog that exposed full Iceberg-native snapshot chaining
per table would have to reconcile that chain against Nessie's own commit graph on every read, and the
tradeoff Nessie's maintainers chose instead is to let the commit graph be the single source of truth
for version history, with each table's own `metadata.json` reduced to a single, current-state
snapshot rather than a parallel history-keeping structure of its own.

### What genuinely does not work here

`SELECT ... FOR VERSION AS OF <snapshot-id>` against any table on this catalog does not resolve to
anything before the current commit, because there is nothing before it to find in the table's own
metadata log; `$snapshots` and `$history` never show more than one row for the same reason. Anyone
building a feature on the assumption that Iceberg-native snapshot history works uniformly on every
Iceberg catalog would be wrong here, specifically and structurally, not due to a missing feature flag
this project could flip on.

### What was actually built instead: Nessie-native time travel and rollback

Rather than force Iceberg-native snapshot time travel to work on a catalog that structurally cannot
provide it, this project implemented time travel and rollback against Nessie's own commit graph and
branch-pointer mechanics directly (`ops/nessie.py`'s `get_log` and `assign_reference`, `ADR-008`),
demonstrated against a dedicated branch, `time_travel_demo`, cut from `main` and never merged back
or reset on `main` itself (a branch reset moves the whole branch's pointer, not one table's key, so
resetting `main` to undo one bad demo commit would revert every other real table cataloged there
along with it).

Three real hashes anchor the whole demonstration
(`docs/evidence/time-travel/01-commit-hashes.txt`): `main`'s HEAD at branch creation
(`2da700d70f1e...`), `hash_good` after a 5-row good batch (`e90564dbd80c...`), and `hash_bad` after
a 3-row bad batch with two negative amounts, landing the table at 8 rows total
(`8f525054bf9d...`). Nessie's REST bridge accepts a checked reference, a branch name and a specific
commit hash joined by `@`, as a first-class value anywhere a plain branch name is accepted,
including inside the Iceberg REST catalog's own URL path, confirmed live against all three
documented forms (`branch@hash`, a bare hash, and `branch#timestamp`). Because `ops/wap.py`'s own
catalog-registration function already builds its `iceberg.rest-catalog.uri` from an arbitrary branch
name with no assumption about its contents, pointing a Trino catalog at
`time_travel_demo@e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2` required no new
catalog-registration code, only a different string handed to the same function.

**Point-in-time query.** A Trino catalog registered against that exact checked reference returns
exactly the 5 good rows, none of the 3 bad-batch rows (`02-asof-good-query.json`):

```json
{
  "ref": "time_travel_demo@e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2",
  "rows": [
    {"batch_id": "batch-001", "subscriber_id": "sub-001", "amount_usd": "14.99", ...},
    ...
  ]
}
```

five rows total. Querying the same table through the ordinary, no-hash, live branch catalog at the
same moment returns all 8 rows, the 2 negative amounts included
(`03-live-query-before-rollback.json`), proving the bad batch is genuinely live on the branch, not
merely present somewhere in its history.

**Rollback.** A genuinely different Nessie operation from a merge, not a variant of it: `PUT
.../trees/time_travel_demo@<hash_bad>` with body `{"type": "BRANCH", "name": "time_travel_demo",
"hash": <hash_good>}` moves the branch pointer directly, including backward, with no ancestor
relationship required between the two hashes (this is the same compare-and-swap shape from section
2, applied to move a pointer to an *earlier* state rather than a later one). The response confirms
the new HEAD (`05-rollback-assign-response.json`):

```json
{
  "ref_name": "time_travel_demo",
  "expected_hash_before_reset": "8f525054bf9d3c93ced40857190b83296f1460ce73f3ebccd873eff02e06b9ab",
  "target_hash": "e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2",
  "reference_after_reset": {"type": "BRANCH", "name": "time_travel_demo", "hash": "e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2"}
}
```

Querying the identical live, no-hash-in-the-URI catalog again immediately afterward now returns 5
rows, no negative amounts (`06-live-query-after-rollback.json`): this is what proves the rollback is
real rather than merely a different historical read, the exact same catalog that showed 8 rows
before now shows 5, because the branch's own HEAD moved. The commit log confirms `hash_bad` is no
longer reachable from the branch's HEAD (`07-nessie-history-after-rollback.json` lists only the
create and good-insert commits), not because the bad commit was deleted, only because history walks
parent pointers backward from HEAD and `hash_bad` is no longer an ancestor of anything reachable from
it; the commit still exists in Nessie's own storage until a garbage-collection pass reclaims it.

Even Nessie's own history log for this table shows the identical no-parent-chain pattern from the
Iceberg side, reproduced a second time independently of `fct_billing_transactions`, on a plain
`INSERT`, no `dbt` involved at all
(`docs/evidence/time-travel/04-nessie-history-before-rollback.json`): each of the three commits
(create, good insert, bad insert) points at its own distinct `00000-<uuid>.metadata.json`, none
carrying a previous-metadata-log entry pointing at the one before it. Nessie's own commit graph, not
that log, is what makes the rollback above coherent at all.

### Retention: what actually governs storage growth on this catalog

Because each table only ever has one live Iceberg snapshot to find here, Iceberg's own
`expire_snapshots` procedure has nothing meaningful to expire per table under this catalog's design.
Nessie itself never auto-expires anything, commits or the files they reference; the real tool for
bounding storage growth is a separate program, `nessie-gc`, not part of this stack's
`docker-compose.yml` today, doing mark-and-sweep of orphaned data files against a per-reference
cutoff policy. A policy was chosen and documented (`docs/evidence/time-travel/09-retention-policy-
notes.txt`, `ADR-008`): `main=P14D`, `wap_.*=P3D`, `time_travel_demo=P7D`, `default-cutoff=P30D` as
a backstop, all time-based rather than commit-count-based given this project's bursty, uneven commit
cadence. This is a documented, justified policy and the literal command that would enforce it, not
an action actually taken against the live stack; storage usage should be spot-checked by hand until
a scheduler for it exists.

### How to verify this is actually working

Confirm the limitation itself against any real table:

```sql
select * from iceberg.dev_facts."fct_billing_transactions$snapshots";
-- returns exactly one row, parent_id null, regardless of how many MERGEs actually ran
```

Reproduce the working alternative directly against the live stack with
`uv run python -m ops.time_travel_demo`, or by hand: read a table's current Nessie commit hash from
`GET /api/v2/trees/main`, register a one-off Trino catalog whose
`iceberg.rest-catalog.uri` ends in `/iceberg/<branch>@<hash>` instead of `/iceberg/<branch>`, and
query through it; the result is that table's exact content as of that commit, Nessie-native time
travel standing in for the Iceberg-native mechanism this catalog cannot provide.
