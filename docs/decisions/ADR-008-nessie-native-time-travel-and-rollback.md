# ADR-008: Nessie-native time travel and rollback, not Iceberg-native snapshot chaining

## Status

Accepted, 2026-08-05.

## Context

A red team review (2026-08-05, part 3) tried to prove Iceberg-native time travel and rollback work
on this stack, by killing a live Trino MERGE query mid-write against `fct_billing_transactions` and
inspecting the table's snapshot history afterward. The atomicity proof succeeded (a killed query
left zero trace on a committed read), but it surfaced a real, load-bearing fact about this project's
catalog choice (ADR-001): Nessie's REST catalog does not preserve Iceberg-native snapshot lineage
across commits the way a plain Hadoop or Glue catalog would. After two consecutive successful
MERGEs, `"fct_billing_transactions$history"` and `"...$snapshots"` both showed only the single most
recent snapshot, `parent_id` empty; the raw `metadata.json` files in MinIO confirmed every commit
writes a fresh `00000-<uuid>.metadata.json` (version-number 0), never chaining to a previous one.
`FOR VERSION AS OF` and querying `$snapshots`/`$history` for anything before the current commit do
not work on this catalog.

A follow-up work package confirmed the root cause directly, rather than guessing: Nessie's own
`iceberg-rest.md` guide states plainly that the REST bridge deliberately exposes only the single
snapshot matching the current commit, to preserve catalog-wide cross-table consistency guarantees.
This is a documented design tradeoff, not a bug, a missing config flag, or something specific to
dbt's write path (independently ruled out: a plain Trino `INSERT` with no dbt involved produces the
identical pattern).

## Decision

Implement time travel and rollback against Nessie's own commit log and branch-pointer mechanics
(`ops/nessie.py`'s `get_log` and `assign_reference`), not against Iceberg-native snapshot chaining.

Point-in-time query works because Nessie's Iceberg REST bridge accepts a checked reference
(`<branch>@<commit-hash>`, or a bare hash, or `<branch>#<ISO-8601-timestamp>`) as a first-class
value in its URL path, everywhere a plain branch name is accepted, confirmed live against all three
forms. Because `ops/wap.py`'s existing catalog-registration function already builds its Trino
catalog's `iceberg.rest-catalog.uri` from an arbitrary branch-name string with no assumption about
its contents, passing it a string that already includes `@<hash>` works with no new
catalog-registration code, just a different string handed to the same function.

Rollback is a genuinely different Nessie operation from merge: `PUT
/api/v2/trees/{branch}@{expected-current-hash}` with body `{"type": "BRANCH", "name": <branch>,
"hash": <target-hash>}` moves the branch pointer directly, including backward, with no ancestor
relationship required between the two hashes. This was confirmed live: the working request body is
flat (`{type, name, hash}`), not the nested `{"assignTo": {...}}` shape a first guess, by analogy
with the Nessie Java client's naming, assumed and had rejected by the live server.

The entire demonstration, including the rollback, runs on a dedicated branch cut from main
(`time_travel_demo`), never on main itself: a branch reset moves the whole branch's pointer, not one
table's key, so resetting main to undo a demo's bad commit would revert every other table cataloged
on main along with it.

## Alternatives Considered

- **Iceberg-native snapshot chaining** (`$snapshots`, `$history`, `FOR VERSION AS OF` against the
  table's own metadata log). This was the first approach tried, per the work package's own framing,
  and is the mechanism most tooling assumes will just work on any Iceberg catalog. Ruled out, not by
  choice but by direct confirmation that it does not function on this catalog: Nessie's REST bridge
  never exposes more than the single current snapshot, a deliberate, documented tradeoff for
  catalog-wide atomicity, not a configuration gap this project could close.
- **Running the demonstration, including rollback, directly on main.** This is what the work
  package's own brief literally asked for. Deviated from deliberately: a branch-wide reset on main
  would revert every other table cataloged there (`dev_dimensions`, `dev_facts`, `dev_silver`, every
  WAP bookkeeping commit) to the same point, which is exactly the destructive-operation-against-real-
  tables risk this project's other work packages are explicitly constrained against. Running on a
  dedicated branch instead is consistent with how `ops/wap.py` already treats main (only ever
  fast-forward merged into, never reset).

## Consequences

- Anyone building a future feature on the assumption that Iceberg-native snapshot history works on
  this catalog will be wrong; this is now a documented, root-caused fact
  (`docs/evidence/time-travel/10-root-cause-confirmation.txt`), not an open question.
- Nessie never automatically expires anything, commits or the data files a commit references. The
  real risk this creates is MinIO storage growth, not Postgres growth (each commit row is small):
  every superseded data file, manifest, and metadata.json persists in `s3://warehouse/...` for as
  long as any named reference's history can still reach it, and Iceberg's own `expire_snapshots`
  procedure has nothing meaningful to expire per table under this catalog's single-snapshot-per-
  commit design.
- A retention policy was chosen to bound that growth, using `nessie-gc` (a separate tool, not part
  of this project's `docker-compose.yml` today) with per-reference cutoffs: `main=P14D` (covers a
  normal active-development window; nothing in the project plan needs auditing further back),
  `wap_.*=P3D` (only matters for failed WAP branches, kept by default for human inspection; three
  days is enough time for that before their files become sweep-eligible), `time_travel_demo=P7D`
  (bounds the side effect of a demo branch pinning a slice of main's own historical files live, since
  branch creation adds a pointer, not a copy), and `default-cutoff=P30D` as a backstop for any
  reference this policy did not anticipate. This is a documented, not-yet-scheduled policy: the
  literal `nessie-gc` invocation is recorded, but no scheduler runs it, and MinIO usage should be
  spot-checked by hand until one exists.
- The demo branch and its table were kept in place after the run, rather than dropped, matching the
  schema-evolution work package's own precedent (ADR-011): the footprint is trivial and the evidence
  references live query results a later reviewer may want to re-run directly.
