# Time travel and snapshot expiry

Time travel and retention are two sides of one tradeoff: the ability to query a table as it existed
in the past exists only because old data and metadata are not deleted the instant a new snapshot
commits, and every day that ability is preserved is a day of storage this project's catalog is
carrying that it is not actively using. This document covers the theory of why time travel is
possible at all, then this project's own real, hard-won story on top of it: the mechanism the theory
predicts does not work through this catalog's Trino path, why, and what was built instead, using this
project's own real commit hashes and query results as proof, not narration. It closes with the
retention math and the actual policy this project chose, and is honest about what part of that policy
is enforced today and what part is a documented intention.

## The theory: why querying the past is possible at all

An Iceberg table's current state is a pointer, `current-snapshot-id`, into an array of snapshots the
table's `metadata.json` keeps. Each snapshot points at a manifest list, which points at manifests,
which point at data files. Every write, an insert, an update, a delete, produces a brand-new snapshot
alongside the ones already there; it does not edit any existing snapshot, manifest, or data file in
place. Committing a new snapshot is exactly one atomic operation: swap which snapshot id
`current-snapshot-id` points at. Nothing about that swap requires touching, let alone deleting, what
the previous pointer referenced.

This is what makes time travel possible as a matter of first principles, not a special feature bolted
on top: as long as an old snapshot's manifest list and every file it transitively references still
exist in storage, that snapshot describes a complete, internally consistent, queryable view of the
table exactly as it was at that commit, indefinitely, regardless of how many newer snapshots have
since been created. Querying "as of" a snapshot id or a timestamp is just resolving to a different
entry in that same array instead of the current one, then reading forward from there the same way any
query reads from the current pointer. Nothing about the mechanism changes; only which snapshot the
read starts from does.

The corollary is the retention half of this document's title: this only stays cheap and correct until
something actually deletes an old snapshot's files. Iceberg does not do that automatically. The
standard mechanism (`expire_snapshots`, run manually or on a schedule) walks a table's snapshot
history, decides which snapshots are no longer reachable from any retained tag or branch reference,
and only then removes their now-orphaned manifests and data files. Until that procedure runs, every
snapshot a table has ever produced, and every file any of them reference, sits in storage doing
nothing but costing space. Trino's Iceberg connector exposes this theory directly as `SELECT ... FOR
VERSION AS OF <snapshot-id>` and `FOR TIMESTAMP AS OF <timestamp>`, resolving against the table's own
`metadata.json` snapshot log. That is the mechanism the rest of this document is about, because it is
not the mechanism that actually works on this project's catalog.

## This project's story: the native mechanism does not work here

The gap was found during a red team review of the already-built stack (`.notes/open-questions.md`,
2026-08-05 red team pass, part 3), not designed around from the start. `SELECT ... FOR VERSION AS OF`,
`$snapshots`, and `$history` against this project's real tables never showed more than a single
current commit, even on tables with an obviously real prior history (a table extended by two separate
`INSERT` statements shows one row in `$snapshots`, `parent_id` null, not two chained snapshots). At
the time, three explanations were open: something inherent to how this catalog's Nessie REST bridge
materializes `metadata.json`, a missing `docker-compose.yml` configuration, or something specific to
`dbt-trino`'s `MERGE`-based write path.

All three were resolved with direct evidence rather than left as a guess, and the review's original
hypothesis turned out to be exactly right. `docs/evidence/time-travel/10-root-cause-confirmation.txt`
quotes Nessie's own `iceberg-rest.md` guide directly:

> "To retain Nessie's consistency and cross-branch/tag isolation guarantees, we have deliberately
> chosen to only return the state of a table or view as a *single* snapshot in Iceberg."

That single sentence resolves all three hypotheses at once. It is inherent to the REST bridge, by
design: **confirmed**. It is not a missing configuration flag: **ruled out**, no setting in Nessie's
REST catalog implementation controls this behavior, it is unconditional. It is not specific to
`dbt-trino`'s `MERGE` write path: **ruled out**, this project confirmed the identical no-parent-chain
pattern from a plain Trino `INSERT` with no `dbt` involved at all
(`docs/evidence/time-travel/04-nessie-history-before-rollback.json`, three separate commits, each
pointing at its own distinct `00000-<uuid>.metadata.json` with no previous-metadata-log entry), and
independently, the already-committed schema-evolution evidence
(`docs/evidence/schema-evolution/00-setup-snapshots.json`) shows the same thing from two plain
`INSERT ... SELECT` statements. Nessie's own tradeoff here is real and deliberate: it tracks every
table and view's version history in its own commit graph, atomically, across every table in a
catalog at once, which is exactly what gives Nessie's catalog-wide branch merges (this project's
write-audit-publish mechanism) their consistency guarantee. Exposing Iceberg-native multi-snapshot
lineage through the REST bridge on top of that would mean two separate, potentially disagreeing
sources of truth for "what version is this table at": Nessie's commit graph, and Iceberg's own
snapshot log. Nessie chose to expose only one.

## The real mechanism: Nessie's own checked references

The same Nessie guide documents the actual replacement, and this project verified it live rather than
trusting the documentation alone. Nessie's REST API, including inside the Iceberg REST catalog bridge
it exposes to Trino, accepts a **checked reference** anywhere a plain branch name is accepted: a
branch name, optionally followed by `@<commit-hash>` for a specific commit, or `#<ISO-8601-timestamp>`
for a point in time. Three forms were confirmed against the live server
(`docs/evidence/time-travel/10-root-cause-confirmation.txt`):

- `<branch>@<hash>`: `GET /iceberg/time_travel_demo@<hash>/v1/config` returns 200, hash echoed back.
- a bare `<hash>`, no branch name: `GET /iceberg/<hash>/v1/config` also returns 200.
- `<branch>#<timestamp>`: `GET /iceberg/time_travel_demo%232026-08-05T04%3A33%3A06.50Z/v1/config`
  (URL-encoded `#`) also returns 200, syntactically confirmed live though not exercised end to end
  with a full query in this demonstration, since `@hash` already proves the same underlying mechanism.

The practical consequence is that no new code was needed to use this. `ops/wap.py`'s
`_create_catalog` already registers a Trino catalog by handing Nessie's Iceberg REST bridge a branch
name string, with no assumption about what that string contains; passing it a string that already
includes `@<hash>` works unmodified. `ops/time_travel_demo.py` and two new functions on
`ops/nessie.py` (`get_log`, reading `GET /api/v2/trees/{ref}/history`, and `assign_reference`, the
rollback mechanism below) built the demonstration on that existing plumbing.

### The real evidence: point-in-time query

The whole demonstration runs on a dedicated Nessie branch, `time_travel_demo`, cut from `main` and
never merged back, specifically because a branch reset is branch-wide, not scoped to one table:
resetting `main` itself to undo one bad demo commit would revert every other real table cataloged on
`main` back to that same point. The three commit hashes that matter
(`docs/evidence/time-travel/01-commit-hashes.txt`):

```
main HEAD at branch creation: 2da700d70f1ec8d5b94bc00145230186f91f11b414eb8e31a204631f014377d4
hash_good (after the 5-row good batch): e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2
hash_bad  (after the 3-row bad batch, 2 of 3 rows with negative amount_usd): 8f525054bf9d3c93ced40857190b83296f1460ce73f3ebccd873eff02e06b9ab
```

A Trino catalog registered with its `iceberg.rest-catalog.uri` pointed at
`time_travel_demo@e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2` and queried
(`docs/evidence/time-travel/02-asof-good-query.json`) returns exactly the 5 good-batch rows, none of
the 3 bad-batch rows: `sub-001` through `sub-005`, amounts `14.99`, `9.99`, `24.99`, `14.99`, `9.99`.
The same table queried through the ordinary, no-hash branch-scoped catalog at that same moment
(`03-live-query-before-rollback.json`) returns all 8 rows, including `sub-007` at `-14.99` and
`sub-008` at `-9.99`, confirming the bad batch really is live on the branch, not merely present
somewhere in history. This is the direct proof that Nessie-native point-in-time query genuinely
works, even though `FOR VERSION AS OF` does not.

### The real evidence: rollback

Rollback and point-in-time query are two different Nessie operations, not variants of one mechanism.
Nessie calls the rollback operation "assign" (the CLI calls it `nessie branch --force`, git calls the
equivalent `reset --hard`): `assign_reference` issues `PUT /api/v2/trees/{branch}@{expected_hash}`
with body `{"type": "BRANCH", "name": <branch>, "hash": <target_hash>}`, moving the branch pointer
directly, with no ancestor relationship required between the two hashes. The real call
(`docs/evidence/time-travel/05-rollback-assign-response.json`):

```json
{
  "ref_name": "time_travel_demo",
  "expected_hash_before_reset": "8f525054bf9d3c93ced40857190b83296f1460ce73f3ebccd873eff02e06b9ab",
  "target_hash": "e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2",
  "reference_after_reset": {"type": "BRANCH", "name": "time_travel_demo",
                             "hash": "e90564dbd80cf94a1661d5b0430f08ca6e3f67bdb615b7588734a247a7d86ee2"}
}
```

The same ordinary, no-hash catalog that returned 8 rows in `03-live-query-before-rollback.json`,
queried again immediately after the reset (`06-live-query-after-rollback.json`), now returns exactly
the 5 good rows, no negative amounts. This is what proves the rollback is real and not just a second
historical read: the same catalog, same connection pattern, no hash in the URI at all, changed its
answer because the branch's own HEAD moved. The commit log confirms the mechanism precisely: before
the reset, `04-nessie-history-before-rollback.json` lists 3 entries for this table (create, good
insert, bad insert); after, `07-nessie-history-after-rollback.json` lists 2 (create, good insert).
`hash_bad` is not deleted, it is unreachable: history walks parent pointers backward from the current
HEAD, and once HEAD moved to `hash_good`, `hash_bad` is no longer an ancestor of anything reachable
from it. The commit itself still exists as a row in Nessie's own store until a separate reclaim pass
runs, which is exactly the retention question below. `docs/evidence/time-travel/08-summary.txt`
records the demonstration script's own pass/fail check on the live run this evidence was captured
from: point-in-time query PASS, live-before PASS, live-after PASS, `hash_bad` reachable from HEAD
after rollback: `False` (expected `False`).

## The math: what unbounded retention would cost

**The following is a labeled estimate**, not a measurement, because this project deliberately never
lets unbounded retention run long enough to measure: the whole point of the policy below is that it
was set short specifically because nothing here auto-expires. Two things grow without bound if
nothing ever sweeps them: the count of `metadata.json` objects per table, and the data files those
old commits' snapshots still reference.

On the `metadata.json` count: because this catalog materializes a distinct, standalone
`metadata.json` per commit rather than appending to one growing snapshot log (the same no-parent-chain
pattern documented above), every commit against a table is a wholly new file, never an update to an
existing one. This project's own retention investigation observed directly that ordinary `dbt`
activity produces several commits per single `dbt run` invocation, not one: a live check during that
investigation showed `dbt_invocations`, `dbt_run_results`, and `elementary_test_results` each getting
their own separate commit around one ordinary run (`docs/evidence/time-travel/09-retention-policy-notes.txt`).
Take `fct_playback_events`, this project's dominant-volume real table, and a conservative floor of one
commit per day for that table specifically (a daily incremental `MERGE` run, ignoring the additional
bookkeeping-table commits a real `dbt run` generates alongside it): 365 additional standalone
`metadata.json` objects a year, for this one table alone. The demo partitioned table's own final
`metadata.json` (two partition specs, one schema, one snapshot) is 3,298 bytes; a wider production
table's commit carries a bigger schema and its own statistics blob, so call it on the order of a few
kilobytes per commit as a rough anchor, not a measurement. That puts a year of `metadata.json`
accumulation for one table on the order of one to a few megabytes, trivial by itself.

The `metadata.json` count is not the real risk, and this project's own retention notes say so
directly: "Postgres growth from the commit graph alone is not the laptop-storage risk this policy
exists to bound." The real risk is that every one of those 365-plus commits' **data files** also
persists in `s3://warehouse/...` for as long as any reference's history can still reach them, and
nothing deletes them automatically. A table rebuilt or incrementally merged repeatedly during normal
development leaves an ever-growing trail of superseded files behind it, and this catalog's own
single-snapshot-per-commit design means Iceberg's usual lever for this, `expire_snapshots`, has
essentially nothing to expire per table, since there is only ever one live snapshot to find in the
first place. It is not a usable tool for the concern it normally addresses on this catalog.

## The actual mechanism, and this project's chosen policy

The real tool is a separate program, `nessie-gc` (its own JAR or Docker image, not part of this
project's `docker-compose.yml` today), doing two-phase mark-and-sweep: **mark** walks every live named
reference, applies a cutoff policy to decide which of its commits still count as live, and records
every table/view version those live commits point at; **sweep**, for each table, maps those live
content references to actual data and manifest files using Iceberg's own metadata, then deletes files
in the table's location that are not in that live set. It deletes orphaned *files* only, never commits:
an unreachable commit like `hash_bad` above still exists as a row in Nessie's store; `nessie-gc`'s
mark phase simply never marks its content as live, so its data files become sweep-eligible once the
cutoff policy for that reference makes it so. The commit graph itself is never pruned by this tool at
all; the only thing that can do that is a separate, explicitly advanced and effectively irreversible
`nessie-server-admin-tool` command not exercised or needed by this project's commit volume.

`nessie-gc`'s cutoff can be a commit count, a relative duration, or an absolute timestamp, settable
per reference by regex pattern. Its own documented default, if no cutoff is given, is **none**: every
commit ever made is treated as live and nothing is ever swept, which is the wrong default for a
project whose explicit constraint is that laptop storage must not grow unbounded. The policy this
project chose instead sets an explicit cutoff everywhere, per reference class
(`docs/evidence/time-travel/09-retention-policy-notes.txt`):

- **`main=P14D`** (two weeks). `main` is the only branch real downstream consumers (`dbt`, Trino's
  static `iceberg` catalog, any future BI tool) ever read from. Two weeks comfortably covers a normal
  active-development iteration window, this project's own commit cadence runs several commits per
  `dbt run`, so even a single day of active work generates meaningful history, without ever needing to
  time-travel further back than that in ordinary use.
- **`wap_.*=P3D`** (three days). `ops/wap.py` already deletes a successful run's branch immediately
  after merge, so this cutoff only actually governs failed branches, which `ops/wap.py` keeps by
  default specifically so a human can inspect what went wrong. Three days is enough time for that
  inspection, short enough that an unreviewed failed run does not pin its files indefinitely.
- **`time_travel_demo=P7D`** (one week), and the pattern any future demo branch should follow. Worth
  stating precisely: as long as a demo branch exists, it also holds live every real commit on `main`'s
  history up to the point it was cut from, since branch creation adds a second name pointing at the
  same ancestry rather than copying anything. An uncapped demo branch would quietly pin an entire slice
  of `main`'s own historical files live forever, purely as a side effect of having been cut from it.
- **`default-cutoff=P30D`**, a conservative backstop for anything unmatched: a one-off scratch branch
  created and forgotten (this investigation itself created and manually deleted one,
  `tt_scratch_probe`, but the backstop exists for the case where that deletion is forgotten). Chosen
  deliberately over `nessie-gc`'s own `NONE` default, since an explicit backstop is the only way to
  guarantee bounded storage for reference patterns this policy did not anticipate.

All four are time-based rather than commit-count-based, matching this project's own observed commit
pattern: bursty and uneven (several bookkeeping commits per single `dbt run`), where a count-based
cutoff would be hard to reason about consistently across reference classes with very different commit
rates.

The command a scheduled job would actually invoke, once `nessie-gc` is added to this stack (it is a
separate artifact, not bundled in the `nessie:0.108.4-java` image this project runs today):

```
java -jar nessie-gc.jar gc \
  --uri http://nessie:19120/api/v1 \
  --default-cutoff P30D \
  --cutoff 'main=P14D' \
  --cutoff 'wap_.*=P3D' \
  --cutoff 'time_travel_demo=P7D'
```

## Where this stands, honestly

This is a documented, justified policy and the literal command that would enforce it, not an executed
action. Building the scheduler that would actually run it on a cadence was explicitly out of scope for
the work package that produced this policy. Dagster is already in this stack, but wired for the `dbt`
DAG, not infra housekeeping; a weekly cron entry or a Dagster job pointed at the command above is what
would close this gap, and neither exists yet. Until one does, this policy is an intention this project
has committed to, not a guarantee: MinIO usage should be spot-checked by hand occasionally rather than
assumed bounded, and every table on this catalog is, right now, retaining every file every commit it
has ever made has produced.

## Limitations worth stating precisely

**Point-in-time query is branch-wide, not a single-query clause.** Unlike `FOR VERSION AS OF
<snapshot-id>` inside one SQL statement, this project's real mechanism requires registering an entire
Trino catalog against a specific ref string before any query against it runs. There is no way to say
"just this one query, as of this hash" without that registration step; every query against that
catalog for the life of the registration reads as of that same fixed point.

**Rollback moves an entire branch, not one table.** A Nessie `assign` call resets every table
cataloged on that branch back to the target hash simultaneously; there is no table-scoped rollback.
This is exactly why this project's own demonstration deliberately never runs on `main`, and why any
real production rollback needs to be scoped to a branch that holds only what actually needs reverting.

**An unreachable commit is not a deleted commit.** Rollback makes bad data unreachable from a branch's
current read path immediately, which is the property that matters for correctness, but the underlying
commit and, until a GC sweep actually runs, its data files still exist in storage. Anyone auditing
storage usage or doing compliance-driven deletion needs to know the difference between "no longer
readable through normal query paths" and "actually gone."

**The retention policy above is not enforced today.** It exists as a documented cutoff schedule and a
literal command, not a running job. Until a scheduler is added, every table on this catalog
accumulates snapshots and files with no expiry at all, regardless of what the policy says should
happen.

## How to verify this is actually working

Every claim above is reproducible against the live stack. `ops/time_travel_demo.py` (`uv run python -m
ops.time_travel_demo`) rebuilds `time_travel_demo` fresh off `main`'s current HEAD each run
(idempotent by design) and re-captures all of `docs/evidence/time-travel/01` through `08`. To confirm
the root cause independently rather than trust the citation, `GET
{nessie_uri}/api/v1/config` against a running Nessie 0.108.4 server, or reading
`site/docs/guides/iceberg-rest.md` directly from `projectnessie/nessie`'s repository, both surface the
same single-snapshot statement quoted above. To confirm the checked-reference mechanism directly:

```
GET /iceberg/<branch>@<hash>/v1/config   -> 200, hash echoed in the resolved prefix
GET /iceberg/<hash>/v1/config            -> 200, bare hash also resolves
GET /iceberg/<branch>%23<iso-timestamp>/v1/config  -> 200, URL-encoded '#'
```

To see rollback's reachability guarantee directly rather than take the evidence file's word for it,
call `ops.nessie.get_log(nessie_uri, "time_travel_demo")` before and after an `assign_reference` call
and confirm the target commit's hash disappears from the returned list, exactly as
`07-nessie-history-after-rollback.json` shows. To check current, unenforced retention exposure by
hand: list every branch and tag on the live Nessie server, and for any table of interest, compare its
`metadata.json` object count in MinIO against how many commits actually touched it; the two should be
equal, since this catalog produces exactly one standalone `metadata.json` per commit, and neither
count should be shrinking on its own.
