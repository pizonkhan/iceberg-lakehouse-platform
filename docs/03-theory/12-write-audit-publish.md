# Write-audit-publish

Write-audit-publish (WAP) is a load pattern with a specific structural property: bad data is
never visible to a consumer, not even briefly. This document states that property precisely,
walks through this project's actual implementation (`ops/wap.py`, `ops/nessie.py`), reproduces
the real evidence captured against the live stack, and covers a real incident where the WAP CI
workflow's own teardown step destroyed this project's local warehouse, an operational lesson
about validation-harness blast radius that is distinct from, and does not undermine, the
correctness argument WAP itself makes.

## The problem: post-hoc checking has a window

The alternative to WAP is post-hoc checking: load the data, then validate it, then alert or
roll back if something is wrong. This is the default shape of most data quality tooling
(`dbt test` run after `dbt run` against the same table consumers already query, a monitoring
job that scans a table on a schedule, a data contract check that fires after a write commits).
It is also, structurally, always wrong in one specific way: there exists a real interval of
wall-clock time, however small, during which the bad data is live in the table consumers read
from, and the validation that would have caught it has not run yet or has not finished running.

Call that interval the exposure window. It exists for a structural reason, not an implementation
oversight: post-hoc checking validates a state that has already been published. Publication and
validation are two separate events in time, and any two separate events have a nonzero interval
between them, no matter how fast the validator runs. During that interval, the data is
indistinguishable, from a consumer's point of view, from good data. A dashboard can render it. A
downstream job can join against it. A person can look at a number derived from it and make a
decision. None of those readers know to wait for the audit to finish, because from where they
sit, the load already finished.

This matters because a consumer's read is not undone by a later rollback. If a scheduled report
executes inside the exposure window and a bad row is included in an aggregate, rolling the table
back afterward does not roll back the report, the decision made from it, or anything downstream
of that decision. The correctness violation is not "the table was briefly wrong." It is "the
table was briefly wrong while it was reachable, and reachability is what makes a violation able
to propagate into something outside the system's own control." A monitoring job that fires an
alert five minutes after a bad load is not a correctness mechanism. It is a damage-control
mechanism operating after the fact, and its speed only bounds the size of the exposure window,
it does not close it.

## The mechanism: branch-based validation has no window

Write-audit-publish closes the window by changing what "publish" means. Instead of writing
directly to the branch consumers read (call it `main`), the write happens on an isolated branch
that consumers have no path to reach. The audit runs against that branch. Only if the audit
passes does the branch merge into `main`, and that merge is the only point at which the data
becomes visible to anyone reading through `main`. If the audit fails, the branch is simply never
merged, and nothing about `main` ever changes.

The correctness argument is not "this makes bad data less likely to be seen." It is stronger
than that: for a consumer who only ever reads through `main` (which is every consumer, since
`main` is the branch this project's ordinary `iceberg` catalog resolves to), there is no
sequence of events that leads to that consumer seeing the bad data, because the bad data was
never written to any state reachable from `main` in the first place. Reachability, not
transience, is the property doing the work. A post-hoc check makes the bad window small. WAP
makes it not exist, because there is no commit on `main`'s history that ever contained the
violation. This is a difference in kind, not degree: no audit speed, however fast, closes a
window that branch isolation removes structurally.

## Walking through `ops/wap.py`

`ops/wap.py`'s docstring states the stages plainly:

```
Stages, in order (write / audit / publish):
    0. give main one real commit if it has none yet ...
    1. create a Nessie branch off main, named <branch-prefix>_<run-id>.
    2. register a Trino catalog scoped to that branch ...
    3. `dbt run --select <scope>` against that catalog (write).
    4. `dbt test --select <scope>` against what was just built (audit, the quality gate).
    5. if both passed: merge the branch into main via the Nessie REST API (publish),
       then delete the branch.
    6. if anything failed: main is never touched. The branch is left in place for
       inspection by default (--no-keep-failed-branch deletes it instead).
```

### Branch creation

`run_wap` reads `main`'s current commit hash and cuts a new branch from it:

```python
main_ref = nessie.get_reference(config.nessie_uri, "main")
...
branch_ref = nessie.create_branch(config.nessie_uri, branch_name, main_ref)
```

`nessie.create_branch` is a plain call against Nessie's v2 REST API (`ops/nessie.py`), and its
docstring records a real, non-obvious detail about the request shape: the new branch's name goes
in the query string, but the *source* ref's name and hash go in the body, an asymmetry confirmed
directly against the running server, not assumed from Nessie's docs (a body shaped after the new
branch was rejected outright: `"missing type id property 'type'"`). This is a catalog-wide
branch, the same kind of operation `git branch` performs on a whole repository: every table and
namespace under the Nessie catalog is branched at once, not one table at a time. That is the
property Nessie was chosen for over Polaris, whose branching is limited to per-table snapshot
refs (`.notes/decisions.md`, 2026-08-03) and cannot express "isolate an entire multi-table build."

The branch name itself, `wap_<run-id>`, is unique per invocation
(`f"{run_started:%Y%m%dt%H%M%Sz}_{uuid.uuid4().hex[:8]}"`), so concurrent WAP runs never collide
on a branch name or a Trino catalog name.

### Why Trino needs a distinct catalog per branch, not a session property

The write and audit stages have to run against the branch, not `main`, or there is nothing to
isolate. The natural first guess is a session-level parameter: something like
`SET ref = 'wap_20260805t005001z_8664cb4f'`, the way a session can `SET SCHEMA`. Trino's Iceberg
REST connector has no such lever. This project confirmed it directly rather than assuming it
from documentation gaps: Trino's `iceberg.catalog.type=rest` connector exposes
`iceberg.rest-catalog.uri` (and the handful of related REST-catalog properties) only as
catalog-registration-time configuration, fixed for the life of that catalog object, with no
session-level or per-query equivalent. `.notes/decisions.md`'s 2026-08-05 entry records the
confirmation: Trino 483's own documentation, cross-checked against a filed upstream issue
(Trino GitHub #24134) that asks for exactly this as a still-unimplemented feature request.

The reason a session property cannot work is where the branch actually lives in the request.
Nessie's REST catalog protocol encodes the branch as a path segment, not a query parameter or a
header Trino could vary per session: querying Nessie's own `/iceberg/v1/config` shows
`"nessie.prefix-pattern": "{ref}|{warehouse}"`, meaning the ref name is baked into the base URL a
catalog object was registered with (`http://nessie:19120/iceberg/<branch>`). A Trino catalog is,
from the connector's point of view, a fixed binding to one such URL. Changing which branch a
query sees means changing which URL the catalog points at, and Trino's static catalog store
(`catalog.management=static`, the default) makes catalog properties immutable after startup, a
`CREATE CATALOG` against it was tried directly and correctly refused: "not supported by the
static catalog store" (ADR-009). Given no session-level lever exists and catalog properties are
otherwise fixed, the only mechanism left is a separate catalog registration per branch, which is
exactly what `_create_catalog` does:

```python
def _create_catalog(config: WapConfig, catalog_name: str, branch_name: str) -> None:
    branch_uri = f"{TRINO_INTERNAL_NESSIE_URI}/iceberg/{branch_name}"
    sql = f"""
        CREATE CATALOG {catalog_name} USING iceberg WITH (
          "iceberg.catalog.type" = {_sql_literal("rest")},
          "iceberg.rest-catalog.uri" = {_sql_literal(branch_uri)},
          "iceberg.rest-catalog.warehouse" = {_sql_literal(NESSIE_WAREHOUSE_NAME)},
          ...
        )
    """
```

This requires Trino's dynamic catalog management mode (`catalog.management=dynamic` in
`infra/trino/etc/config.properties`, one config line, enabled once, still labeled experimental
upstream, accepted here because the alternative, a static catalog properties file written to
disk and a full Trino container restart per WAP run, is slower and disruptive to anyone else
using the shared stack concurrently). The catalog created this way (`iceberg_wap_<run-id>`) is a
completely distinct object from the project's ordinary main-scoped `iceberg` catalog. Nothing
written through it is visible through `iceberg` until the Nessie merge happens; this was
verified directly before any of `ops/` existed, by creating a table through a branch-scoped
catalog and confirming `SHOW SCHEMAS FROM iceberg` did not list it and a direct `SELECT` against
it failed with "Schema does not exist."

### `dbt run` and `dbt test` against the branch-scoped catalog

Both dbt invocations point at the freshly-registered catalog via an environment variable dbt's
Trino profile reads (`DBT_TRINO_DATABASE`), not at `main`'s catalog:

```python
env = {
    **os.environ,
    "DBT_TRINO_DATABASE": catalog_name,
    "DBT_TRINO_HOST": config.trino_host,
    "DBT_TRINO_PORT": str(config.trino_port),
}
```

`_run_dbt("run", ...)` (the write stage) and `_run_dbt("test", ...)` (the audit stage) are two
separate subprocess calls, not one `dbt build`. This split is deliberate, and it is what makes
the demonstration below meaningful rather than staged: `dbt run` has no data-quality awareness
of its own. It materializes whatever the model's SQL produces, bad rows included, and reports
success as long as the CTAS or MERGE statement itself executed without error. `dbt test` is the
only stage that actually inspects the data that was just written. If the write stage always
succeeds and only the audit stage can fail, then "audit catches what write cannot" is a real
property of the pipeline, not an artifact of how the demonstration was constructed
(`.notes/decisions.md`, WAP entry 4).

### The conditional merge-or-abandon logic

After both dbt stages pass, `run_wap` re-reads both the branch's and `main`'s current hashes
(both may have moved since the branch was cut: dbt's own writes advanced the branch, and a
concurrent WAP run could have already merged into `main`) and merges through the Nessie REST API:

```python
built_branch_ref = nessie.get_reference(config.nessie_uri, branch_name)
current_main_ref = nessie.get_reference(config.nessie_uri, "main")
try:
    merge_result = nessie.merge_branch(
        config.nessie_uri, "main", current_main_ref.hash, branch_name, built_branch_ref.hash,
    )
except nessie.NessieError as exc:
    raise WapStageError("merge", str(exc)) from exc
if not (merge_result.was_applied and merge_result.was_successful):
    raise WapStageError(
        "merge",
        f"merge reported wasApplied={merge_result.was_applied} "
        f"wasSuccessful={merge_result.was_successful}, main left untouched",
    )
```

`current_main_ref.hash` is passed as the merge's expected target hash, Nessie's
optimistic-concurrency guard: if `main` moved since it was last read, the merge is rejected
rather than silently clobbering a concurrent write. If either dbt stage instead raised
`WapStageError`, execution never reaches this block at all; the `except WapStageError` handler
at the bottom of `run_wap` logs `"wap run failed, main untouched"` and returns a nonzero exit
code without ever calling `merge_branch`. There is no code path in which a failed audit still
attempts a merge. The branch, by default, is left in place (`keep_failed_branch=True`) rather
than deleted, specifically so a human can point a one-off catalog at the failed branch and
inspect the bad rows directly, which is exactly what the evidence below does. The Trino catalog
registration, in contrast, is dropped unconditionally in the `finally` block on every run,
success or failure, because it holds no data of its own, only a name-to-branch binding: there is
nothing about it worth preserving as evidence.

## Worked example: the real captured evidence

Both runs below are raw output captured directly against the live Trino/Iceberg/Nessie/MinIO
stack (`docs/evidence/write-audit-publish/`), not narrated or hand-edited.

### The clean run

Before the run, `main` is at hash `9f1691c0...`, with no `dev_wap_demo` schema visible
(`00-before-refs.txt`). Running:

```
uv run python -m ops.wap --select wap_demo_dim
```

`dbt run` materializes the model's 8 static rows (`CREATE TABLE (8 rows)`), then `dbt test`
passes all three tests (`not_null_wap_demo_dim_demo_id`, `not_null_wap_demo_dim_demo_code`,
`unique_wap_demo_dim_demo_id`, `PASS=5 WARN=0 ERROR=0`). The log then shows the merge and cleanup:

```
merged to main, publish stage complete   resultant_main_hash=9522c1eb...
wap run succeeded, branch deleted
trino catalog deregistered
EXIT_CODE=0
```

(`01-clean-run-log.txt`). After the run, `main`'s refs show exactly one branch again, now at the
new hash `9522c1eb...`, and `dev_wap_demo` now appears in the schema list (`02-after-refs.txt`).
Querying through the ordinary main-scoped `iceberg` catalog, the same catalog every other query
in this project uses, returns all 8 rows, and `$history` proves this landed as a real Iceberg
snapshot commit on `main`, not a side effect confined to the script's own run:

```
 demo_id |  demo_code   |  demo_label
---------+--------------+--------------
       1 | us-east      | US East
       ...
       8 | af-south     | AF South
(8 rows)

       made_current_at       |     snapshot_id     | parent_id | is_current_ancestor
-----------------------------+---------------------+-----------+---------------------
 2026-08-05 00:50:06.883 UTC | 7624856498538845792 |      NULL | true
```

(`03-clean-run-main-query.txt`). This is the WAP mechanism doing exactly what it is supposed to
do when the data is genuinely good: the isolated build becomes the new, real state of `main`.

### The bad-load run

Before the run, `main` is unchanged from the clean run's result: 8 rows (`04-before-bad-run-refs.txt`).
Running:

```
uv run python -m ops.wap --select wap_demo_dim --vars '{"wap_demo_inject_bad_row": true}'
```

sets `wap_demo_inject_bad_row`, which the model unions in as a ninth row with `demo_id=99` and
`demo_code` cast to `null`. `dbt run` succeeds and materializes all 9 rows, exactly as the
write/audit split predicts, since `dbt run` has no way to know one of those rows is bad:

```
1 of 1 OK created sql table model dev_wap_demo.wap_demo_dim .................... [CREATE TABLE (9 rows)]
```

`dbt test` then catches it:

```
1 of 3 FAIL 1 not_null_wap_demo_dim_demo_code .................. [FAIL 1 in 0.10s]
...
Failure in test not_null_wap_demo_dim_demo_code (models/wap_demo/_wap_demo_dim.yml)
  Got 1 result, configured to fail if != 0
```

and the script's own log confirms the abort:

```
wap run failed, main untouched   detail='dbt test exited 1'   stage=dbt-test
leaving failed branch in place for inspection
trino catalog deregistered
EXIT_CODE=5
```

(`05-bad-run-log.txt`, exit code 5, matching `_STAGE_EXIT_CODES["dbt-test"]`). The script never
attempted a merge; the log has no `"merged to main"` line at all, because execution raised out of
the `dbt test` call before ever reaching that code. After the run, `main`'s hash is
byte-for-byte identical to before the run (`9522c1eb...`, same as `04-before-bad-run-refs.txt`),
row count is still exactly 8, and an explicit check for the injected violation returns zero rows:

```
=== explicit check: any null demo_code or demo_id=99 on main? expect 0 rows ===
"0"
```

(`06-after-bad-run-main-query.txt`). `main`'s refs list now shows two branches: `main` at its
unchanged hash, and the leftover `wap_20260805t005038z_2b912605` branch, kept for inspection.
Registering a one-off Trino catalog directly against that leftover branch (the same mechanism
`ops/wap.py` itself uses) and querying it shows the bad row is fully present there:

```
 demo_id |  demo_code   |                             demo_label
---------+--------------+---------------------------------------------------------------------
       1 | us-east      | US East
       ...
      99 | NULL         | bad row: null demo_code, injected by the WAP bad-load demonstration
(9 rows)
```

(`07-failed-branch-inspection.txt`). This is the concrete demonstration of the correctness
argument from the top of this document: the bad row is real, it was written, it is queryable,
and it exists nowhere that a consumer reading through the ordinary `iceberg` catalog can ever
reach it. It was never unreachable-then-visible-then-rolled-back. It was never reachable from
`main` at any point in time, full stop.

## When it fails

### The quality gate itself: what a passing audit does and does not prove

`dbt test` only catches what a test was written to check. The bad-load demonstration works
because `_wap_demo_dim.yml` declares a `not_null` test on `demo_code`; a violation with no
corresponding test would sail through the audit stage exactly as cleanly as good data, and merge
to `main` untouched. WAP is a mechanism for making a passing audit's result trustworthy, in the
sense that consumers never see a state the audit rejected; it does not make the audit itself more
complete. The coverage of `dbt test` against a real model is a separate concern, covered by
[13-data-contracts.md](13-data-contracts.md).

### Nessie's genesis-commit merge defect

`ops/wap.py`'s stage 0 exists because of a confirmed Nessie 0.108.4 defect: merging a branch
into a target fails outright with a 404 `REFERENCE_NOT_FOUND` whenever the two refs' common
ancestor is the server's genesis, empty-repository commit, even for a branch cut directly from
that hash with real commits layered on top. A brand-new repository, a fresh CI run above all,
starts in exactly that state every time, so `bootstrap_main_if_empty` gives `main` one real,
idempotent commit (a namespace create, tolerant of a 409 "already bootstrapped" response) before
the first branch is ever cut. Without this workaround, the very first WAP run against a fresh
environment would fail at the merge stage regardless of whether the data was good, which is a
failure of the underlying catalog, not of the write-audit-publish pattern itself, but one the
implementation has to route around to keep the pattern usable in practice.

### Concurrent dbt bookkeeping writes conflicting at merge time

A separate, real failure surfaced during development before the fix now baked into
`_ELEMENTARY_DISABLE_VARS`: the elementary-data dbt package registers project-wide hooks that
write bookkeeping tables (`dbt_invocations`, `dbt_run_results`, `elementary_test_results`) into
the plain `dev` schema, the same schema name on every branch regardless of which branch a run is
scoped to. A WAP run's own dbt invocations were writing to those unscoped keys at the same time
ordinary, unrelated dbt activity against `main` was also writing to them, and the merge then
failed with a genuine Nessie `REFERENCE_CONFLICT` on those keys, nothing to do with the actual
model under test. This is a real way WAP can fail that has nothing to do with data quality: a
merge conflict on unrelated bookkeeping state. The fix disables elementary's autoupload paths for
the duration of a WAP run via its own documented config vars, removing the conflict at its
source rather than special-casing the merge logic around it.

### The incident: the CI harness wiped the real warehouse

The most important "when it fails" case here is not a failure of the WAP pattern at all, and it
is worth stating that distinction precisely before describing what happened. The write-audit-
publish work package's own CI workflow, `.github/workflows/wap-gate.yml`, was tested locally
with `act` before being trusted on a real GitHub-hosted runner. `act` mounts the host's real
Docker daemon by default rather than an isolated one. `infra/docker-compose.yml` hardcodes
`name: iceberg-lakehouse` at the top of the file, a fixed Compose project name. The workflow's
own teardown step, `docker compose -f infra/docker-compose.yml --env-file .env down -v`, is
correct and necessary on a real, isolated GitHub-hosted runner: it tears down that run's own
throwaway containers and volumes. Run locally via `act` against the same fixed project name, it
tore down the developer's actual, already-running local stack instead, because `act`'s "up" step
had, moments earlier, reused the exact same container names as the real stack, meaning the whole
exercise was operating on the real infrastructure throughout, not a parallel copy of it. `down -v`
does exactly what it says: it removed `iceberg-lakehouse_postgres-data` and
`iceberg-lakehouse_minio-data`, the real volumes the real containers were using. This erased
Nessie's entire version-store history and every table in MinIO: all of bronze, `dev_silver`,
`dev_dimensions`, `dev_facts`, and the schema-evolution work package's own live demo tables,
none of which this work package had any authorization to touch (`.notes/failures.md`,
2026-08-05).

The twist worth naming directly: nothing about this incident is a counterexample to the
correctness argument this document makes. The WAP mechanism itself, branch isolation, the
audit gate, the conditional merge, was not exercised incorrectly and did not let bad data
through. What failed was infrastructure the validation harness shared with the very thing it
was supposed to be protecting. A validation pipeline's blast radius is not automatically
confined to the data path it is nominally testing; it is confined to whatever infrastructure it
actually touches, and a teardown step is exactly as destructive locally as it is correct
remotely, if it is pointed at the same resources either way. The lesson is about isolation of
the harness's whole environment, not about the pattern the harness exists to validate.

The fix, once identified, is a single environment variable at the workflow level:

```yaml
env:
  COMPOSE_PROJECT_NAME: wap-ci-${{ github.run_id }}
```

This overrides `infra/docker-compose.yml`'s fixed project name for this workflow's own
containers and volumes, so its "up" step creates `wap-ci-<run-id>-*` resources instead of
reusing `iceberg-lakehouse-*` ones, and its "down -v" step can only ever tear down resources it
itself created. The fix was verified as actually closing the gap, not just reasoned about: a
subsequent `act` run against the corrected workflow failed fast and safely on a host port
conflict (`127.0.0.1:9000` already bound by the real dev stack, since host port bindings are
fixed in the compose file independent of project name) rather than attaching to or tearing down
any `iceberg-lakehouse-*` container or volume, confirmed directly by checking `docker ps` and
`docker volume ls` immediately afterward. On a genuinely isolated GitHub-hosted runner, this
override changes nothing at all, since nothing else is ever present on that machine to collide
with; it exists purely because a validation pipeline that is safe on isolated infrastructure is
not automatically safe when someone, reasonably, tries to dry-run it against shared infrastructure
first. The general lesson: isolation has to extend to everything a validation pipeline touches,
not just the data path it was designed to protect. A pipeline can get its own core mechanism
exactly right and still cause real damage through a side effect, like a teardown step, that was
never part of the mechanism being validated in the first place.

## How to verify this is actually working

Reproduce the two runs directly against the local stack:

```
uv run python -m ops.wap --select wap_demo_dim
echo "exit: $?"   # expect 0

uv run python -m ops.wap --select wap_demo_dim --vars '{"wap_demo_inject_bad_row": true}'
echo "exit: $?"   # expect 5 (dbt-test stage)
```

After the bad run, confirm `main` is untouched and the bad row exists only on the abandoned
branch:

```sql
-- through the ordinary main-scoped catalog: expect 8 rows, none matching the violation
select count(*) from iceberg.dev_wap_demo.wap_demo_dim;
select count(*) from iceberg.dev_wap_demo.wap_demo_dim where demo_code is null or demo_id = 99;
```

```
curl -s http://localhost:19120/api/v2/trees   # confirm the failed branch is still listed
```

The exit code map (`ops/wap.py`'s `_STAGE_EXIT_CODES`) makes the failing stage identifiable
without parsing log text: 2 branch-create, 3 catalog-create, 4 dbt-run, 5 dbt-test, 6 merge,
7 dbt-deps, 8 bootstrap, 1 anything else, 0 success. `.github/workflows/wap-gate.yml` runs both
demonstrations on every push touching `ops/`, `transform/`, or `infra/`, and is deliberately
written to go red only if the gate fails to do its job (the bad load reaching `main`, or the
clean load failing to), not on every successful block, since a check that goes red on every
correct block of bad data would be a permanently-failing, non-actionable signal.
