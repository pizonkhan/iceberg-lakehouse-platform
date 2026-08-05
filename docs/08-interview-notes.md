# Interview notes

This is a private study guide, not a customer-facing document. Its purpose is to prepare the
repository owner to defend every real decision in this project under direct technical
questioning, with the specific numbers, file names, and mechanisms behind each answer close at
hand. It lives in the same public repo as everything else, so it is held to the same style
standards as the rest of `docs/`: plain writing, no hedging, no invented certainty.

Every answer below is backed by a real file, test, or evidence artifact somewhere in this
repository, not a generic dimensional-modeling talking point. Where a real gap or limitation
exists, the answer says so directly and explains the actual tradeoff, because a dodge is worse
than an honest "no, and here is why."

Source material: `.notes/decisions.md`, `.notes/modeling.md`, `.notes/failures.md`,
`.notes/surprises.md`, `.notes/open-questions.md` (gitignored working logs), plus
`docs/04-model.md`, `docs/06-tradeoffs.md`, and the ADRs under `docs/decisions/`.

---

## 1. Why Type 6 for dim_subscriber specifically, not just Type 2?

Because the business needs two genuinely different questions answered by the same dimension, and
neither a pure Type 1 nor a pure Type 2 table can answer both from one row. The first question is
"what was this subscriber's plan and status at the moment of this playback session," which needs
full version history, a Type 2 question. The second is "what is this subscriber's plan right
now, regardless of which historical row of them I happen to be looking at," which a Type 2
dimension cannot answer without a separate current-row lookup, and which a pure Type 1 dimension
could answer but only by throwing away the history the first question needs.

Type 6 (the name blends 1 plus 2 plus 3) answers both from one row. `plan_tier` and `status` are
Type 2 tracked: they drive versioning and sit inside `row_hash`, so a change to either opens a
new version. `email`, `display_name`, `country_code`, `acquisition_channel`, and
`current_plan_tier` are Type 1: overwritten on every historical row of that subscriber whenever a
new value shows up, so the latest truth is visible from any row in the chain, not just the
current one. `previous_plan_tier` is Type 3, one step of prior value, maintained by a plan
segment cursor rather than a plain `lag()`, specifically so a run of status-only versions between
two real plan changes all carry the same `previous_plan_tier` (the tier before the earlier
change), not the tier of whichever row happens to precede them. `churn_date_key` gets the same
Type 1 treatment as `current_plan_tier`: set on the version where status first becomes
`churned`, mirrored onto every historical row of that subscriber, and cleared back to NULL on
every row if the subscriber reactivates. It answers "did this subscriber ever churn and on what
date," a fact about the subscriber, not about one version of them.

Real build: about 50,000 entities, 125,616 rows including the unknown member. The Type 6
maintenance logic is verified against a real spot check, subscriber `sub_046072` (5 versions: a
real plan change standard to premium at version 2, then three status-only versions 3 through 5,
still premium): `previous_plan_tier` reads `'standard'` on versions 2 through 5,
`current_plan_tier` reads `'premium'` (final state) on all 5 rows, exactly matching the contract.
`churn_date_key` is verified the same way: zero subscribers with a non-churned current status
still carry a non-NULL `churn_date_key`, and 441 rows carry a non-NULL `churn_date_key` across
the 150 subscribers whose current status is `churned`.

One honest note on where this decision itself came from: the type assignment (`dim_subscriber` =
Type 6, `dim_title` = Type 2, `dim_plan` = Type 3, `dim_device` = Type 1) was fixed in the
approved plan before any builder work started, per this project's own working standards and the
opening line of `.notes/modeling.md`. A builder who disagreed with a grain or type assignment had an explicit
escape hatch, stop and flag it in `modeling.md`, and nobody ever used it for `dim_subscriber`.
The repo's answer to "why Type 6" is therefore the mechanics, the maintenance rules, and the
verification numbers above, all real and extensively exercised, rather than a first-principles
argument the repo itself had to construct from nothing.

Files: `transform/lakehouse/models/marts/dimensions/dim_subscriber.sql`, `.notes/modeling.md`
("dim_subscriber (Type 6 hybrid)"), `docs/04-model.md` ("dim_subscriber (Type 6 hybrid)").

---

## 2. Prove your point-in-time join is actually correct, not just present.

The predicate itself is a half-open interval match on the fact's own event-time column against
the dimension's `[effective_from, effective_to)`:

```sql
from silver_playback_sessions as f
left join dim_subscriber as ds
  on ds.subscriber_id = f.subscriber_id
  and f.session_started_at >= ds.effective_from
  and f.session_started_at <  ds.effective_to
```

Correctness rests on the dimension side being genuinely non-overlapping and gap-free, which is
not asserted, it is tested: `dbt_utils.mutually_exclusive_ranges` with `gaps='not_allowed'` and
`zero_length_range_allowed=false` on every SCD dimension, plus a direct `effective_from <
effective_to` check, plus exactly-one-`is_current`-row-per-natural-key. Because the intervals are
provably half-open, contiguous, and non-overlapping, the join can match at most one row per fact,
and a post-load test asserts the join changed no row count: `fct_billing_transactions` carries
`dbt_expectations.expect_table_row_count_to_equal_other_table` against `silver_billing_ledger`
directly in its schema file.

The harder proof is the one a real build actually surfaced: the predicate as literally specified
does not resolve correctly against the real data, and the fix is bounded and verified rather than
a blind fudge factor. Root cause: the vectorized numpy generators (`generation/playback.py`,
`generation/billing.py`) store event timestamps as `datetime64[s]`, whole-second only, for
throughput at 120-million-row scale, while the plain Python-loop generators
(`generation/subscribers.py`, `generation/titles.py`) keep true microsecond precision. So
`dim_subscriber.effective_from` and `dim_title.effective_from` carry real microseconds, but every
fact event timestamp lands on an exact whole second. This only matters when a fact's event time
and a dimension version boundary are supposed to be near-simultaneous, and it is worst for
`fct_signup_funnel`, where `registered_at` is the very event that creates the subscriber's first
`dim_subscriber` version: measured directly against real data, the literal predicate resolved
only 75 of 62,976 registered rows, 0.12%, sending nearly every registered attempt to the unknown
member.

The fix widens only the lower bound of the interval by exactly one second, the maximum possible
truncation error given whole-second source precision:

```sql
f.event_time + interval '1' second >= ds.effective_from
and f.event_time < ds.effective_to
```

Applied to `fct_signup_funnel`, this resolved all 62,976 registered rows with zero ambiguity,
0.12% to 100%, verified against the real built table, not just reasoned about. It is safe because
dimension versions in this dataset are built from real change timestamps that are themselves at
least a full second apart in practice, a condition worth reverifying before reusing the tolerance
on a new dimension, not an assumption to carry forward blind.

Files: `.notes/modeling.md` ("Point-in-time join rule" and the timestamp precision mismatch
section), `docs/04-model.md` ("Point-in-time join mechanism"),
`transform/lakehouse/models/marts/facts/_fct_billing_transactions.yml`,
`.notes/surprises.md` (2026-08-04 entry).

---

## 3. Walk me through the surrogate key collision math, why md5, why not a sequence.

Trino over Iceberg has no sequence or identity object. Faking one means assigning keys with
`row_number()` at build time, which is nondeterministic across rebuilds: a full refresh
reassigns every key from scratch and silently severs every fact already written against the
previous assignment. A hash key is a pure function of its input data, so an incremental merge, a
full rebuild, and a backfill all produce the identical key for the identical row, and a fact load
never has to wait on a dimension load to learn what key a given version was assigned. The natural
key alone does not work either, specifically for versioned dimensions: a Type 2 or Type 6
dimension maps one natural key to many rows over time, and a fact needs to pin one specific
version, which the natural key cannot express on its own.

Every hash-keyed dimension calls `dbt_utils.generate_surrogate_key([component_1, component_2,
...])`, never a hand-written formula. That distinction matters in practice, not just in style: an
earlier version of this project's own contract documented the formula as `md5(a || '||' || b...)`
with `'_null_'` for NULL, which does not match the installed dbt_utils 1.4.1 macro's real
behavior (delimiter `'-'`, NULL placeholder `'_dbt_utils_surrogate_key_null_'`), confirmed
directly against the macro's own source and cross-checked with Python's `hashlib`. Verified end
to end in Trino for `dim_subscriber`: `to_hex(md5(to_utf8('sub_000001' || '-' ||
'2023-05-01 16:20:01.962276')))` lowercased equals `8db31d15af4de707bcc8a37a72fedc7f`, the
model's actual `subscriber_sk` output for that row. Non-versioned dimensions hash the natural key
alone (or the full attribute combination for the junk dimension); versioned dimensions hash
`(natural_key, effective_from)`.

Collision arithmetic, worked at the project's worst-case table, `dim_subscriber` at roughly
200,000 rows with history:

- md5 is 128 bits, keyspace N = 2^128 = 3.40e38.
- Birthday approximation for at least one collision among n uniformly hashed inputs: P =
  n(n-1)/2N, effectively n²/2N.
- n = 2.0e5, so n² = 4.0e10. 2N = 6.80e38.
- P = 4.0e10 / 6.80e38 = 5.9e-29.
- Headroom check at 100x the planned history (20 million subscriber versions): n² = 4.0e14, P =
  5.9e-25. Still negligible.

For contrast, a 64-bit hash (N = 1.84e19) at the same n gives P = 4.0e10 / 3.69e19 = 1.1e-9,
which would also be acceptable at this scale, but md5 was kept anyway: it is the dbt_utils
default, reproducible identically across Trino and PyIceberg, and the 16-byte hex cost is
immaterial at 200,000 dimension rows. The real cost lands on the fact side: `fct_playback_events`
at roughly 120 million rows carries the same 32-character keys on every foreign key column, which
is the one place the width is a measurable cost, accepted deliberately for determinism.

Files: ADR-007 (`docs/decisions/ADR-007-deterministic-hash-surrogate-keys.md`),
`.notes/modeling.md` ("Surrogate key strategy" and "Collision arithmetic"), `docs/04-model.md`
("Surrogate key strategy").

---

## 4. Why Nessie over Polaris? Isn't Polaris the more standard choice now?

Polaris is the Iceberg ecosystem's post-graduation default and carries real advantages that come
with that status: broader tooling assumptions, more integration momentum, a simpler mental model
for anyone who only ever needs single-table time travel. It was still rejected here because its
branching model is per-table snapshot refs only, not a catalog-wide construct. A real
write-audit-publish workflow needs to stage changes across several tables at once and merge or
discard them as one atomic unit, and per-table refs cannot express that without coordinating refs
across tables by hand, which defeats the point of building the demonstration on catalog machinery
instead of application logic. Lakekeeper was the leanest of the three (a Rust binary, smallest
footprint) but offers no branching at all, so it was never really in contention for this specific
requirement. Nessie is the only one of the three with catalog-wide, git-style branching: one
branch spans every table registered under it, and a merge or rollback of that branch applies to
all of them atomically, which is a direct match for the WAP requirement as stated.

That choice was not free of real cost, and both costs were hit and root-caused directly rather
than glossed over. First: a fresh Nessie repository's genesis commit cannot be merged from, a
confirmed, reproduced defect at the pinned version (`ghcr.io/projectnessie/nessie:0.108.4-java`).
Every brand-new repo's `main` reports the identical hash
(`2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d`, confirmed across two
independently created fresh repos, so it is a deterministic constant, not derived from content),
and a branch cut from that hash cannot merge back into any target whose history also bottoms out
there. Worked around with `nessie.bootstrap_main_if_empty()`, called at the top of every
`ops/wap.py` run, so `main` always has one real commit before any branch is cut from it. Second:
Nessie's Iceberg REST bridge deliberately exposes only the single snapshot matching the current
commit, which breaks the naive assumption that `SELECT ... FOR VERSION AS OF` works unmodified on
this catalog. That is answered fully in question 8 below.

What this bought in return is real and demonstrated, not theoretical: `ops/wap.py`, a genuine
write-audit-publish gate wired into `.github/workflows/wap-gate.yml`, and a rollback story built
on Nessie's actual branch-reset operation rather than a workaround. Polaris's ecosystem-default
status was a real thing given up, and the honest answer to "isn't Polaris more standard now" is
yes, and this project chose the catalog that could deliver the specific mechanism (multi-table
atomic branching) the project's own stated goal required, over the one with more ecosystem
momentum but a narrower branching model.

Files: ADR-001 (`docs/decisions/ADR-001-catalog-choice-nessie-over-polaris-and-lakekeeper.md`),
`docs/06-tradeoffs.md` ("Nessie chosen over Polaris"), `.notes/surprises.md` (2026-08-05 genesis
commit entry), `ops/wap.py`, `ops/nessie.py`.

---

## 5. What actually stops a gold model from reading bronze directly?

Until recently, nothing mechanical did. This project's working standards state the rule as a hard
constraint, "a gold model reading directly from bronze is an architecture violation and gets
rejected, no exceptions," but that was enforced only by human and agent review at build time. A red team pass
that ran an interview simulation against this exact repo asked the obvious follow-up: how do you
know that rule actually holds, prove it. The honest answer at the time was "someone would have
checked at review time," which is not a proof. That gap was closed in the same pass, not left
open.

`tests/unit/test_medallion_boundary.py` now enforces it mechanically. It parses the static dbt
manifest (`transform/lakehouse/target/manifest.json`, the same pre-parsed artifact
`test_orchestration_definitions.py` already relies on, so it needs no live stack) and asserts that
no model under `intermediate/` (silver) or `marts/` (gold: dimensions, facts, bridge) has a
direct dependency edge on a `source('bronze', ...)`. Only `staging/` is allowed to touch bronze
directly, which is the layer whose whole job is turning a bronze source into something `ref()`-
able for everything downstream. A separate sanity test in the same file
(`test_manifest_has_bronze_sources_and_medallion_layers`) asserts the manifest actually contains 9
bronze sources and all three expected layers, specifically so the real assertion cannot pass
silently over a stale or empty manifest. Verified directly: zero violations across all 68 models
in the project at the time this test was added. It now runs as part of `make test`, no separate
wiring needed.

Files: `tests/unit/test_medallion_boundary.py`, `docs/05-implementation.md` (medallion layer
boundaries), `transform/lakehouse/models/staging/sources.yml`, `.notes/decisions.md` (2026-08-05,
red team pass part 4).

---

## 6. Why watermark on ingestion time instead of event time for your incremental models?

Because this project's own synthetic data generator deliberately injects out-of-order arrival as
a required pathology, and an event-time watermark would silently and permanently defeat it. A
fraction of every playback batch (`out_of_order_fraction`, 3%) is peeled off and spliced into a
file written 3 to 15 batches later than the batch its own `session_started_at` actually belongs
to, confirmed and enforced by the generator's own manifest
(`_pathology_manifest/out_of_order_summary.json`) and `generation/sanity_check.py`. An incremental
filter on the fact's business event time (`session_started_at`, `transaction_posted_at`,
`added_at`) would advance past a straggler's true event time as soon as any later run scans a
newer range, and the straggler would then never be picked up again by any subsequent run, no
error, no trace, just silently missing data.

Every converted fact's `is_incremental()` filter is instead built on bronze `_ingested_at`,
passed through silver unchanged. Silver rebuilds full-refresh every run but preserves each row's
original bronze `_ingested_at` untouched, so `_ingested_at > watermark` correctly identifies rows
genuinely new to bronze on this run, regardless of how old their business event time is. This was
verified directly against `fct_playback_events`, not just reasoned about: the built table's own
`max(session_started_at)` already sits at or past where a straggler's true event time would need
to be, so an event-time watermark would have silently excluded it, while an ingestion-time
watermark carries no event-time information at all and structurally cannot make that mistake.

A dual watermark (event time for the common case, a separate reconciliation pass on ingestion
time for stragglers) was considered and rejected: it would recover correctness at the cost of two
filters and two code paths per incremental model, for no benefit over an ingestion-time watermark
alone, which is correct by construction with a single filter.

One honest caveat worth stating plainly: this project's current bronze history is a single
ingestion batch per table for most facts, so the out-of-order pathology is the strongest real
evidence available that the design is correct. A second, genuinely trickling ingestion run has
not been exercised against these watermarks yet.

Files: ADR-004 (`docs/decisions/ADR-004-ingestion-time-watermark-for-incremental-processing.md`),
`.notes/modeling.md` ("Incremental processing: which facts convert, which stay full-refresh"),
`docs/06-tradeoffs.md` ("Which facts converted to incremental").

---

## 7. How does a billing transaction's plan_sk actually get resolved, and why isn't it just from the subscriber dimension?

Because `dim_subscriber` does not track a concrete plan at all, it tracks `plan_tier`, and
`plan_tier` maps many-to-one from `plan_id`: multiple concrete plans share a tier, confirmed
against real data at 7 to 8 plan rows per tier. There is no path from `dim_subscriber` to a
specific `plan_id`, so the only real source of a subscriber's actual plan is
`silver_billing_ledger`, the billing history itself.

For `fct_billing_transactions`, this is simple: each row already carries its own `plan_id`
directly from the source, so `plan_sk` is a plain foreign key lookup against `dim_plan`, no
point-in-time interval needed.

For `fct_daily_subscription_snapshot`, it is genuinely harder, because a snapshot day needs "the
plan this subscriber was on as of end of that day," not the plan tied to any single transaction.
This is built as a half-open plan-history interval table constructed directly from
`silver_billing_ledger` inside the model: one row per billing transaction, ordered by
`transaction_posted_at`, with each transaction's interval running to the next transaction (or the
high date), the same interval shape `dim_subscriber` itself uses for its own versioning. The
snapshot then resolves `plan_sk` as the `plan_id` from that subscriber's most recent billing
transaction at or before the snapshot instant.

Two real correctness details had to be handled, not glossed over. First, a same-instant tie-break:
19 `(subscriber_id, transaction_posted_at)` pairs in the real data carry more than one transaction
at the identical timestamp, the worst case being `sub_048887` with 105 transactions across 29
distinct `plan_id` values at one instant, clearly synthetic-data noise rather than a real billing
pattern. Deduplicated with `partition by (subscriber_id, transaction_posted_at) order by
_ingested_at desc, billing_transaction_id desc`, keeping rank 1, the same style
`silver_billing_ledger`'s own retry dedup already uses, extended with `billing_transaction_id` as
a final deterministic tie-break since `_ingested_at` alone does not guarantee a total order across
those groups. Second, the unbilled case: before a subscriber's first billing transaction (an
unbilled trial), `plan_sk` correctly takes the unknown member rather than guessing. Verified
directly: 1,394,403 of 27,011,346 real snapshot rows, 5.2%, fall before that subscriber's first
billing transaction and take the unknown `plan_sk`, averaging 27.9 pre-billing days across the
50,000 subscribers (max 656 days), consistent with real unbilled-trial behavior, not a join bug.

This makes `plan_sk` on the snapshot a slowly-changing-as-of-billing-event attribute, deliberately
not strictly tied to the `dim_subscriber` version current at that instant. That is correct
behavior, not an inconsistency: a subscriber's tier can read `'standard'` while their concrete
`plan_id` has not changed row since their last invoice.

Files: `.notes/modeling.md` ("fct_daily_subscription_snapshot", "plan_sk resolution, clarified
after the initial draft"), `.notes/decisions.md` (plan_sk resolution entry, roughly line 1484),
`transform/lakehouse/models/marts/facts/fct_daily_subscription_snapshot.sql`.

---

## 8. Does time travel actually work on this stack?

Not the way most engineers familiar with Iceberg would expect on the first try, and the honest
version of that answer is more useful than a confident wrong one.

The first attempt assumed Iceberg-native snapshot chaining, `$snapshots`, `$history`,
`SELECT ... FOR VERSION AS OF`, would simply work on any Iceberg catalog. It was tested directly,
not assumed: a live Trino MERGE against `fct_billing_transactions` was deliberately killed
mid-write to prove Iceberg's commit atomicity, and while that atomicity proof succeeded (a killed
query left zero trace on a committed read, confirmed by polling for an in-flight Parquet file
before killing the query), inspecting the table's snapshot history afterward surfaced a real
problem: after two consecutive successful MERGEs, both `"fct_billing_transactions$history"` and
`"...$snapshots"` showed only the single most recent snapshot, `parent_id` empty. The raw
`metadata.json` files in MinIO confirmed it directly: every commit writes a fresh
`00000-<uuid>.metadata.json`, version-number 0, never chaining to a previous one. Iceberg-side
time travel genuinely does not work through Trino on this catalog.

Root cause, confirmed rather than guessed at: Nessie's own `iceberg-rest.md` guide states it
outright, the REST bridge deliberately exposes only the single snapshot matching the current
commit, to preserve catalog-wide, cross-branch consistency guarantees. This is a documented
design tradeoff for the same reason Nessie was chosen over Polaris in question 4, not a bug, not
a missing docker-compose flag, and not something specific to dbt's write path: independently
confirmed that a plain Trino `INSERT` with no dbt involved produces the identical pattern.

The real mechanism, built and demonstrated once the root cause was understood: Nessie's REST API
accepts a checked reference as a first-class value everywhere a plain branch name is accepted,
including inside the Iceberg REST catalog's own URL path, confirmed live in three forms:
`<branch>@<commit-hash>`, a bare commit hash with no branch name, and
`<branch>#<ISO-8601-timestamp>`. Because `ops/wap.py`'s existing catalog-registration function
already builds its Trino catalog's `iceberg.rest-catalog.uri` from an arbitrary branch-name
string with no assumption about its contents, handing it a string that already includes
`@<hash>` gives point-in-time query with zero new catalog-registration code. Rollback is a
genuinely different operation: a real Nessie branch reset, `PUT
/api/v2/trees/{branch}@{expected-hash}` with a flat body `{"type": "BRANCH", "name": <branch>,
"hash": <target-hash>}` (not the nested `{"assignTo": {...}}` shape a first guess assumed by
analogy with the Nessie Java client, and rejected live by the real server), which moves the
branch pointer directly, backward included, with no ancestor relationship required between the
two hashes.

`ops/time_travel_demo.py` builds a table on a dedicated branch (`time_travel_demo`, cut from
`main`, never run directly on `main`), lands a good batch then a deliberately bad one with
negative amounts, proves a point-in-time query at the good commit's hash excludes the bad rows,
then proves the branch reset is a real rollback: the live, no-hash catalog view shows only good
rows afterward and the bad commit is no longer reachable in the branch's history. Retention is
the one piece left honestly incomplete: Nessie never auto-expires anything, neither commits nor
the data files they reference, and Iceberg's own `expire_snapshots` has nothing meaningful to
expire under this catalog's single-snapshot-per-commit design. The real retention tool is a
separate program, `nessie-gc`, mark-and-sweep against a per-reference cutoff. A policy was chosen
and justified (`main=P14D`, `wap_.*=P3D`, `time_travel_demo=P7D`, `default-cutoff=P30D`), but it
is not scheduled anywhere in this stack today, so MinIO storage growth is currently unbounded in
practice.

Files: ADR-008 (`docs/decisions/ADR-008-nessie-native-time-travel-and-rollback.md`),
`docs/evidence/time-travel/` (including `10-root-cause-confirmation.txt` and
`09-retention-policy-notes.txt`), `ops/nessie.py` (`get_log`, `assign_reference`),
`ops/time_travel_demo.py`, `docs/07-operations.md` ("Incident 2").

---

## 9. What's the most embarrassing bug you shipped and caught, and how did you catch it?

`fct_playback_events` resolved `title_sk` to the unknown member on 20,646,958 of 119,640,099
rows, 17.3% of the entire fact, and the embarrassing part is not the bug itself, it is that the
bug was found, correctly root-caused, and then shipped anyway the first time.

Root cause: `generation/titles.py`'s `generate_titles()` drew each title's `catalog_add_at`
uniformly from the same `[platform_launch, now)` span that `generate_playback_events()` and
`generate_watchlist_events()` draw their own session timestamps from, with zero relationship
between which title got sampled for a given playback row and that title's own `catalog_add_at`.
Because subscriber signups are front-loaded right at `platform_launch` (real density there), and
titles were sampled uniformly across the entire span, a title could easily have a
`catalog_add_at` dated almost at the generator's own "now" cutoff (one sampled title, `tt_00221`,
had its first tracked version dated the day before `GENERATION_NOW`) while still showing up in
playback sessions timestamped years earlier. Verified directly before touching any code: 4,833 of
5,000 titles had at least one playback session earlier than their own first `catalog_add`. This
directly contradicted `modeling.md`'s stated design assumption for `dim_title`, "titles arrive
from a controlled catalog feed ahead of playback," the reason `dim_title` has no
`is_inferred`/late-arrival self-heal logic the way `dim_subscriber` does. At 4,833 of 5,000
titles this was a generator defect, not the rare "unseen title" edge case the design assumed.

It was caught the first time in the diagnostic sense: correctly root-caused and written up in
`decisions.md` and `open-questions.md`. It was then shipped as "implemented exactly as specified,
flagged in open-questions.md," which is not the same thing as fixing it. Root-causing a defect is
a diagnosis, not a repair, and flagging it in a working log is not a substitute for going back to
the source when the source is what is actually broken. It was caught for real on a later review
pass, not the original build.

Fix: gave titles their own catalog seed window that ends strictly before `platform_launch`,
instead of sharing playback and watchlist's `[platform_launch, now)` span. Added
`CATALOG_SEED_LEAD_TIME` (180 days) and `CATALOG_SEED_BUFFER` (1 day) to `generation/config.py`;
`generate_titles()` now draws each title's first `catalog_add_at` from
`[platform_launch - lead_time - buffer, platform_launch - buffer]`, always strictly earlier than
the earliest instant playback or watchlist events can produce, so the ordering holds by
construction rather than by chance.

Verified to zero, not just "improved": regenerated the full dataset (same seed, 103.5 seconds,
every row count identical to the prior run), reran `generation/sanity_check.py` (all 11 pathology
checks still pass unchanged), and confirmed directly against the regenerated Parquet files that 0
of 5,000 titles have a playback session before their first `catalog_add` (down from 4,833), and 0
of 120,000,300 playback rows violate their title's `catalog_add_at`, a row-level check, not just a
title-level one. Forced a reload of just `bronze_title_events`, rebuilt `stg_title_events`,
`silver_title_events`, `dim_title`, and `fct_playback_events` with `--full-refresh`: 48 of 48 dbt
tests and models pass, `dim_title` still 9,256 real history rows (9,257 with the unknown member),
`fct_playback_events` still 119,640,099 rows. `title_sk` now resolves to the unknown member on 0
of 119,640,099 rows, down from 20,646,958.

Files: `.notes/failures.md` (2026-08-04, generator defect entry), `generation/titles.py`,
`generation/config.py` (`CATALOG_SEED_LEAD_TIME`, `CATALOG_SEED_BUFFER`),
`.notes/open-questions.md` (the entry that shows this being flagged before it was fixed),
`docs/04-model.md` ("dim_title (Type 2)").

---

## 10. If I deleted a row from your fact table right now, would anything catch it?

Mostly no, and that is a real, current gap worth stating plainly rather than talking around.

Structurally, the incremental facts run on MERGE, which only inserts or updates rows matched by
its source; it never deletes a target row that has disappeared. Deleting a row directly from
`fct_billing_transactions`, `fct_playback_events`, `fct_watchlist_adds`, or
`fct_daily_subscription_snapshot` bypasses dbt entirely, and the next normal incremental run's
ingestion-time watermark would not treat that row as new, its `_ingested_at` already sits below
whatever watermark the last successful run advanced to, so a routine incremental run would not
restore it. Only a `--full-refresh` rebuild from silver brings it back.

Whether a dbt test would notice depends on which fact. `fct_billing_transactions` is the one real
exception: it carries `dbt_expectations.expect_table_row_count_to_equal_other_table` against
`silver_billing_ledger`, added specifically after `dbt_utils.equal_rowcount`'s generated SQL
failed on this project's Trino with a `COLUMN_NOT_FOUND` bug (its `group by
id_dbtutils_test_equal_rowcount` groups by a SELECT-list alias for a constant, a genuine Trino
planner limitation, not a bug in the model). That test runs on every `dbt test` regardless of
materialization, so it would catch a missing billing row on the next `make test`.

No other fact has an equivalent. `fct_playback_events` does not even have a dbt-native
uniqueness test on `playback_session_id` at its ~120-million-row scale: the equivalent query has
crashed the Trino coordinator container outright, not failed gracefully, so uniqueness was
verified once by hand, a 44-way monthly-chunked manual check finding zero duplicates across all
119,640,099 rows, and never wired into `dbt test`. A deleted row there would be caught by nothing
automated at all, only by a manual row-count check against the known-good baseline documented in
`docs/07-operations.md` (119,640,099). `fct_watchlist_adds` and `fct_daily_subscription_snapshot`
carry `not_null`, `unique`, and `relationships` tests on their foreign keys, none of which a
missing row trips, since a deletion does not create a null, a duplicate, or an orphaned key, it
just makes the row not exist.

The honest fix, not yet built: extend the row-count-reconciliation pattern already proven on
`fct_billing_transactions` to the other three incremental facts, or add a scheduled reconciliation
job outside the dbt test framework the way playback's manual uniqueness check already exists
outside it. Neither is done today.

Files: `transform/lakehouse/models/marts/facts/_fct_billing_transactions.yml` (the
`expect_table_row_count_to_equal_other_table` test and its comment explaining the
`equal_rowcount` rejection), `.notes/open-questions.md` (the `fct_playback_events` uniqueness
gap), `docs/07-operations.md` ("Monitoring", the row-count baseline table).

---

## 11. Prove your incremental pipeline is idempotent, don't just claim it.

The method used throughout the project is consistent: build the same model two or three times
against unchanged upstream data, then compare both the row count and an order-independent
full-row content checksum that excludes `loaded_at` (wall-clock audit metadata, expected to
differ run to run by design). Real numbers, not a description of the method in the abstract:

- `dim_subscriber`: three consecutive full-refresh builds produced the identical checksum
  (`7E4764A639A45DF380D639CC2EE6D409`) and the identical row count (125,616) every time. The
  first attempt at this checksum was itself a false negative worth knowing about: putting
  `ORDER BY` inside a subquery feeding an unordered `array_agg` produced two different checksums
  on genuinely identical data, exactly the failure mode Trino's own "ORDER BY in subquery may
  have no effect" warning describes. Fixed by moving `ORDER BY` inside the aggregate itself,
  `array_agg(row_str ORDER BY row_str)`.
- `fct_billing_transactions`: two consecutive incremental builds held an identical row count and
  full-row checksum.
- `fct_watchlist_adds`: a from-scratch full-refresh reproduced the original 750,000-row build's
  checksum (`C3B540624D04AE72`) exactly; three consecutive no-op incremental MERGE runs (`MERGE
  (0 rows)` each time) held that same checksum; a backfill that force-rewrote all 750,000 rows
  (`MERGE (750,000 rows)`) still produced the identical checksum afterward, with `loaded_at`
  collapsing to one new shared timestamp, proof the merge genuinely rewrote every row rather than
  skipping them.
- `fct_daily_subscription_snapshot`: three consecutive incremental runs held row count
  (27,011,346) and checksum (`725378a3d48c791d`) constant. Confirmed directly which rows the
  MERGE actually touched each run (`loaded_at` newer than the run's start): exactly 149,384 rows
  every time, spanning the 3-day reprocess window and nothing else, not all 27 million, which is
  the mechanical proof that the bounded design is genuinely bounded, not accidentally rewriting
  the whole table every run.
- `fct_playback_events` (~120 million rows): a full-table checksum does not fit this project's
  1.5GB Trino memory cap, even restricted to a single calendar month, because the deliberate 3%
  out-of-order pathology means every physical file spans nearly the table's whole date range, so
  file and row-group pruning cannot narrow a scan the way it would on ordered data. Solved with
  an evenly-spread 30-day sample across the full date range (3,288,953 rows, 2.7% of the table),
  XORing 30 partial `xxhash64` checksums into one combined value: identical
  (`-8261973429135120039`) across three consecutive incremental runs, alongside the exact,
  full-precision whole-table row count (119,640,099), also identical all three times. This is
  recorded honestly as a real but partial-coverage fingerprint, not silently presented as
  equivalent to the full-table technique used on the smaller facts.

There is also a real-world proof beyond synthetic repetition: a live Trino MERGE against
`fct_billing_transactions` was deliberately `SIGKILL`ed mid-write, after polling MinIO directly to
confirm a real Parquet file had actually landed before the kill, to test whether a violently
interrupted write could leave the table in an inconsistent state. It could not: row count stayed
1,500,100, the content checksum was unchanged, `SELECT count(*)` succeeded throughout with no
lock or partial-read state, and rerunning the identical command to completion matched the
original baseline exactly, no manual repair required. Iceberg's atomic commit means a killed
write either never happened as far as any reader is concerned, or it left a harmless orphaned
file with no committed reference to it.

Files: `.notes/decisions.md` (idempotency entries under each dimension and fact's own build
section), `docs/07-operations.md` ("Incident 2: proving Iceberg's atomic commit under a mid-merge
kill").

---

## 12. What would you do differently if you built this again?

Six concrete things, each tied to a real gap this project actually hit rather than a generic
lesson:

1. **Run the real fresh-clone sequence early and often, not once at the end.** Four separate
   reproducibility gaps, `make seed` never invoking the generator, `make build` defaulting to an
   unwired `duckdb` target, `make test` being an unconditional stub, and `dbt deps` never being
   wired into any target at all, were only found because a red team pass finally ran `make up &&
   make seed && make build && make test` against a directory that had genuinely never had any of
   those steps run in it before. A fourth gap (`dbt deps`) was found immediately after fixing the
   first three, in the same pass, which is itself the lesson: fixing every known-plausible gap
   and declaring reproducibility done, without actually running the real sequence again, would
   have shipped a fifth one.
2. **Give every generator the same timestamp precision from the start.** The whole-second versus
   microsecond mismatch between the vectorized numpy generators and the plain Python-loop
   generators was a real, verified correctness problem, not just an inconvenience, and required a
   bounded one-second interval-widening mitigation bolted onto the point-in-time join rule after
   the fact. Building all four generators to the same precision contract up front removes an
   entire class of join-boundary bug rather than mitigating it after discovery.
3. **Fix a diagnosed data bug at the source, not around it.** The `dim_title` catalog-ordering
   defect in question 9 is the sharpest version of this lesson: it was correctly root-caused the
   first time and shipped anyway with a flag in `open-questions.md`. A defect that is found and
   understood but not fixed at its source is not resolved, no matter how precisely it is
   documented.
4. **Write the mechanical enforcement test at the same time as the rule, not after an interview
   simulation asks for it.** The medallion-boundary rule sat as review-only convention for the
   whole build; `tests/unit/test_medallion_boundary.py` should have existed
   from the first gold model, not been added retroactively once a red team pass asked "prove it"
   and got no good answer.
5. **Parameterize every host port from the start.** MinIO's host API port and Postgres's host
   port both turned out not to be independently configurable despite `.env.example` implying
   otherwise, because Nessie's own S3 endpoint config hardcodes the container-internal address
   regardless of the host mapping. The practical effect is that only one copy of this stack can
   run on a given machine at a time. The fix (`${MINIO_API_PORT:-9000}`,
   `${POSTGRES_PORT:-5432}` in `docker-compose.yml`) is small and mechanical; doing it up front
   costs nothing and avoids a real reproducibility surprise discovered later.
6. **Close the row-count reconciliation gap on every incremental fact, not just one.** Question
   10's answer is the honest version of this: `fct_billing_transactions` has a real test that
   would catch a deleted row, and the other three incremental facts do not. That pattern should
   have been applied uniformly the first time a fact converted to incremental MERGE, not left as
   a known asymmetry.

None of these are hedges. Every one names a real, already-documented gap this project hit, not a
hypothetical improvement invented for this answer.

Files: `.notes/failures.md` (the three-gaps-then-a-fourth reproducibility entries), `.notes/
surprises.md` (the timestamp precision entry), `.notes/decisions.md` (2026-08-05, red team pass
part 4), `.notes/open-questions.md` (the two docker-compose port gaps).
