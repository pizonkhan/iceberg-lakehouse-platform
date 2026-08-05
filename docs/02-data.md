# The data

Every row in this project is synthetic. There is no real subscriber, no real payment, no real
viewing history behind any of it. This document explains what gets generated, how much of it,
how it stays reproducible, and, in detail, the data quality problems deliberately built into it
on purpose.

## Why synthetic

A portfolio data platform needs source data that is dirty in specific, known ways, not dirty by
accident. Three things made synthetic generation the right call here instead of pulling a public
dataset:

- **Controllable pathologies.** The whole point of a medallion pipeline is what it does with bad
  data: late-arriving dimensions, duplicate keys, out-of-order events, malformed rows. A public
  dataset does not usually come with those defects catalogued, so proving the pipeline catches
  them means either hunting for a dataset that happens to have the right defects in the right
  proportions, or building the defects deliberately. Generating the data means every pathology
  below is injected on purpose, at a known rate, with the affected rows listed in a manifest, so
  the pipeline's handling of each one can be checked directly against ground truth instead of
  inferred after the fact.
- **No privacy concerns.** Subscriber emails, names, and viewing habits are fabricated. Nothing
  here is a real person's data, so there is nothing to anonymize, redact, or worry about
  mishandling.
- **Reproducibility.** The entire dataset regenerates byte-for-byte identical from one seed. That
  matters for the same reason it matters everywhere else in this project: a claim about the
  pipeline's behavior should be something anyone can rerun and verify, not something to take on
  faith. See "Determinism" below.

## What gets generated

Nine entity streams come out of `generation/`, one Python module per stream (`generation/config.py`
holds the scale and pathology parameters; `generation/rng.py` is the shared seeding utility;
`generation/generate.py` is the CLI entry point that runs all nine in order and writes
`generation/output/run_summary.json`). Each writes Parquet directly to `generation/output/<entity>/`,
never one giant file, so the largest streams stay batch-writable without holding the whole thing
in memory.

| entity | generator | shape |
|---|---|---|
| plans | `generation/reference.py` | static reference table, no change history |
| devices | `generation/reference.py` | static reference table, no change history |
| title_genres | `generation/reference.py` | title-to-genre bridge, 1 to 4 genres per title, Dirichlet-sampled weights summing to exactly 1.0000 |
| subscriber_events | `generation/subscribers.py` | change-event stream: one row per subscriber profile snapshot at a change moment (signup, then follow-on plan/status/attribute changes) |
| title_events | `generation/titles.py` | change-event stream: one row per title metadata snapshot at a change moment (catalog_add, then follow-on metadata_update events) |
| playback_sessions | `generation/playback.py` | one row per completed viewing session |
| billing_ledger | `generation/billing.py` | one row per billing ledger event (charge, refund, credit, proration) |
| watchlist_adds | `generation/watchlist.py` | one row per subscriber adding a title to their watchlist |
| signup_funnel | `generation/signup_funnel.py` | one row per signup attempt, carrying only the milestone timestamps a source system would actually observe |

The subscriber and title streams are change-event feeds, not one row per entity: a later stage
(the dimensional model's SCD build) hashes the tracked attributes to construct version history
from them. `signup_funnel`'s generator emits only observed milestone timestamps and
`plan_id_selected`; the funnel status and stage-duration measures the business actually wants are
computed downstream, from those timestamps, not emitted here, because a real source system would
not emit a derived status either.

### Row counts, full scale

These are the actual counts from the current `generation/output/` (`run_summary.json` and direct
Parquet row counts; the two disagree slightly for `playback_sessions` and `billing_ledger`
because `run_summary.json`'s own count is captured before the deliberately-injected duplicate and
midstream pathology rows are appended, a reporting quirk in the generator's summary step, not a
data problem, confirmed by counting every Parquet file's rows directly):

| entity | rows |
|---|---|
| plans | 30 |
| devices | 3,000 |
| title_genres | 12,385 |
| subscriber_events | 199,928 (~50,000 distinct subscribers) |
| title_events | 15,098 (5,000 distinct titles) |
| playback_sessions | 120,000,300 |
| billing_ledger | 1,515,049 |
| watchlist_adds | 750,000 |
| signup_funnel | 70,701 |

Full-scale generation runs in about 104 seconds and produces roughly 3.0GB of Parquet on disk, at
a peak memory footprint of about 2.15GB, well inside a laptop's budget. The dominant cost is
`playback_sessions`: 120 million rows built in batches (`playback_batch_size` of 5,000,000 rows
per batch) so peak memory stays bounded regardless of total volume, rather than materializing the
whole fact in memory at once.

Downstream, after deduplication and dimensional modeling, these source rows become (also real,
verified counts): `dim_subscriber` 125,616 rows (125,615 real versions plus the unknown member),
`dim_title` 9,257 rows (9,256 real versions plus the unknown member), `dim_plan` 31 rows,
`dim_device` 3,001 rows, `fct_playback_events` 119,640,099 rows, `fct_billing_transactions`
1,500,100 rows, `fct_signup_funnel` 70,000 rows, `fct_watchlist_adds` 750,000 rows, and
`fct_daily_subscription_snapshot` 27,011,346 rows. The gap between a dimension's raw event count
and its final versioned row count is not a bug: `dim_subscriber` only versions on a change to
`plan_tier` or `status`, so the majority of the 199,928 raw profile-change events touch some
other attribute and correctly collapse into an existing version rather than creating a new one.
The same collapsing explains `dim_title`: of its 15,098 raw catalog and metadata events, 5,842
are exact repeats of the immediately prior event on every tracked column and produce no new
version.

A second, smaller scale exists for fast iteration: `--scale small` targets 1,000 subscribers with
everything else scaled proportionally (roughly 1/50th), used to validate that every pathology is
present before committing to a full-scale run.

## Determinism: one seed, many independent streams

Every random draw anywhere in `generation/` comes from `generation/rng.py`'s `child_rng(seed,
label)`, never from a bare, unseeded `np.random.default_rng()` and never from the global numpy
random state. The single project-wide `SEED` (`20260804`, set in `generation/config.py`) is
folded together with a CRC32 hash of a stable string label (`"playback.batch.7"`,
`"subscribers.core"`, `"billing.pathology.midstream"`, and so on) into a `numpy.random.SeedSequence`,
which seeds an independent generator for that one purpose.

The label-keyed design, rather than keying streams off spawn order, is deliberate: two calls with
the same `(seed, label)` always produce the same output regardless of what else ran earlier in
the same invocation, so re-running one stage in isolation during development never perturbs an
unrelated stream. This was verified directly, not just assumed: running `--scale small` twice and
diffing the SHA-256 of every output Parquet file produced byte-identical results both times.

This is the same lesson the project's own idempotency proofs depend on elsewhere in the pipeline
(dbt models re-run against unchanged input reproduce identical row checksums): a pipeline that
cannot reproduce its own output on demand cannot be trusted when something looks wrong, because
there is no way to tell a real regression from run-to-run noise. Seeding the generator the same
way extends that guarantee to the very first stage, before any of the pipeline's own logic runs.

## The injected pathologies

`generation/sanity_check.py` asserts all eleven required pathologies at both scales and is the
executable source of truth for this list if the two ever drift. The numbering below is the
project's own (pathologies 3 and 4 were never specified and do not exist; the gap in the sequence
is intentional, not a missing item). Every constructed row is listed in a CSV or JSON manifest
under `generation/output/_pathology_manifest/`, so each pathology's affected rows can be pulled
up directly rather than taken on faith.

**1. Late-arriving dimension members.** 200 subscriber_ids are generated normally (real signup
date, real follow-on profile changes) but their rows are written to the physically last file,
`subscriber_events/part-00001-late-arrival.parquet`, while `playback_sessions`, `billing_ledger`,
and `watchlist_adds` sample from the same full subscriber pool without regard to file order. The
practical effect: a fact event for one of these subscribers can be ingested before that
subscriber's own profile row exists anywhere. This simulates a dimension feed lagging behind the
event feed it is supposed to describe, a routine occurrence in real systems where different
services publish on independent schedules. Handled in `dim_subscriber.sql`: on every build, the
model unions every subscriber_id referenced by `silver_playback_sessions`,
`silver_billing_ledger`, and `silver_watchlist_adds`, finds any that appear nowhere in
`silver_subscriber_events`, and synthesizes an inferred row for each (`subscriber_sk` derived
from a sentinel `1900-01-01` `effective_from`, so any event timestamp falls inside its interval
and the point-in-time join always resolves). Manifest: `late_arriving_subscribers.csv`.

**2. Late-arriving facts against a historical dimension version.** For any subscriber with three
or more change events, the generator records the first non-final gap between two consecutive
versions as a candidate. 300 playback rows and 100 billing rows are then constructed with an
event timestamp sampled strictly inside such a gap, so a join that naively resolves to "the
subscriber's current version" would attach the fact to the wrong plan tier or status; only a
correct point-in-time interval join resolves it to the version that was actually current at that
moment. These rows are written to their own dedicated files
(`playback_sessions/part-pathology-midstream-*.parquet`,
`billing_ledger/part-pathology-midstream-*.parquet`). This simulates a fact event that arrives (or
is reprocessed) late enough that a newer dimension version already exists by the time it lands, a
routine occurrence for anything replayed from a queue or backfilled from an outage. Handled by the
half-open interval point-in-time join predicate in `fct_playback_events.sql` and
`fct_billing_transactions.sql`, which resolves on the fact's own event-time column against
`[effective_from, effective_to)`, not on load order. Manifest: `midstream_join_targets.csv`.

**5. Duplicate business keys.** 1% of billing rows and 1% of signup_funnel rows are re-appended
verbatim under their original `billing_transaction_id` or `signup_id` after normal generation,
simulating an upstream retry that re-posted the same event. Verified counts: 1,515,049 billing
rows collapse to 1,500,100 distinct `billing_transaction_id` values (14,949 duplicated, every
duplicate group exactly size 2 and byte-identical); 70,701 signup_funnel rows collapse to 70,000
distinct `signup_id` values (701 duplicated). Handled in `silver_billing_ledger.sql` and
`silver_signup_funnel.sql`, both of which deduplicate with a `row_number()` window over the
business key before anything downstream sees the row. Manifests: `duplicate_billing_ids.csv`,
`duplicate_signup_ids.csv`.

**6. Out-of-order arrival.** Each playback batch corresponds to a chronological time slice, so
file-write order tracks event-time order by construction. 3% of each batch's rows (3,451,483 rows
total) are then peeled off and re-injected into a later-numbered batch file, 3 to 15 batches
later, so a file written later in the run can contain events with an earlier `session_started_at`
than files written before it. This simulates the routine reality that ingestion order and event
order are not the same thing once retries, queues, or multi-region delivery are involved. Every
incremental fact model watermarks on bronze's `_ingested_at`, never on the fact's own event-time
column, specifically so this pathology cannot cause a late-arriving-but-old-timestamped row to be
silently skipped by an incremental run. Manifest: `out_of_order_summary.json`.

**7. Null join keys.** 2% of playback rows get `device_id = NULL`, simulating a client that failed
to report its device (a routine gap in telemetry, not a system failure). Handled by
`fct_playback_events.sql` coalescing a missing `device_sk` resolution to the unknown member's
surrogate key rather than dropping the row; the resulting unknown-member `device_sk` share of the
fact (2.0% of 119,640,099 rows) matches the null `device_id` rate in silver exactly.

**8. Same-day multiple attribute changes.** 500 subscribers are guaranteed at least two follow-on
profile-change events forced onto the same calendar date, with distinct, randomly ordered
microsecond timestamps. This simulates a subscriber changing plan and status twice in one day
(a plan swap immediately followed by a pause, for instance), which stresses whether a dimension
build actually versions on distinct instants within a day rather than collapsing to one row per
day. Handled by `dim_subscriber.sql` treating `effective_from` at full microsecond precision
(inherited from silver) so distinct intraday changes become distinct sub-day SCD versions, with a
`row_number()` tie-break (order by `_batch_id` descending, then `change_event_id` descending,
keep rank 1) reserved for the case of two changes at the exact identical instant. Manifest:
`same_day_change_subscribers.csv`.

**9. Mid-stream schema drift.** Playback batches are numbered sequentially; batches before batch
9 (40% of the way through the run, boundary at 2024-05-06 15:00:00) have no `playback_quality`
column in their Parquet schema at all, not merely nulls; batches at or after it do. This
simulates a producer adding a field mid-stream without backfilling history, which is exactly how
schema evolution happens in a real event pipeline. A straggler row from pathology 6 that
originated pre-cutoff but lands in a post-cutoff batch file gets `playback_quality` backfilled as
an explicit NULL rather than dropped, matching how a genuinely reprocessed late-arriving record
would look once it reaches a newer schema. A second, incidental schema-width drift surfaced only
at full scale: the midstream pathology-2 file encodes `buffering_count` and `avg_bitrate_kbps` as
int64 while every ordinary batch file has them as int32, which required loosening ingestion's
Arrow concatenation setting to allow the safe int32-to-int64 widening rather than rejecting the
file outright. Manifest: `schema_drift_cutoff.json`.

**10. Mixed soft and hard deletes.** Two disjoint subscriber subsets, 150 each. Soft-deleted
subscribers get a guaranteed final change event with status `cancelled` or `deleted`, a broader
raw status domain than the finalized gold `dim_subscriber.status` domain (`trial`, `active`,
`paused`, `churned`, `unknown`), so mapping the soft-delete signal onto the business's actual
churned state is explicitly downstream work, not something the generator resolves itself. This
simulates the common pattern of a source system marking a record deleted rather than removing it.
Hard-deleted subscribers instead simply stop appearing in any output after a per-subscriber
cutoff timestamp, no tombstone at all, every fact generator truncates its sampling window for
that subscriber at the cutoff. This simulates a genuine data-erasure event (a GDPR-style deletion
request, for instance) where the record is not retained anywhere downstream, including in later
event streams. Manifests: `soft_deleted_subscribers.csv`, `hard_deleted_subscribers.csv`.

**11. Malformed rows.** 0.3% of playback rows (360,201 rows) get one of three corruptions, chosen
roughly evenly: negative `watch_duration_seconds`, `session_ended_at` before
`session_started_at`, or `session_started_at` pushed 1 to 300 days into the future (capped under
`dim_date`'s 2027-12-31 upper bound). This simulates client-side clock skew and buggy telemetry,
both routine in any large-scale event collection system. The future-timestamp flavor is
deliberately never applied to a hard-deleted subscriber's rows, so it can never plant a
fabricated post-cutoff row that would contradict pathology 10's "stops appearing entirely"
guarantee. Handled by three narrow rejection models
(`int_playback_rejected_negative_duration.sql`, `int_playback_rejected_ended_before_started.sql`,
`int_playback_rejected_future_timestamp.sql`) that quarantine these rows into
`silver_playback_sessions_rejected` rather than letting them reach the fact table. Manifest:
`malformed_playback_events.csv`.

## The title catalog_add timing bug

This is worth telling in full because it is exactly the kind of thing that makes a dataset
trustworthy: not a claim that the data is clean, but a record of a real defect, how it was found,
and proof that the fix actually worked.

**What went wrong.** `dim_title`'s design assumes titles arrive from a controlled catalog feed
ahead of playback, which is why, unlike `dim_subscriber`, it has no late-arriving self-heal
mechanism: an unseen title is supposed to be a rare edge case that falls back to the unknown
member. The original `generate_titles()` drew each title's `catalog_add_at` from the same
`[platform_launch, now)` span that `generate_playback_events()` and `generate_watchlist_events()`
draw their own session timestamps from, with the title sampled for any given playback row
completely independent of that title's own `catalog_add_at`. Both playback's earliest batch and
many subscribers' earliest possible signups land right at `platform_launch` (the subscriber
growth curve is front-loaded, so real density sits there), so a title whose `catalog_add_at`
happened to land late in the shared span could easily have playback sessions timestamped years
before it was ever added to the catalog.

**How it was found.** Caught during review of `fct_playback_events`, not at the original build:
`title_sk` resolved to the unknown member on 20,646,958 of 119,640,099 rows, 17.3% of the entire
fact. At first this was logged in `open-questions.md` as a "flagged, built as specified" join-miss
finding rather than diagnosed as a bug. A direct check against the real data settled it: 4,833 of
5,000 titles had at least one playback session earlier than their own first `catalog_add` event.
One sampled title, `tt_00221`, had its first tracked catalog version dated 2026-08-03, the day
before the generator's own "now" cutoff, meaning nearly the entire run's worth of playback for
that title predated its own catalog entry. At 4,833 of 5,000 titles, this was not the rare
"unseen title" edge case the design assumed. It was a generator defect large enough to make
title-level analysis on the fact table meaningless for most rows, and shipping it as "implemented
exactly as specified" instead of going back to the generator was, in its own words, the
embarrassing part.

**How it was fixed.** `generation/config.py` gained `CATALOG_SEED_LEAD_TIME` (180 days) and
`CATALOG_SEED_BUFFER` (1 day). `generate_titles()` now draws each title's first `catalog_add_at`
from `[platform_launch - lead_time - buffer, platform_launch - buffer]`, a window that ends
strictly before `platform_launch`, which is always earlier than the earliest instant playback or
watchlist can produce a timestamp, since both are anchored to start exactly at `platform_launch`.
The ordering holds by construction now, not by chance. Follow-on metadata-update events are
untouched: the same cursor-based spread over `[catalog_add_at, now)` still runs, just starting
from the new, earlier `catalog_add_at`.

**Verification.** The full dataset was regenerated with the same seed (103.5 seconds, identical
row counts to the prior run, confirming determinism held across the fix) and every one of the
eleven pathology checks in `sanity_check.py` still passed unchanged. A direct query against the
regenerated data found 0 of 5,000 titles with a playback session before their own first
`catalog_add` (down from 4,833), checked at the row level as well: 0 of 120,000,300 playback rows
violate their title's `catalog_add_at` (down from a defect that had touched the overwhelming
majority of titles). After forcing a genuine reload of just the `bronze_title_events` table
(bronze is otherwise append-only and does not naturally support replacing one source table's
history without touching the other eight) and rebuilding the affected staging, silver, dimension,
and fact models with a full refresh, `title_sk` resolved to the unknown member on 0 of 119,640,099
`fct_playback_events` rows, down from 20,646,958. Every `title_id` referenced by playback now
exists in `dim_title` with a `catalog_add_at` at or before that row's `session_started_at`, so
there is no remaining legitimate "unseen title" case left to explain away in this dataset.

The lesson recorded alongside the fix: root-causing a defect correctly is not the same as fixing
it, and flagging something in `open-questions.md` is not a substitute for going back to the
source when the source is what is actually broken.
