# ADR-006: Synthetic, seeded, deterministic data generator with deliberately injected pathologies

## Status

Accepted, 2026-08-04.

## Context

This project needed a dataset large and messy enough to genuinely exercise a medallion pipeline:
late-arriving dimension members, out-of-order fact arrival, duplicate business keys, mid-stream
schema drift, mixed soft and hard deletes, malformed rows, and more, at a scale (120M+ playback
rows at full scale) that would actually stress Trino's memory limits rather than just look
plausible on paper. A real or scraped streaming-service dataset of this shape and scale, with these
specific pathologies present and locatable, does not exist publicly.

## Decision

Build a purpose-written, seeded, deterministic generator (`generation/`) rather than sourcing or
scraping real data. Every random draw traces back to a single `SEED` constant
(`generation/config.py`), and every RNG stream is derived through `generation/rng.py:child_rng`,
which folds a CRC32 of a stable string label (for example `"playback.batch.7"`) into a NumPy
`SeedSequence`. Labels are chosen deliberately over sequential spawn order so that re-running one
stage in isolation during development never perturbs an unrelated stream's output.

The generator injects eleven required pathologies on purpose, each with a fixed fraction or count
and a manifest listing the exact rows it constructed (`_pathology_manifest/`), so downstream
builders can verify their handling of a pathology against a known-correct answer rather than
against whatever the data happened to contain: late-arriving dimension members, late-arriving facts
against a historical dimension version, duplicate business keys, out-of-order arrival, null join
keys, same-day multiple attribute changes, mid-stream schema drift, mixed soft and hard deletes, and
malformed rows (numbering in the work package skips 3 and 4; no pathologies were specified there).

## Alternatives Considered

- **A real or scraped streaming-service dataset.** Would have offered more organic realism in the
  parts of the data that were not the point of the exercise (viewing pattern distributions, title
  metadata). Rejected because no available dataset carries the specific, verifiable pathologies this
  project needed to demonstrate a medallion pipeline's handling of: late arrival, out-of-order
  arrival, schema drift, and the rest are not naturally labeled or guaranteed present in any public
  dataset, which would have made "did the pipeline handle this correctly" unanswerable without first
  building a detection and labeling layer on top of someone else's data, at which point most of the
  benefit of using real data (not having to build anything) is already gone.
- **A non-deterministic generator** (unseeded, or seeded but without the label-keyed RNG design).
  Rejected on this project's own engineering standard (every random generator explicitly seeded,
  seed lives in config) and because non-determinism would have broken every downstream idempotency
  check this project relies on: dimension and fact builds are repeatedly verified by rebuilding
  twice and diffing a checksum of every column except `loaded_at`, which requires the upstream data
  to be identical, not merely similar, across regenerations.
- **Sequential-spawn RNG keying** (each stream gets the next `SeedSequence.spawn()` output in
  invocation order) instead of label-keyed. Considered and rejected: it makes a given stream's
  output depend on everything that ran before it in the same invocation, so re-running one stage in
  isolation during development (a routine need, exercised repeatedly across this project's build
  history) would silently perturb every other stream's output, defeating the point of seeding at
  all for anything short of a full, identical, from-scratch run.

## Consequences

- True determinism was verified empirically, not assumed: running `--scale small` twice and diffing
  the SHA-256 of every output Parquet file produced byte-identical results both times.
- A generator defect (`generation/titles.py` drawing catalog add times from the same shared span
  as playback timestamps, rather than a window guaranteed to precede it) shipped into the dataset
  and was only caught in a later review pass, by which point it had made 17.3% of
  `fct_playback_events` resolve to the unknown title member. Fixing it required regenerating the
  full dataset, reloading the one affected bronze table, and rebuilding four downstream models.
  This is a real cost of owning the generator: bugs in it are indistinguishable from bugs in the
  pipeline until specifically investigated, and a synthetic dataset's row-count estimates
  (`modeling.md`'s illustrative counts) can turn out optimistic against what the generator actually
  produces, as happened with `dim_title`.
- The dataset's realism is bounded by what the generator's authors thought to model. Distributions,
  correlations, and edge cases not explicitly coded into `generation/` simply do not exist in the
  data, unlike a real dataset where they might appear unprompted.
- Every number this project's build history cites (row counts, memory ceilings, join-miss rates) is
  specific to this seed and this generator version. Changing `SEED` or any generation parameter
  invalidates those numbers and would require re-verifying them, not just re-running the pipeline.
