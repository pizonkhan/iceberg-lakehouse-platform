# ADR-007: Deterministic hash surrogate keys, not a sequence or the natural key

## Status

Accepted, 2026-08-04.

## Context

Every hash-keyed dimension in this project needs a surrogate key: a stable identifier a fact can
pin to, independent of the business key's own lifecycle. The two conventional alternatives to a
hash are an auto-incrementing sequence (or identity column) and using the natural key directly.

Trino over Iceberg has no sequence or identity object. Faking one would mean assigning keys with
`row_number()` at build time.

## Decision

Every hash-keyed dimension derives its surrogate key by calling
`dbt_utils.generate_surrogate_key([component_1, component_2, ...])`, never a hand-written hash
formula. Non-versioned dimensions (Type 1, Type 3, the junk dimension) hash the natural key alone,
or the full attribute combination for the junk dimension. Versioned dimensions (Type 2, Type 6)
hash `(natural_key, effective_from)`, one key per version.

Builders are required to call the macro itself rather than reproduce its formula by hand: an early
version of this project's own contract (`modeling.md`) stated the formula as
`md5(a || '||' || b...)` with `'_null_'` for NULL, which does not match the installed dbt_utils
1.4.1 macro's real behavior (delimiter `'-'`, NULL placeholder
`'_dbt_utils_surrogate_key_null_'`), confirmed against the macro's own source and cross-checked with
`hashlib` during the `dim_plan` build. Any hand-reconstructed hash, in a model or in a test, would
have silently diverged from the macro's real output for any multi-component key or any test that
tries to reconstruct an expected key independently.

## Alternatives Considered

- **A faked sequence via `row_number()` at build time.** Rejected because it is nondeterministic
  across rebuilds and across parallel loads: a full refresh reassigns every key from scratch, which
  would silently sever every fact already written against the previous assignment. A hash key is a
  pure function of its input data, so an incremental merge, a full rebuild, and a backfill all
  produce the identical key for the identical row, and a fact load never has to wait on a dimension
  load to learn what key a given version was assigned.
- **The natural key directly, no surrogate.** Rejected for the versioned dimensions specifically: a
  Type 2 or Type 6 dimension maps one natural key to many rows over time, and a fact must pin to one
  specific version, which the natural key alone cannot express. It was also rejected on grounds of
  cost at scale: a composite natural key carried directly on a 120M-row fact bloats every join
  column compared to a fixed 32-character hash.
- **A shorter, cheaper hash** (a 64-bit hash instead of md5's 128-bit output). Considered on
  collision-arithmetic grounds and found acceptable even at 100x this project's planned history (a
  64-bit hash's collision probability at that scale is roughly 1.1e-9, still negligible), but
  rejected in favor of md5 anyway: md5 is the dbt_utils default, so choosing anything else would
  mean maintaining a nonstandard hashing convention across silver, gold, and every test that needs
  to reconstruct a key, for a cost (32 hex characters instead of some shorter encoding) that is
  immaterial at this project's largest dimension size (~200,000 rows with history).

## Consequences

- Collision risk is real but negligible by direct calculation, not just assumed: at
  `dim_subscriber`'s worst-case size (~200,000 rows with history), the birthday-approximation
  collision probability is 5.9e-29; at 100x that history, still 5.9e-25.
- Every fact fixed-width foreign key column carries a 32-character hex string rather than a narrower
  integer. At `fct_playback_events`'s ~120M rows, this is the one place the width has a measurable
  cost, accepted deliberately for determinism rather than optimized away.
- The unknown-member convention (surrogate key `md5('-1')`, natural key `'-1'`) depends on every
  hash-keyed dimension having a literal natural key to hash. The junk dimension (`dim_payment_method`)
  has no natural key column by design, so its unknown row's surrogate key is not `md5('-1')`; it is
  generated the same way as every real row, hashing the sentinel attribute combination. This is a
  documented exception to the general rule, not an oversight, but it means "every unknown row shares
  the literal `md5('-1')` value" is not universally true across this project's dimensions.
- Because every builder is required to call the macro rather than hand-compute a hash, any future
  dbt_utils version upgrade that changes the macro's delimiter or NULL placeholder would silently
  change every surrogate key in the project on the next full rebuild. Nothing currently guards
  against that beyond the version pin on dbt_utils itself.
