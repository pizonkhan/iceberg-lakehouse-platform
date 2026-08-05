# Surrogate key strategies

Every dimension in this project needs a key that a fact table can carry as a foreign key. The
obvious candidate, the business's own identifier for the entity (`subscriber_id`, `title_id`,
`plan_id`), turns out not to work once a dimension has more than one row per entity. This
document explains why, works through the three candidate mechanisms (hash, sequence, natural
key), and reproduces the actual collision-probability arithmetic this project derived for its
largest dimension.

Column-level source of truth: `.notes/modeling.md`, "Surrogate key strategy". Real model code:
`transform/lakehouse/models/marts/dimensions/dim_subscriber.sql`, `dim_title.sql`, `dim_plan.sql`,
`dim_device.sql`, `dim_payment_method.sql`.

## The problem, stated precisely

A fact row must reference exactly one version of a dimension row, not the entity in the
abstract. `dim_subscriber` is Type 6: a single subscriber accumulates a new row every time
`plan_tier` or `status` changes. Real build output has 50,000 distinct `subscriber_id` values
spread across 125,616 total rows (decisions.md, 2026-08-04 entry, "125,616 rows (125,615 real
versioned rows plus the one unknown member)"). If a fact stored `subscriber_id` as its foreign
key, a join to `dim_subscriber` would return every version that subscriber ever had, not the one
that was true when the playback session happened. The key a fact carries has to identify a row,
and once a dimension is versioned, "row" and "entity" are different things.

So every dimension needs a key that is:

- **Unique per row** the dimension actually materializes (one per version, for a versioned dim;
  one per entity, for a non-versioned dim).
- **Deterministic**, so the same input always produces the same key regardless of which run, which
  loader, or how much of the table gets rebuilt.
- **Stable across a full rebuild**, so a fact written last week still resolves correctly against a
  dimension rebuilt this week.
- Cheap enough to carry at fact-table volume (`fct_playback_events` is ~120M rows, so whatever key
  shape gets chosen is repeated 120M times per foreign key column).

Three mechanisms compete for this role: a database sequence, the natural key itself, and a
deterministic hash of chosen columns. This project uses a deterministic md5 hash for every
dimension except `dim_date`. The reasoning for ruling out the other two is worth reproducing in
full, because it is specific to running a dimensional model on Trino over Iceberg rather than a
generic argument.

## Why not a sequence

A sequence (an auto-incrementing integer, `IDENTITY` in a warehouse like Snowflake or Postgres)
is the default surrogate-key mechanism in most dimensional-modeling material, because Kimball-era
guidance assumes a database that has one. Trino over Iceberg does not. There is no sequence
object and no identity column type. Modeling.md states the consequence directly:

> Why hash and not a sequence: Trino over Iceberg has no sequence or identity object, so a
> sequence would have to be faked with `row_number()` at build time. That is nondeterministic
> across rebuilds and across parallel loads: a full refresh reassigns every key and silently
> severs every fact already written. Hash keys are pure functions of the data, so an incremental
> merge, a full rebuild, and a backfill all produce the same key, and fact loads never have to
> wait on a dimension load to learn key assignments.

The mechanism of the failure is worth spelling out, since "nondeterministic" is doing a lot of
work in that sentence. `row_number() over (order by ...)` only returns a stable integer if the
ordering key is unique and the input set never changes. Neither holds here across a real
lifecycle: a full-refresh dimension build reprocesses its entire source every run (all six
dimensions in this project are full-refresh, per modeling.md's incremental-conversion section),
and any change to the row count or row order between two builds, whether from late-arriving data,
a tie-break reordering, or simply a different number of rows surviving a filter, reassigns
`row_number()`'s output from that point forward. A fact table that stored `subscriber_sk = 41`
last night now finds row 41 belongs to a different subscriber. The foreign key does not raise an
error; it silently points at the wrong dimension row. A hash of the row's own content has no such
dependency on what else happens to be in the table or in what order: `md5(subscriber_id,
effective_from)` for a specific subscriber's specific version is the same value whether it is
computed alone, in a batch of ten, or in a full rebuild of all 125,616 rows.

## Why not the natural key alone

The natural key is the right identifier for the entity, and this project keeps it on every
dimension as `<entity>_id`. But it cannot serve as the surrogate key once a dimension versions,
for the same reason a sequence fails in the other direction: a natural key is not unique per row.
`dim_subscriber` maps one `subscriber_id` to as many rows as that subscriber has had tracked
changes. Modeling.md:

> Why hash and not the natural key: a versioned dimension maps one natural key to many rows, and
> facts must pin one version. The natural key alone cannot do that. Composite natural keys on
> facts also bloat 120M-row join columns.

The second sentence covers the fallback that "use the natural key plus `effective_from` as a
composite fact foreign key" tempts. It technically identifies a version, but it means every fact
row carries two join columns per dimension instead of one, and at `fct_playback_events` scale
(~120M rows, three dimension references each) that is a real storage and join-plan cost for no
benefit over hashing the same two columns down to one.

Non-versioned dimensions (Type 1 `dim_device`, Type 3 `dim_plan`) do not have this problem: one
natural key really does map to one row. Their surrogate key is still a hash, but of the natural
key alone, not because the natural key would fail as a join column, but for uniformity: every
fact-to-dimension join in the model uses the same key shape (`VARCHAR(32)`, lowercase md5 hex)
regardless of which dimension it targets, so nothing about a fact query has to know whether the
target dimension happens to version.

## The mechanism: deterministic md5 hash

Every hash-keyed dimension derives its surrogate key by calling
`dbt_utils.generate_surrogate_key([component_1, component_2, ...])`, never by hand-writing
`md5(...)`. Key composition, per modeling.md:

- Non-versioned dims (Type 1, Type 3, junk): hash of the natural key alone, or of the full
  attribute combination for the junk dimension. One entity, one immutable key.
- Versioned dims (Type 2, Type 6): hash of `(natural_key, effective_from)`. One key per version.

The real calls, quoted directly from the model files:

`dim_plan.sql` (Type 3, single component):

```sql
{{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_sk,
```

`dim_device.sql` (Type 1, single component), with the builder's own note on why a
single-element list still goes through the macro rather than a hand-written `md5()`:

```sql
-- single-component key: dbt_utils.generate_surrogate_key(['device_id'])
-- reduces to md5(device_id) for a one-field list, matching the
-- contract exactly while keeping key generation on the shared macro.
{{ dbt_utils.generate_surrogate_key(['device_id']) }} as device_sk,
```

`dim_title.sql` (Type 2, versioned, two components):

```sql
{{ dbt_utils.generate_surrogate_key(['title_id', 'effective_from']) }} as title_sk,
```

`dim_subscriber.sql` (Type 6, versioned, two components, plus the same macro reused for row_hash
change detection and for the unknown and inferred rows):

```sql
{{ dbt_utils.generate_surrogate_key(['v.subscriber_id', 'v.effective_from']) }}
    as subscriber_sk,
...
{{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash
```

`dim_payment_method.sql` (junk dimension, four components, no natural key at all, so the
combination of attributes is the identity):

```sql
{{ dbt_utils.generate_surrogate_key([
    'payment_type',
    'is_promo_applied',
    'is_retry',
    'is_autopay'
]) }} as payment_method_sk,
```

Every dimension additionally carries exactly one unknown-member row, key `md5('-1')` (or the
macro's output over the sentinel combination for the natural-key-less junk dimension), so a fact
that cannot resolve a real dimension member still has a valid, joinable key rather than a NULL
foreign key.

### What the macro actually does, and where the documented formula was wrong

The installed package is dbt_utils 1.4.1. Its real macro
(`dbt_packages/dbt_utils/macros/sql/generate_surrogate_key.sql`):

```sql
{%- macro default__generate_surrogate_key(field_list) -%}

{%- if var('surrogate_key_treat_nulls_as_empty_strings', False) -%}
    {%- set default_null_value = "" -%}
{%- else -%}
    {%- set default_null_value = '_dbt_utils_surrogate_key_null_' -%}
{%- endif -%}

{%- set fields = [] -%}

{%- for field in field_list -%}

    {%- do fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
    ) -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{{ dbt.hash(dbt.concat(fields)) }}

{%- endmacro -%}
```

Two things fall out of reading this: the delimiter between components is the literal string
`'-'`, and the NULL placeholder is `'_dbt_utils_surrogate_key_null_'`, not the more guessable
values (`'||'` and `'_null_'`) that this project's own contract originally described. Modeling.md
used to give an illustrative formula, `md5(a || '||' || b...)`, and claimed it matched the macro.
It did not. The discrepancy surfaced during the `dim_plan` build and is now recorded permanently
in modeling.md itself, in the section every builder reads before generating a key:

> Always call the macro, never hand-compute the hash: the illustrative formula this section used
> to give (`md5(a || '||' || b...)`, NULL to `'_null_'`) does not match the installed dbt_utils
> 1.4.1 macro's real behavior (delimiter `'-'`, NULL placeholder
> `'_dbt_utils_surrogate_key_null_'`, confirmed against the actual macro source and cross-checked
> with hashlib during the dim_plan build). For single-component never-NULL keys this made no
> difference, but any multi-component key, or any unknown-member row, or any test that
> reconstructs an expected key by hand instead of calling the macro, must use the macro's actual
> output as ground truth, not the formula above.

Why the discrepancy was invisible on `dim_plan` specifically: `plan_sk` is
`generate_surrogate_key(['plan_id'])`, a single component that is never NULL, so the macro's
`for` loop never emits the `'-'` delimiter (it only appends between fields, and there is only one
field) and the `coalesce(...)` never falls through to the NULL placeholder. The macro's output
for that call is therefore byte-identical to a raw `md5(plan_id)` computation regardless of which
delimiter or placeholder the macro uses internally, which is exactly why the bug sat undetected
until a multi-component key (`dim_subscriber.subscriber_sk`, hashing two components) was checked
against Trino directly. `.notes/decisions.md`, 2026-08-04, records that check:

> Confirmed (independently of the dim_plan and dim_title builders' entries in open-questions.md)
> that dbt_utils.generate_surrogate_key's real delimiter is '-' and real NULL placeholder is
> '_dbt_utils_surrogate_key_null_', not the '||' / '_null_' modeling.md's prose describes.
> Cross-checked subscriber_sk for subscriber_id 'sub_000001', effective_from
> '2023-05-01 16:20:01.962276' directly in Trino: to_hex(md5(to_utf8('sub_000001' || '-' ||
> '2023-05-01 16:20:01.962276'))) lowercased equals the model's actual output,
> 8db31d15af4de707bcc8a37a72fedc7f, confirming the macro's real delimiter behavior end to end.

That exact value was reproduced independently for this document (`python3 -c "import hashlib;
print(hashlib.md5(('sub_000001' + '-' + '2023-05-01 16:20:01.962276').encode()).hexdigest())"`
also returns `8db31d15af4de707bcc8a37a72fedc7f`). The practical rule this produced, stated in
modeling.md's own words, is not "here is the corrected formula, hand-compute against that
instead": it is "always call the macro, never hand-compute", because the macro is itself the
contract now. Any test or downstream code that reconstructs an expected key by formula, rather
than by invoking `dbt_utils.generate_surrogate_key` with the identical field list, is one dbt_utils
minor-version bump away from silently diverging from what the models actually write.

`dim_payment_method.sql` documents the same lesson from the opposite direction, showing that even
knowing the macro casts every component to varchar before hashing, hand-verifying the boolean
representation still matters:

```sql
-- dbt_utils.generate_surrogate_key casts each component to varchar
-- before hashing (see dbt_packages/dbt_utils/macros/sql/
-- generate_surrogate_key.sql), and Trino's cast(boolean as varchar)
-- already produces lowercase 'true' / 'false', so passing the three
-- flag columns straight through satisfies modeling.md's "booleans
-- cast to 'true'/'false' text before hashing" without a redundant
-- explicit case expression: verified directly in Trino,
-- cast(true as varchar) = 'true', cast(false as varchar) = 'false'.
```

And the Trino-specific hash implementation underneath `dbt.hash(...)`
(`dbt/include/trino/macros/utils/hash.sql` in the installed adapter) matters too, because Trino's
native `md5()` returns `varbinary`, not the lowercase hex string modeling.md's `VARCHAR(32)`
contract requires:

```sql
{% macro trino__hash(field) -%}
    lower(to_hex(md5(to_utf8({{field}} as varchar)))))
{%- endmacro %}
```

(`to_hex` then `lower`, converting the binary digest into the lowercase hex form every surrogate
key column in this project actually stores.)

## The math: collision probability for the worst-case table

`dim_subscriber` is the largest hash-keyed dimension and the one modeling.md picks for the
worst-case collision arithmetic. This section reproduces that derivation step by step rather than
just citing the final number, and every intermediate value below was independently recomputed
with Python's `hashlib`/plain arithmetic while writing this document, not copied.

### Why the birthday bound is the right model here

The question being asked is: "given `n` surrogate keys, each an md5 digest of distinct input data,
what is the probability that any two of them collide?" That is a *many-items-into-one-keyspace,
pairwise-coincidence* question: `n` draws from a keyspace of size `N`, asking about agreement
between any two of the draws, not about any one draw matching a fixed target. That is exactly the
shape of the classical birthday problem (the chance that two people in a room share a birthday,
not the chance that someone shares a specific person's birthday), and the birthday bound is the
standard tool for it in hash-collision analysis generally (it is the same math used to size UUID
and content-hash collision risk).

It is worth naming the model that would be wrong to reach for instead: the probability that a
hash collides with one *fixed* target value is `n/N` (`n` chances, each independently `1/N`
against a single value). That is a different question, "does any of my n keys equal this one
specific value," and it understates the real risk enormously, because it is linear in `n` while
the real quantity of interest, the number of *pairs* among `n` items that could coincide with each
other, is quadratic in `n` (there are `C(n,2) = n(n-1)/2` such pairs). Using the fixed-target
model here would answer a question nobody is asking and produce a false sense of a smaller margin
than actually exists in the wrong direction (it would even understate an already-negligible
number). The birthday bound is the right approximation precisely because it is built from the
number of *pairs*, not the number of items.

### Deriving the formula

Let `n` be the number of surrogate keys generated (one per dimension row), and `N` be the size of
the hash keyspace (`2^128` for md5). Treat each key as landing uniformly at random in the
keyspace, an idealization that holds well in practice because md5 mixes its input thoroughly even
though it is not cryptographically secure against a deliberate adversary; the concern here is
accidental collision between legitimate, distinct `(subscriber_id, effective_from)` pairs, not a
crafted preimage attack.

1. The number of distinct pairs among `n` keys is `C(n,2) = n(n-1)/2`.
2. Under the uniform-random idealization, any one specific pair collides (both keys land on the
   same value) with probability `1/N`.
3. The expected number of colliding pairs across all `C(n,2)` pairs is therefore:

   `E[collisions] = C(n,2) * (1/N) = n(n-1) / 2N`

4. When this expectation is small (`« 1`), the Poisson approximation says the probability of at
   least one collision is closely tracked by the expectation itself (a small expected count and
   the probability of a nonzero count converge as the expectation shrinks). That gives the working
   formula:

   `P(at least one collision) ≈ n(n-1) / 2N`

5. For `n` in the thousands to millions and `N = 2^128`, `n - 1 ≈ n`, so this simplifies further
   to the form modeling.md states:

   `P ≈ n² / 2N`

This is exactly the derivation modeling.md's "Collision arithmetic" section states as its
starting formula (`P = n(n-1) / 2N`, effectively `n^2 / 2N`), reproduced here with the
intermediate steps shown.

### Plugging in the real numbers

md5 is 128 bits, so the keyspace is:

`N = 2^128 = 340,282,366,920,938,463,463,374,607,431,768,211,456 ≈ 3.40e38`

modeling.md picks `n = 2.0e5` as the planning figure for `dim_subscriber` ("~200,000 rows with
history"), close to but not identical to the real build's 125,616 rows; the arithmetic
deliberately uses the larger, rounder planning number so the margin holds even if the real table
grows.

`n² = (2.0e5)² = 4.0e10`

`2N = 2 × 3.40e38 = 6.80e38`

`P = n² / 2N = 4.0e10 / 6.80e38 = 5.877e-29 ≈ 5.9e-29`

That matches modeling.md's stated result exactly (`P = 5.9e-29`), and matches an independent
recomputation done for this document (`n*(n-1)/(2*2**128)` in Python returns
`5.877442366752667e-29`).

### The 100x headroom check

The same formula, run at 100 times the planned history (`n = 2.0e7`, i.e. 20 million subscriber
versions, a scale this project's actual dimension is nowhere near):

`n² = (2.0e7)² = 4.0e14`

`P = 4.0e14 / 6.80e38 = 5.877e-25 ≈ 5.9e-25`

Still sixteen orders of magnitude below any probability that would concern a production system.
This is the check modeling.md runs specifically to establish that the margin is not fragile to
`dim_subscriber` growing well past its current design point; nothing about the argument depends on
the exact `n` staying near 200,000.

### Why md5 (128 bits) and not a narrower hash

A 64-bit hash would still be adequate at this scale. Using `N = 2^64 = 1.8446744e19` at the same
`n = 2.0e5`:

`P = 4.0e10 / (2 × 1.8446744e19) = 4.0e10 / 3.69e19 = 1.084e-9 ≈ 1.1e-9`

Independently recomputed as `1.084196751474642e-09`, matching modeling.md's `1.1e-9`. That is
still an acceptable margin, but modeling.md keeps md5 anyway:

> md5 is the dbt_utils default, reproducible identically in Trino and PyIceberg, and the 16-byte
> hex cost is immaterial at 200k dimension rows. Facts store the same 32-char keys; at 120M rows
> this is the one place the width costs anything, accepted for determinism.

The tradeoff is explicit: md5's extra width (32 hex characters versus, say, 16 for a 64-bit hash)
costs storage and join-key bytes at fact scale, but the choice is driven by ecosystem consistency
and the dbt_utils default, not by a shortfall in the narrower hash's collision margin.

## Failure modes and counter-indications

**Non-deterministic components break the entire justification.** The argument for hashing over a
sequence rests on "hash keys are pure functions of the data." If a builder ever included a
volatile value inside the hashed component list, such as `loaded_at` or `current_timestamp`, the
resulting key would change on every rebuild, which is precisely the failure mode this project
rejected a `row_number()`-based sequence for in the first place. This is why modeling.md is
explicit that `loaded_at` and bookkeeping columns are "never used in any join or hash," and why
every real model computes its surrogate key only from natural-key and versioning columns.

**Hand-computed keys diverge from the macro.** Covered above: any test or downstream code that
reconstructs an expected key by formula instead of calling
`dbt_utils.generate_surrogate_key` risks producing a value that does not match what the model
actually wrote, especially for multi-component keys where the delimiter and NULL placeholder
matter.

**A hash is the wrong choice when order or human legibility matters more than uniformity.**
`dim_date` is this project's own counter-example: its surrogate key, `date_key`, is a plain
`INTEGER` in `YYYYMMDD` form, not a hash. Modeling.md states the reasoning directly in the naming
conventions section: "a human-readable, order-preserving date key is worth more than hash
uniformity and the domain can never collide." A hash of a date would satisfy uniqueness but throw
away the ability to range-filter or partition-prune on the key itself, and the domain (calendar
days) is small, closed, and never versions, so none of the reasons to hash (versioning, avoiding a
faked sequence) apply.

**Width has a real cost at fact scale, even though it never threatens correctness.** A 32-character
`VARCHAR` foreign key is four to eight times wider than an integer surrogate key would be. At
`fct_playback_events`'s ~120M rows with three dimension foreign keys per row, that is real bytes
under management, accepted deliberately (see the md5-versus-64-bit tradeoff above) rather than
overlooked.

**The collision margin degrades only at scales this project will never reach.** Collision
probability crosses into a range worth worrying about only when `n` approaches `sqrt(2N)`, which
for `N = 2^128` is on the order of `1.3e19` keys. No realistic dimension in this or any comparable
warehouse gets remotely close to that; the "failure mode" for the hash strategy is not
"insufficient margin," it is one of the two mistakes above (a non-deterministic component, or a
hand-computed key that silently drifts from the macro).

## Verifying it

**Data tests, from this repo's actual schema files.** Every hash-keyed dimension's surrogate key
column carries `not_null` and `unique` tests. From `_dim_subscriber.yml`:

```yaml
- name: subscriber_sk
  tests:
    - not_null
    - unique
```

The identical pattern appears on `title_sk` in `_dim_title.yml`. Run them with
`dbt test --select dim_subscriber dim_title dim_plan dim_device dim_payment_method`, or the
project-wide `make test`.

**Direct cross-checks already performed against this repo's real data**, recorded in
`.notes/decisions.md` and reproducible by anyone with the built tables:

- `dim_plan`, 2026-08-04: "Surrogate keys cross-checked directly against python hashlib.md5: real
  row plan_sk for plan_00 equals md5('plan_00') = 42b29552561d07fa699a8f0d388357e1, unknown row
  plan_sk equals md5('-1') = 6bb61e3b7bce0931da574d19d1d82c88, both bit-for-bit exact matches."
  Independently reproduced for this document: `hashlib.md5(b'plan_00').hexdigest()` and
  `hashlib.md5(b'-1').hexdigest()` both return the quoted values.
- `dim_device`, 2026-08-04: "unknown row device_sk is 6bb61e3b7bce0931da574d19d1d82c88, which is
  exactly md5('-1') computed independently, matching every other unknown-member dimension in this
  project." Note that this is the same value as `dim_plan`'s unknown row, which is the expected
  behavior of a deterministic function applied to the same input (`'-1'`) across dimensions, not a
  coincidence.
- `dim_subscriber`, 2026-08-04: the multi-component cross-check quoted in full above, confirming
  the macro's real `'-'` delimiter directly against a live Trino query on `subscriber_id
  'sub_000001'`.

**Idempotency, as indirect verification that the keys really are pure functions of the data.**
Two consecutive `dbt build --select dim_subscriber` runs against unchanged silver input produced
an identical row checksum across three separate full rebuilds (`.notes/decisions.md`,
2026-08-04); `dim_plan`, `dim_title`, and `dim_device` each report the same byte-identical result
across repeated builds (excluding only `loaded_at`, which is wall-clock audit metadata by
design). This is the practical demonstration of the exact property that ruled out a
`row_number()`-based sequence in the first place: rebuilding the table does not reassign any key.

**A live query anyone can run against the built warehouse**, mirroring the cross-check pattern
above for any dimension:

```sql
select subscriber_sk, subscriber_id, effective_from
from dim_subscriber
where subscriber_id = 'sub_000001'
order by effective_from
limit 1;
-- compare to: select lower(to_hex(md5(to_utf8('sub_000001' || '-' || '<effective_from>'))))
```

If the two values match, the model is computing the key exactly as the macro (and this document's
derivation) describes.
