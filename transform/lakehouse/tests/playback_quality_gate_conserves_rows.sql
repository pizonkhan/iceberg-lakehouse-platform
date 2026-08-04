-- proves the playback quality gate is a partition, not a leak: every row
-- staged lands in exactly one of the clean model or the rejects model.
-- a non-empty result fails the test.

with staged as (
    select count(*) as n from {{ ref('stg_playback_sessions') }}
),

clean as (
    select count(*) as n from {{ ref('silver_playback_sessions') }}
),

rejected as (
    select count(*) as n from {{ ref('silver_playback_sessions_rejected') }}
)

select
    staged.n as staged_rows,
    clean.n as clean_rows,
    rejected.n as rejected_rows
from staged
cross join clean
cross join rejected
where staged.n <> clean.n + rejected.n
