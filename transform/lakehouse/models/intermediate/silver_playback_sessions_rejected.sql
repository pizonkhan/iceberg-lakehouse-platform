-- companion reject stream for silver_playback_sessions: every row the
-- quality gate excluded from the clean model, kept alongside it (rather
-- than dropped silently) so the exclusion is observable and auditable. A
-- dbt test asserts this model's row count plus silver_playback_sessions'
-- row count equals stg_playback_sessions' row count.
--
-- unions three upstream helper tables (int_playback_rejected_*), one per
-- malformation type, instead of scanning stg_playback_sessions three
-- times in this one query: on this single-node Trino, a single query
-- containing a UNION ALL of three scans against the ~120M-row playback
-- table reliably exceeded the 1.5GB per-query memory cap even though
-- each branch's predicate alone does not (and even though the equivalent
-- single-scan, single-predicate query on the clean side, see
-- silver_playback_sessions.sql, fits comfortably). Materializing each
-- branch separately first means each of those three scans pays the same
-- proven-safe memory cost as the clean model, and this final union reads
-- three already-small tables (low hundreds of thousands of rows each)
-- rather than the wide source table. See decisions.md.

select * from {{ ref('int_playback_rejected_negative_duration') }}
union all
select * from {{ ref('int_playback_rejected_ended_before_started') }}
union all
select * from {{ ref('int_playback_rejected_future_timestamp') }}
