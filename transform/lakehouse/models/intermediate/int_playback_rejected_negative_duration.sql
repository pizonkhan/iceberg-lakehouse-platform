-- one of three internal helper tables feeding silver_playback_sessions_rejected,
-- not a deliverable in its own right. Split out to its own
-- single-predicate, single-scan query, and deliberately narrow (see
-- silver_playback_sessions_rejected.sql for the full column-width
-- rationale): this single-node Trino's 1.5GB per-query memory cap could
-- not be reliably kept under with the full 16-column row width across a
-- ~120M-row scan, regardless of predicate selectivity, session
-- concurrency settings, or date-range chunking (Iceberg file pruning on
-- session_started_at is defeated by this table's deliberately injected
-- out-of-order pathology, confirmed with EXPLAIN (TYPE IO), so chunking
-- by date does not reduce the scanned volume). Column width was the only
-- lever that produced a reliable, repeatable fit. See decisions.md.

select
    playback_session_id,
    subscriber_id,
    title_id,
    session_started_at,
    session_ended_at,
    watch_duration_seconds,
    'negative_duration' as rejection_reason,
    _ingested_at,
    _batch_id
from {{ ref('stg_playback_sessions') }}
where watch_duration_seconds < 0
