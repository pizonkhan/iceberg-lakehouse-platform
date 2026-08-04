-- one of three internal helper tables feeding silver_playback_sessions_rejected,
-- not a deliverable in its own right. See int_playback_rejected_negative_duration.sql
-- for why this is a separate model and why its columns are deliberately narrow.

select
    playback_session_id,
    subscriber_id,
    title_id,
    session_started_at,
    session_ended_at,
    watch_duration_seconds,
    'ended_before_started' as rejection_reason,
    _ingested_at,
    _batch_id
from {{ ref('stg_playback_sessions') }}
where watch_duration_seconds >= 0
  and session_ended_at < session_started_at
