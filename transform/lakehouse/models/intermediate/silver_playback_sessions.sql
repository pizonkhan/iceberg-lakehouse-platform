-- silver playback session stream, quality-gated: rows with negative
-- watch_duration_seconds, session_ended_at before session_started_at, or
-- a session_started_at after ingestion are routed to
-- silver_playback_sessions_rejected instead of landing here. The filter
-- is written as direct column comparisons (see macros/playback_malformed_predicate.sql
-- for why, not as a derived column) so Trino can push it down to the
-- Iceberg scan on this ~120M-row table.
--
-- null device_id rows (failed source-side fingerprinting) are left in
-- this clean model with device_id null, not filtered and not backfilled;
-- gold resolves them to the unknown device member at fact-load time.
--
-- out-of-order arrival needs no handling here: rows land wherever bronze
-- put them, ordering is a query-time concern, not a load-time one.

select
    playback_session_id,
    subscriber_id,
    title_id,
    device_id,
    session_started_at,
    session_ended_at,
    watch_duration_seconds,
    playback_percent,
    buffering_count,
    avg_bitrate_kbps,
    playback_quality,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ ref('stg_playback_sessions') }}
where not {{ playback_malformed_predicate() }}
