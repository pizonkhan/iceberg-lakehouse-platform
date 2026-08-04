-- thin 1:1 staging model over the playback session stream, the dominant
-- volume table (~120M rows). No filtering or dedup happens here, that is
-- intermediate's job (the quality gate lives in int_playback_sessions_flagged).
--
-- buffering_count and avg_bitrate_kbps are widened to bigint here rather
-- than left at bronze's declared integer width: see decisions.md, the
-- source generator produced a batch of rows with int64 values for these
-- columns (the pathology-2 midstream file) alongside the int32 main
-- batches, so casting up defensively protects against a future ingest
-- that surfaces the wider type again, at negligible cost.

select
    playback_session_id,
    subscriber_id,
    title_id,
    device_id,
    cast(session_started_at as timestamp(6)) as session_started_at,
    cast(session_ended_at as timestamp(6)) as session_ended_at,
    watch_duration_seconds,
    cast(playback_percent as decimal(5, 4)) as playback_percent,
    cast(buffering_count as bigint) as buffering_count,
    cast(avg_bitrate_kbps as bigint) as avg_bitrate_kbps,
    playback_quality,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_playback_sessions') }}
