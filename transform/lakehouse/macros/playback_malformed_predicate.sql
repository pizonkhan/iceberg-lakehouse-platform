-- shared malformed-row predicate for the playback quality gate, kept as a
-- macro rather than an upstream computed column deliberately: routing the
-- filter through a derived rejection_reason column defeats Iceberg
-- predicate pushdown on this ~120M-row table (Trino cannot push a CASE
-- expression down to the connector the way it pushes direct column
-- comparisons), which was enough to blow the single-node Trino's 1.5GB
-- per-query memory cap on a plain filter with no aggregation. Inlining
-- this text directly into each model's WHERE clause keeps the three
-- comparisons as pushdown-eligible predicates while still defining the
-- rule once. See decisions.md.
{% macro playback_malformed_predicate() %}
    (watch_duration_seconds < 0 or session_ended_at < session_started_at or session_started_at > _ingested_at)
{% endmacro %}
