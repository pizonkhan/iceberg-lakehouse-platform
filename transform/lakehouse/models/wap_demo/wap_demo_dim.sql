{#-
  Purpose-built demonstration table for the write-audit-publish mechanism
  (ops/wap.py). Not part of the dimensional model: lives in its own
  iceberg.dev_wap_demo schema (see dbt_project.yml), referenced by nothing
  else in this project. Exists only so the WAP script has a small, fast
  scope to build and test against a Nessie branch, on both a clean run and
  a deliberately bad load. See docs/evidence/write-audit-publish/ and
  .notes/decisions.md.

  wap_demo_inject_bad_row is set by the bad-load demonstration
  (--vars '{"wap_demo_inject_bad_row": true}'): it unions in one extra row
  with a null demo_code, which trips that column's not_null test
  (_wap_demo_dim.yml) without touching demo_id's uniqueness, so the failure
  evidence isolates a single, unambiguous quality-gate violation.
#}

{% set inject_bad_row = var("wap_demo_inject_bad_row", false) %}

with base as (

    select *
    from (
        values
            (1, 'us-east', 'US East'),
            (2, 'us-west', 'US West'),
            (3, 'eu-central', 'EU Central'),
            (4, 'eu-west', 'EU West'),
            (5, 'ap-southeast', 'AP Southeast'),
            (6, 'ap-northeast', 'AP Northeast'),
            (7, 'sa-east', 'SA East'),
            (8, 'af-south', 'AF South')
    ) as t (demo_id, demo_code, demo_label)

)

select
    demo_id,
    demo_code,
    demo_label,
    current_timestamp as loaded_at
from base

{% if inject_bad_row %}
union all
select
    99 as demo_id,
    cast(null as varchar) as demo_code,
    'bad row: null demo_code, injected by the WAP bad-load demonstration' as demo_label,
    current_timestamp as loaded_at
{% endif %}
