Write-audit-publish demonstration evidence. ops/wap.py, models/wap_demo/wap_demo_dim.sql.
Every file here is raw output captured directly from the live Trino/Iceberg/Nessie/MinIO
stack (Trino 483, dbt-trino 1.10.3, Nessie 0.108.4), not narrated or hand-edited. Captured
against the final version of ops/wap.py, after the elementary-hook, dbt-deps, and
genesis-merge fixes documented in .notes/decisions.md and .notes/surprises.md; main already
had real history by this point (bronze/dev_dimensions/dev_facts/dev_silver rebuilt from
generation/output/ after an unrelated incident, see .notes/failures.md), so
bootstrap_main_if_empty() ran as a harmless no-op-after-first-call rather than an
observably active step in this specific capture; its own behavior on a genuinely empty
repository is what .notes/surprises.md's repro documents directly.

00-before-refs.txt
    Nessie refs and the main-scoped catalog's schema list before any WAP run: one
    branch (main), no dev_wap_demo schema.

01-clean-run-log.txt
    Full stdout/stderr of `uv run python -m ops.wap --select wap_demo_dim`, a clean
    run with no data quality violation. Ends with EXIT_CODE=0. Shows the branch
    created, the Trino catalog registered against it, dbt run and dbt test both
    passing, the merge to main, and the branch deleted.

02-after-refs.txt
    Nessie refs and schema list immediately after the clean run: back to exactly one
    branch (main, at a new hash), dev_wap_demo now present.

03-clean-run-main-query.txt
    All 8 rows queried through the ordinary main-scoped `iceberg` catalog (the same
    catalog every other query in this project uses), plus $history proving this
    landed as a real Iceberg snapshot commit on main, not a side effect of the run.

04-before-bad-run-refs.txt
    Refs and row count on main immediately before the bad-load run: one branch, 8 rows.

05-bad-run-log.txt
    Full stdout/stderr of `uv run python -m ops.wap --select wap_demo_dim --vars
    '{"wap_demo_inject_bad_row": true}'`. dbt run succeeds (materializes 9 rows,
    including one with a null demo_code: dbt run has no data-quality awareness of its
    own). dbt test then fails on not_null_wap_demo_dim_demo_code. The script never
    attempts the merge, leaves the branch in place for inspection (documented default,
    see .notes/decisions.md), drops the Trino catalog registration regardless, and
    ends with EXIT_CODE=5 (the dbt-test stage code).

06-after-bad-run-main-query.txt
    Refs, row count, full contents, and an explicit null/id=99 check against main
    after the bad-load run: main's hash is byte-for-byte identical to
    04-before-bad-run-refs.txt, still exactly 8 rows, zero rows matching the injected
    violation.

07-failed-branch-inspection.txt
    A one-off Trino catalog registered directly against the leftover failed branch
    (the same mechanism ops/wap.py itself uses), querying its copy of wap_demo_dim
    directly: all 9 rows, including demo_id=99 with a null demo_code. This is what
    "left for inspection" actually means: the bad write is fully present and
    queryable on its own branch, just never reachable from main. The branch and the
    one-off catalog were both deleted immediately after this capture; the stack was
    confirmed to be back to exactly one branch (main) and the two static catalogs
    (iceberg, system) with no leftover iceberg_wap_* registrations.

Exit code map (ops/wap.py's _STAGE_EXIT_CODES): 2 branch-create, 3 catalog-create,
4 dbt-run, 5 dbt-test, 6 merge, 7 dbt-deps, 8 bootstrap, 1 anything else, 0 success.
