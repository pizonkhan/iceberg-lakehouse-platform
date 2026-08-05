Nessie-native time travel and rollback demonstration evidence. ops/time_travel_demo.py,
ops/nessie.py (get_log, assign_reference). Closes the gap recorded in
.notes/open-questions.md (2026-08-05 red team pass, part 3): Iceberg-native time travel
(SELECT ... FOR VERSION AS OF, $snapshots, $history) does not work through Trino on this
catalog because Nessie's Iceberg REST bridge deliberately exposes only the single
snapshot matching the current Nessie commit, by design, not a bug or a missing config
(see 10-root-cause-confirmation.txt). The real mechanism is Nessie's own commit graph,
reached by pointing a dynamically registered Trino catalog at a specific Nessie ref.

Every JSON/text file below except 10-root-cause-confirmation.txt (a documentation
citation, marked as such) is raw output captured directly from the live stack (Trino
483, dbt-trino 1.10.3, Nessie 0.108.4) by ops/time_travel_demo.py or by ad hoc curl/
python calls against the running Nessie REST API, not narrated or hand-edited. Captured
2026-08-05. The whole demonstration runs on a dedicated Nessie branch,
time_travel_demo, cut from main and never merged back or reset on main itself: see
01-commit-hashes.txt and .notes/decisions.md for why (Nessie branch resets are
branch-wide, not scoped to one table, so resetting main to undo one bad demo commit
would also revert every real table cataloged on main back to that same point).

01-commit-hashes.txt
    The three hashes this whole demonstration turns on: main's HEAD when the demo
    branch was cut, hash_good (after the 5-row good batch), hash_bad (after the 3-row
    bad batch, 2 of the 3 rows have a negative amount_usd, landing the table at 8
    rows total).

02-asof-good-query.json
    Point-in-time query: a Trino catalog registered with iceberg.rest-catalog.uri
    pointed at time_travel_demo@<hash_good> (Nessie's checked-reference syntax, a
    branch name and a specific commit hash joined by @). Returns exactly the 5 good
    rows, none of the 3 bad-batch rows, proving Nessie-native point-in-time query
    works even though Iceberg-native FOR VERSION AS OF does not on this catalog.

03-live-query-before-rollback.json
    Same table queried through the ordinary branch-scoped catalog (no hash in the
    URI, current HEAD) before any rollback: 8 rows, including the 2 negative
    amounts. Confirms the bad batch is genuinely live on the branch at this point,
    not just present in Nessie's history.

04-nessie-history-before-rollback.json
    GET .../trees/time_travel_demo/history?fetch=ALL, filtered to commits touching
    demo_billing_batch: 3 entries (create, good insert, bad insert), each carrying
    its own metadataLocation and snapshotId. Note each of the three commits points
    at a distinct 00000-<uuid>.metadata.json, none referencing a previous-metadata
    pointer: the same no-parent-chain pattern the original red team finding
    documented on fct_billing_transactions, reproduced here from Nessie's own commit
    operations rather than by listing MinIO directly, and on a plain Trino INSERT,
    not a dbt MERGE, ruling out the dbt-trino write path as the cause (see
    10-root-cause-confirmation.txt).

05-rollback-assign-response.json
    The real rollback call: PUT .../trees/time_travel_demo@<hash_bad> with body
    {"type": "BRANCH", "name": "time_travel_demo", "hash": <hash_good>}, Nessie's
    branch reset/assign mechanism (ops/nessie.py's assign_reference). Response
    confirms the branch's new HEAD is hash_good.

06-live-query-after-rollback.json
    The same live, no-hash-in-the-URI catalog queried again immediately after the
    reset: 5 rows, no negative amounts. This is what proves the rollback is real,
    not a historical read through a different catalog: the same catalog that showed
    8 rows in 03 now shows 5, because the branch's HEAD itself moved.

07-nessie-history-after-rollback.json
    The commit log again, same filter, after the reset: 2 entries (create, good
    insert). hash_bad is gone from the branch's history because history walks
    parent pointers from the current HEAD backward and hash_bad is no longer an
    ancestor of anything reachable from HEAD, not because the commit was deleted
    (it still exists in Nessie's own storage, unreachable, until a GC pass reclaims
    it; see 09-retention-policy-notes.txt).

08-summary.txt
    Pass/fail summary ops/time_travel_demo.py writes at the end of its own run:
    row counts and the reachability check, all PASS on the run this evidence was
    captured from.

09-retention-policy-notes.txt
    Nessie's actual commit and storage retention story (not automatic; requires
    the separate nessie-gc tool, run manually or on a schedule), the retention
    policy chosen for this project's laptop-local, zero-budget-for-unbounded-growth
    constraint, and why.

10-root-cause-confirmation.txt
    Direct citation from Nessie's own iceberg-rest.md guide confirming the single-
    snapshot-per-commit behavior is a deliberate design choice for cross-table
    catalog consistency, not a missing config flag or a dbt/Trino write-path quirk.
    Resolves the three open hypotheses the original open-questions.md entry left
    unconfirmed.
