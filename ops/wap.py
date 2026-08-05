"""Write-audit-publish: build and test a dbt scope on an isolated Nessie branch, merge
to main only if it passes, otherwise leave main untouched and exit nonzero.

Usage:
    uv run python -m ops.wap --select wap_demo_dim
    uv run python -m ops.wap --select wap_demo_dim --vars '{"wap_demo_inject_bad_row": true}'
    uv run python -m ops.wap --select stg_billing_ledger+   # any dbt scope works the same way

Stages, in order (write / audit / publish):
    0. give main one real commit if it has none yet (a brand-new repo, a fresh CI run
       above all): works around a confirmed Nessie 0.108.4 defect where merging into a
       target whose history traces back to the server's genesis commit fails outright.
       See .notes/surprises.md. A no-op once main has any real history, which is every
       run after the first ever.
    1. create a Nessie branch off main, named <branch-prefix>_<run-id>.
    2. register a Trino catalog scoped to that branch (see .notes/decisions.md for why
       this, not a session property, is how Trino's Iceberg REST connector targets a
       non-default Nessie ref).
    3. `dbt run --select <scope>` against that catalog (write).
    4. `dbt test --select <scope>` against what was just built (audit, the quality gate).
    5. if both passed: merge the branch into main via the Nessie REST API (publish),
       then delete the branch.
    6. if anything failed: main is never touched. The branch is left in place for
       inspection by default (--no-keep-failed-branch deletes it instead). The Trino
       catalog registration is always dropped, success or failure: it holds no data of
       its own, only a name-to-branch binding, so there is nothing to preserve.

Exit code 0 on a full clean run. Nonzero otherwise; the stage that failed is named in
both the log output and the exit code (see _STAGE_EXIT_CODES below).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import structlog
import trino.dbapi
from pydantic import BaseModel, ConfigDict

from ops import nessie

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "transform" / "lakehouse"
ENV_FILE = REPO_ROOT / ".env"

# Nessie's default warehouse name, fixed in infra/docker-compose.yml
# (nessie.catalog.default-warehouse: warehouse). Same constant as
# ingestion/settings.py's NESSIE_WAREHOUSE_NAME, kept local rather than imported so
# ops/ has no dependency on ingestion/'s internals.
NESSIE_WAREHOUSE_NAME = "warehouse"

# Hostnames as Trino itself resolves them from inside the docker compose network, not
# as this script (running on the host, or a CI runner) resolves them. These match
# infra/trino/etc/catalog/iceberg.properties' static "iceberg" catalog exactly: the
# dynamic per-branch catalog this script creates has to reach the same two services.
TRINO_INTERNAL_NESSIE_URI = "http://nessie:19120"
TRINO_INTERNAL_S3_ENDPOINT = "http://minio:9000"

# elementary-data/elementary's own dbt_project.yml (dbt_packages/elementary) registers
# project-wide on-run-start/on-run-end hooks that fire on every dbt invocation
# regardless of --select, and that write project-wide bookkeeping tables
# (dbt_invocations, dbt_run_results, elementary_test_results) into the plain "dev"
# schema, the same schema name on every branch. Confirmed live: without these vars, a
# WAP run's dbt calls modify those same table keys the current main branch is also
# evolving (from ordinary, unrelated dbt activity against the trino target), and the
# merge back to main then fails with a genuine Nessie REFERENCE_CONFLICT on those keys,
# nothing to do with the actual --select payload. Elementary's own config vars
# (macros/edr/system/hooks/on_run_end.sql) gate every one of those writes, so passing
# them true removes the conflict at the source instead of trying to special-case the
# merge around it. WAP runs are a narrow, isolated operation whose own pass/fail is
# already fully captured by this script's exit code and log output, so nothing
# elementary would have tracked is lost.
_ELEMENTARY_DISABLE_VARS: dict[str, object] = {
    "disable_dbt_artifacts_autoupload": True,
    "disable_run_results": True,
    "disable_tests_results": True,
    "disable_dbt_invocation_autoupload": True,
    "disable_freshness_results": True,
}

# Maps the WapStageError stage name to a distinct process exit code, so CI (or a human)
# can tell what failed without parsing log text.
_STAGE_EXIT_CODES = {
    "branch-create": 2,
    "catalog-create": 3,
    "dbt-run": 4,
    "dbt-test": 5,
    "merge": 6,
    "dbt-deps": 7,
    "bootstrap": 8,
}


class WapStageError(RuntimeError):
    """Raised at any WAP stage that must abort the run without touching main."""

    def __init__(self, stage: str, detail: str) -> None:
        self.stage = stage
        self.detail = detail
        super().__init__(f"[{stage}] {detail}")


class WapConfig(BaseModel):
    """Resolved connection details, loaded from the environment or repo .env, same
    pattern as ingestion/settings.py's WarehouseConfig."""

    model_config = ConfigDict(frozen=True)

    nessie_uri: str
    trino_host: str
    trino_port: int
    minio_user: str
    minio_password: str


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def load_config(
    *,
    nessie_uri: str | None = None,
    trino_host: str | None = None,
    trino_port: int | None = None,
) -> WapConfig:
    """Real environment variables win over .env, matching how docker compose itself
    resolves --env-file values (a shell export overrides the file)."""
    env = {**_load_env_file(ENV_FILE), **os.environ}
    required = ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
    missing = [key for key in required if key not in env]
    if missing:
        raise RuntimeError(
            f"Missing warehouse credentials: {', '.join(missing)}. Copy .env.example to "
            ".env and fill it in before running ops/wap.py (the same file `make up` uses)."
        )
    return WapConfig(
        nessie_uri=nessie_uri or f"http://127.0.0.1:{env.get('NESSIE_PORT', '19120')}",
        trino_host=trino_host or env.get("DBT_TRINO_HOST") or "127.0.0.1",
        trino_port=trino_port or int(env.get("DBT_TRINO_PORT") or env.get("TRINO_PORT", "8080")),
        minio_user=env["MINIO_ROOT_USER"],
        minio_password=env["MINIO_ROOT_PASSWORD"],
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _trino_execute(config: WapConfig, sql: str) -> None:
    # trino ships py.typed, but dbapi.connect() itself is defined as
    # `def connect(*args, **kwargs)` with no annotations, a real upstream gap.
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.trino_host,
        port=config.trino_port,
        user="wap",
        catalog="system",
        schema="runtime",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.fetchall()
    finally:
        connection.close()


def _create_catalog(config: WapConfig, catalog_name: str, branch_name: str) -> None:
    """Register a Trino catalog whose Iceberg REST catalog URI targets branch_name
    instead of main. Confirmed live (.notes/decisions.md) that Nessie's REST catalog
    reads the branch from the URI path (.../iceberg/<branch>), and that Trino only
    exposes rest-catalog.uri at catalog-registration time, not as a session property,
    so a fresh catalog per run is the actual mechanism, not a shortcut."""
    branch_uri = f"{TRINO_INTERNAL_NESSIE_URI}/iceberg/{branch_name}"
    sql = f"""
        CREATE CATALOG {catalog_name} USING iceberg WITH (
          "iceberg.catalog.type" = {_sql_literal("rest")},
          "iceberg.rest-catalog.uri" = {_sql_literal(branch_uri)},
          "iceberg.rest-catalog.warehouse" = {_sql_literal(NESSIE_WAREHOUSE_NAME)},
          "iceberg.rest-catalog.nested-namespace-enabled" = {_sql_literal("true")},
          "iceberg.file-format" = {_sql_literal("PARQUET")},
          "fs.native-s3.enabled" = {_sql_literal("true")},
          "s3.endpoint" = {_sql_literal(TRINO_INTERNAL_S3_ENDPOINT)},
          "s3.region" = {_sql_literal("us-east-1")},
          "s3.path-style-access" = {_sql_literal("true")},
          "s3.aws-access-key" = {_sql_literal(config.minio_user)},
          "s3.aws-secret-key" = {_sql_literal(config.minio_password)}
        )
    """
    try:
        _trino_execute(config, sql)
    except Exception as exc:
        raise WapStageError("catalog-create", str(exc)) from exc


def _drop_catalog(config: WapConfig, catalog_name: str) -> None:
    _trino_execute(config, f"DROP CATALOG IF EXISTS {catalog_name}")


def _run_dbt(
    subcommand: str,
    config: WapConfig,
    catalog_name: str,
    target: str,
    select: str,
    vars_json: str,
) -> None:
    command = [
        "uv",
        "run",
        "dbt",
        subcommand,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--target",
        target,
        "--select",
        select,
        "--vars",
        vars_json,
    ]
    env = {
        **os.environ,
        "DBT_TRINO_DATABASE": catalog_name,
        "DBT_TRINO_HOST": config.trino_host,
        "DBT_TRINO_PORT": str(config.trino_port),
    }
    logger.info("running dbt", subcommand=subcommand, catalog=catalog_name, select=select)
    result = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False)
    if result.returncode != 0:
        raise WapStageError(f"dbt-{subcommand}", f"dbt {subcommand} exited {result.returncode}")


def _ensure_dbt_deps() -> None:
    """Install dbt_utils/dbt_expectations/elementary if this checkout doesn't have them
    yet. dbt_packages/ is gitignored (installed, not source, per this project's
    .gitignore), so a fresh checkout, a fresh CI runner above all, starts without it.
    Confirmed live: without this, the very first dbt invocation of a WAP run fails
    with "dbt found 3 package(s) specified in packages.yml, but only 0 package(s)
    installed" before ever reaching the quality gate. Runs once per WAP invocation at
    most (skipped if dbt_packages/ already exists), so repeat local runs pay nothing
    extra."""
    if (DBT_PROJECT_DIR / "dbt_packages").is_dir():
        return
    logger.info("dbt_packages missing, running dbt deps")
    command = [
        "uv",
        "run",
        "dbt",
        "deps",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
    ]
    result = subprocess.run(command, cwd=REPO_ROOT, env=os.environ.copy(), check=False)
    if result.returncode != 0:
        raise WapStageError("dbt-deps", f"dbt deps exited {result.returncode}")


def run_wap(
    *,
    config: WapConfig,
    select: str,
    vars_dict: dict[str, object],
    target: str = "trino",
    branch_prefix: str = "wap",
    keep_failed_branch: bool = True,
) -> int:
    run_started = datetime.now(UTC)
    run_id = f"{run_started:%Y%m%dt%H%M%Sz}_{uuid.uuid4().hex[:8]}"
    branch_name = f"{branch_prefix}_{run_id}"
    catalog_name = f"iceberg_{branch_name}"
    # caller-supplied vars win on key collision, though none of these names are
    # meaningful outside this elementary-silencing purpose.
    vars_json = json.dumps({**_ELEMENTARY_DISABLE_VARS, **vars_dict})

    log = logger.bind(run_id=run_id, branch=branch_name, catalog=catalog_name, select=select)
    log.info("wap run starting")

    branch_created = False
    catalog_created = False

    try:
        _ensure_dbt_deps()

        try:
            nessie.bootstrap_main_if_empty(config.nessie_uri, NESSIE_WAREHOUSE_NAME)
        except nessie.NessieError as exc:
            raise WapStageError("bootstrap", str(exc)) from exc

        main_ref = nessie.get_reference(config.nessie_uri, "main")
        log.info("read main HEAD", main_hash=main_ref.hash)

        try:
            branch_ref = nessie.create_branch(config.nessie_uri, branch_name, main_ref)
        except nessie.NessieError as exc:
            raise WapStageError("branch-create", str(exc)) from exc
        branch_created = True
        log.info("branch created", branch_hash=branch_ref.hash)

        _create_catalog(config, catalog_name, branch_name)
        catalog_created = True
        log.info("trino catalog registered for branch")

        _run_dbt("run", config, catalog_name, target, select, vars_json)
        log.info("dbt run passed, write stage complete")

        _run_dbt("test", config, catalog_name, target, select, vars_json)
        log.info("dbt test passed, audit stage complete (quality gate cleared)")

        # re-read both refs immediately before merging: dbt's writes advanced the
        # branch past branch_ref.hash above, and main may have moved since the top
        # of this run if another WAP run merged concurrently.
        built_branch_ref = nessie.get_reference(config.nessie_uri, branch_name)
        current_main_ref = nessie.get_reference(config.nessie_uri, "main")
        try:
            merge_result = nessie.merge_branch(
                config.nessie_uri,
                "main",
                current_main_ref.hash,
                branch_name,
                built_branch_ref.hash,
            )
        except nessie.NessieError as exc:
            raise WapStageError("merge", str(exc)) from exc
        if not (merge_result.was_applied and merge_result.was_successful):
            raise WapStageError(
                "merge",
                f"merge reported wasApplied={merge_result.was_applied} "
                f"wasSuccessful={merge_result.was_successful}, main left untouched",
            )
        log.info(
            "merged to main, publish stage complete",
            resultant_main_hash=merge_result.resultant_target_hash,
        )

        nessie.delete_reference(config.nessie_uri, branch_name, built_branch_ref.hash)
        branch_created = False
        log.info("wap run succeeded, branch deleted")
        return 0

    except WapStageError as failure:
        log.error("wap run failed, main untouched", stage=failure.stage, detail=failure.detail)
        if branch_created:
            if keep_failed_branch:
                log.warning("leaving failed branch in place for inspection", branch=branch_name)
            else:
                try:
                    latest = nessie.get_reference(config.nessie_uri, branch_name)
                    nessie.delete_reference(config.nessie_uri, branch_name, latest.hash)
                    log.info("deleted failed branch", branch=branch_name)
                except nessie.NessieError as cleanup_error:
                    log.error("failed to delete failed branch", error=str(cleanup_error))
        return _STAGE_EXIT_CODES.get(failure.stage, 1)

    finally:
        if catalog_created:
            try:
                _drop_catalog(config, catalog_name)
                log.info("trino catalog deregistered", catalog=catalog_name)
            except Exception as cleanup_error:
                log.error(
                    "failed to drop trino catalog, manual cleanup needed",
                    catalog=catalog_name,
                    error=str(cleanup_error),
                )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="write-audit-publish a dbt scope through an isolated Nessie branch"
    )
    parser.add_argument(
        "--select", required=True, help="dbt --select scope to build and test, e.g. wap_demo_dim"
    )
    parser.add_argument(
        "--vars",
        default="{}",
        help="JSON object passed through to dbt --vars, e.g. '{\"wap_demo_inject_bad_row\": true}'",
    )
    parser.add_argument(
        "--target",
        default="trino",
        help="dbt target (default: trino, the only one with real branches)",
    )
    parser.add_argument("--branch-prefix", default="wap")
    parser.add_argument(
        "--nessie-uri",
        default=None,
        help="Nessie REST API base URI reachable from where this script runs "
        "(default: derived from .env's NESSIE_PORT)",
    )
    parser.add_argument("--trino-host", default=None)
    parser.add_argument("--trino-port", type=int, default=None)
    parser.add_argument(
        "--keep-failed-branch",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="on failure, leave the branch in Nessie for inspection instead of "
        "deleting it (default: keep)",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    try:
        vars_dict = json.loads(args.vars)
    except json.JSONDecodeError as exc:
        logger.error("invalid --vars json", error=str(exc))
        return 1
    if not isinstance(vars_dict, dict):
        logger.error("--vars must be a JSON object", got=type(vars_dict).__name__)
        return 1

    config = load_config(
        nessie_uri=args.nessie_uri, trino_host=args.trino_host, trino_port=args.trino_port
    )
    return run_wap(
        config=config,
        select=args.select,
        vars_dict=vars_dict,
        target=args.target,
        branch_prefix=args.branch_prefix,
        keep_failed_branch=args.keep_failed_branch,
    )


if __name__ == "__main__":
    sys.exit(main())
