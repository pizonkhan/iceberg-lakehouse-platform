"""Thin REST client for the Nessie branch operations ops/wap.py needs.

Nessie's REST API (v2, `/api/v2/trees/...`) is the only interface used here. A raw
client is deliberately simpler than adding pynessie as a dependency for this scope:
four calls (read a ref, create a branch, merge a branch, delete a ref), each a plain
JSON POST/GET/DELETE with no auth in this local/CI setup. See .notes/decisions.md for
the tradeoff and for how each endpoint's exact request shape (query params vs body
fields, the `{ref}@{hash}` checked-reference path convention) was confirmed against
the live stack, since Nessie's own OpenAPI document is not reliably reachable over
HTTP on this server and the shapes are not obvious from the outer API guide.

Every call here is synchronous and unbuffered on purpose: ops/wap.py runs one branch
per invocation, not a batch, so there is no benefit to an async or pooled client.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# A namespace this project has never used for anything else, only ever created (once,
# idempotently) by bootstrap_main_if_empty below. See that function's docstring for
# why it needs to exist at all.
BOOTSTRAP_NAMESPACE = "_wap_bootstrap"


class NessieError(RuntimeError):
    """A Nessie REST call failed. Message includes the status code and response body."""


class Reference(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: str
    name: str
    hash: str


class MergeResult(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    was_applied: bool = Field(alias="wasApplied")
    was_successful: bool = Field(alias="wasSuccessful")
    resultant_target_hash: str | None = Field(default=None, alias="resultantTargetHash")


def _call(method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise NessieError(f"{method} {url} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise NessieError(f"{method} {url} -> unreachable: {exc.reason}") from exc
    return json.loads(raw) if raw else {}


def get_reference(nessie_uri: str, ref_name: str) -> Reference:
    """Read a branch or tag's current hash. Used to find main's HEAD before branching
    and, again, right before a merge (main may have moved since the branch was cut)."""
    payload = _call("GET", f"{nessie_uri}/api/v2/trees/{ref_name}")
    return Reference.model_validate(payload["reference"])


def create_branch(nessie_uri: str, branch_name: str, source: Reference) -> Reference:
    """Create branch_name pointing at source's current hash.

    The `name`/`type` query params name the *new* ref; the JSON body describes the
    *source* ref the new branch is cut from. This asymmetry is real, not a client
    quirk, confirmed directly against the running server: a body of {"type":
    "BRANCH", "name": <new-name>, ...} is rejected ("missing type id property
    'type'"), the working shape is {"type": "BRANCH", "name": <source-name>, "hash":
    <source-hash>} in the body with the new name and type as query params.
    """
    payload = _call(
        "POST",
        f"{nessie_uri}/api/v2/trees?name={branch_name}&type=BRANCH",
        body={"type": source.type, "name": source.name, "hash": source.hash},
    )
    return Reference.model_validate(payload["reference"])


def merge_branch(
    nessie_uri: str,
    target_branch: str,
    target_expected_hash: str,
    from_ref_name: str,
    from_hash: str,
) -> MergeResult:
    """Merge from_ref_name into target_branch (in this project: always main).

    target_expected_hash guards against a concurrent write to the target moving it
    since it was last read (optimistic concurrency): it goes in the path as
    `{branch}@{hash}`, not the request body. A body-only "expectedHash" field was
    tried against the live server and rejected ("Expected hash must be provided."),
    the checked-reference-in-path form is what the server actually accepts.
    """
    payload = _call(
        "POST",
        f"{nessie_uri}/api/v2/trees/{target_branch}@{target_expected_hash}/history/merge",
        body={"fromRefName": from_ref_name, "fromHash": from_hash},
    )
    return MergeResult.model_validate(payload)


def delete_reference(nessie_uri: str, ref_name: str, ref_hash: str) -> None:
    """Delete a branch. Requires the branch's current hash in the path (same checked-
    reference convention as merge) so a stale delete request can't drop commits a
    concurrent writer just added."""
    _call("DELETE", f"{nessie_uri}/api/v2/trees/{ref_name}@{ref_hash}?type=BRANCH")


def bootstrap_main_if_empty(nessie_uri: str, warehouse: str) -> None:
    """Give main one real commit if it has none yet, working around a confirmed
    Nessie 0.108.4 defect (.notes/surprises.md): merging a branch into a target
    fails with 404 REFERENCE_NOT_FOUND ("No common ancestor in parents of ...") if
    the two refs' common ancestor is the server's genesis/empty-repository commit,
    even for a branch cut directly from that exact hash with real commits on top.
    Reproduced directly against a brand-new, otherwise untouched Nessie instance
    (bypassing Trino and dbt entirely) with a plain namespace-create as the branch's
    only content, so this is not specific to Iceberg tables, dbt, or this project's
    own client code. A brand-new repository (a fresh CI run, always) starts in
    exactly the affected state, every time, so ops/wap.py calls this once at the top
    of every run, before creating a branch: idempotent (a namespace create against an
    already-bootstrapped main returns 409, treated here as success, not an error) and
    cheap once main already has real history (the overwhelming majority of calls).
    """
    prefix = urllib.parse.quote(f"main|{warehouse}", safe="")
    url = f"{nessie_uri}/iceberg/v1/{prefix}/namespaces"
    data = json.dumps({"namespace": [BOOTSTRAP_NAMESPACE]}).encode()
    request = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return  # already bootstrapped, nothing to do
        detail = exc.read().decode(errors="replace")
        raise NessieError(f"POST {url} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise NessieError(f"POST {url} -> unreachable: {exc.reason}") from exc
