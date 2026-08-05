"""Tests for ingestion/hashing.py: the bronze `_payload_hash` SQL expression.

payload_hash_expression does not hash anything itself; it builds a DuckDB
SQL string (coalesce-to-VARCHAR, NULL -> '_null_', joined with '||', wrapped
in md5(...)) that DuckDB evaluates at load time. "Determinism" and
"sensitivity" for this function mean: the same column list always produces
the same SQL text, and a different column list (including just a different
order, which changes the resulting hash value once DuckDB runs it) produces
different SQL text.
"""

from __future__ import annotations

from ingestion.hashing import payload_hash_expression


def test_single_column_expression() -> None:
    expr = payload_hash_expression(["title_name"])
    assert expr == "md5(coalesce(\"title_name\"::VARCHAR, '_null_'))"


def test_multiple_columns_joined_in_given_order() -> None:
    expr = payload_hash_expression(["a", "b", "c"])
    assert expr == (
        "md5(coalesce(\"a\"::VARCHAR, '_null_') || '||' || "
        "coalesce(\"b\"::VARCHAR, '_null_') || '||' || "
        "coalesce(\"c\"::VARCHAR, '_null_'))"
    )


def test_same_columns_produce_identical_expression_every_call() -> None:
    columns = ["subscriber_id", "email", "status"]
    assert payload_hash_expression(columns) == payload_hash_expression(list(columns))


def test_column_order_changes_the_expression() -> None:
    """Column order is part of the hash input (per the module's fixed
    canonicalization convention), so reordering must change the generated
    SQL, not just cosmetically but in a way that changes the resulting
    md5 value once DuckDB evaluates it."""
    forward = payload_hash_expression(["a", "b"])
    reversed_ = payload_hash_expression(["b", "a"])
    assert forward != reversed_


def test_different_column_sets_produce_different_expressions() -> None:
    assert payload_hash_expression(["a"]) != payload_hash_expression(["a", "b"])


def test_every_column_is_individually_null_coalesced() -> None:
    """Each column gets its own coalesce(...::VARCHAR, '_null_'), not one
    coalesce wrapped around the whole joined string, so a NULL in any single
    column is distinguishable from an empty string elsewhere in the row."""
    expr = payload_hash_expression(["x", "y"])
    assert expr.count("coalesce(") == 2
    assert expr.count("'_null_'") == 2


def test_empty_column_list_still_produces_a_valid_wrapper() -> None:
    """Not a realistic call (bronze tables always hash at least one column),
    but documents the actual behavior rather than leaving it unspecified."""
    assert payload_hash_expression([]) == "md5()"
