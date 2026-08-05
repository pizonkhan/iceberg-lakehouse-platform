"""Operational scripts for the lakehouse, run outside the dbt DAG itself.

ops/wap.py is the write-audit-publish mechanism: create a Nessie branch, build and
test a dbt scope against it, and only merge to main if the quality gate passes.
"""
