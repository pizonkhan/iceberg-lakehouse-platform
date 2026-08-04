"""Resolves docker-compose service hostnames to the host loopback address.

Nessie's REST catalog is configured in infra/docker-compose.yml with an S3 endpoint of
http://minio:9000, the hostname MinIO has on the compose network. When a table is
created or a snapshot is committed, Nessie hands that endpoint back to the client as
part of the table's storage config, and pyiceberg uses it in preference to whatever
`s3.endpoint` the client supplied itself. That is correct for Trino, which runs inside
the same compose network and can resolve "minio" directly.

This ingestion pipeline runs on the host through the uv-managed venv (the same pattern
`make build` already uses for dbt), where "minio" and "nessie" do not resolve. Rather
than edit /etc/hosts or add a new compose service for a Python client, this module
redirects those two hostnames to 127.0.0.1 for the lifetime of the process, landing on
the same ports docker-compose already publishes to the host. See .notes/decisions.md.
"""

from __future__ import annotations

import socket
from typing import Any

_DOCKER_SERVICE_ALIASES = {"minio": "127.0.0.1", "nessie": "127.0.0.1"}
_installed = False


def install_docker_host_aliases() -> None:
    """Patch socket.getaddrinfo so compose service names resolve from the host.

    Idempotent: safe to call more than once per process.
    """
    global _installed
    if _installed:
        return

    real_getaddrinfo = socket.getaddrinfo

    def _patched_getaddrinfo(host: bytes | str | None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(host, str) and host in _DOCKER_SERVICE_ALIASES:
            host = _DOCKER_SERVICE_ALIASES[host]
        return real_getaddrinfo(host, *args, **kwargs)

    socket.getaddrinfo = _patched_getaddrinfo
    _installed = True
