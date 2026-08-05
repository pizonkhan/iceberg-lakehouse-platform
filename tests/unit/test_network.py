"""Tests for ingestion/network.py: the docker-compose hostname shim.

install_docker_host_aliases() patches socket.getaddrinfo at the process
level so "minio" and "nessie" (compose service names that only resolve
inside the compose network) resolve to 127.0.0.1 when this pipeline runs on
the host. These tests patch socket.getaddrinfo out entirely so no real DNS
resolution happens, and reset the module's _installed guard around each
test so tests do not leak patched state into each other.

Reads of socket.getaddrinfo below go through _current_getaddrinfo() rather
than the bare attribute: mypy resolves `socket.getaddrinfo` to typeshed's
real signature regardless of what install_docker_host_aliases() replaces it
with at runtime, so comparing its (real, unrelated) return type against our
fake's sentinel values is a static type error even though it is exactly
what these tests need to check at runtime.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

import pytest

import ingestion.network as network


@pytest.fixture(autouse=True)
def _reset_installed_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test gets a fresh, unpatched socket.getaddrinfo and a reset
    _installed flag, so install_docker_host_aliases() actually re-installs
    instead of silently no-op'ing because an earlier test already called it."""
    monkeypatch.setattr(network, "_installed", False)


def _current_getaddrinfo() -> Any:
    return getattr(socket, "getaddrinfo")  # noqa: B009


def _recording_getaddrinfo() -> tuple[Callable[..., str], list[Any]]:
    calls: list[Any] = []

    def fake(host: Any, *args: Any, **kwargs: Any) -> str:
        calls.append(host)
        return "sentinel-result"

    return fake, calls


def test_known_compose_hostnames_are_rewritten_to_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake, calls = _recording_getaddrinfo()
    monkeypatch.setattr(socket, "getaddrinfo", fake)

    network.install_docker_host_aliases()

    result = _current_getaddrinfo()("minio", 9000)
    assert result == "sentinel-result"
    assert calls[-1] == "127.0.0.1"

    result = _current_getaddrinfo()("nessie", 19120)
    assert result == "sentinel-result"
    assert calls[-1] == "127.0.0.1"


def test_unrelated_hostnames_pass_through_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    fake, calls = _recording_getaddrinfo()
    monkeypatch.setattr(socket, "getaddrinfo", fake)

    network.install_docker_host_aliases()

    _current_getaddrinfo()("example.com", 443)
    assert calls[-1] == "example.com"

    _current_getaddrinfo()("localhost", 8080)
    assert calls[-1] == "localhost"


def test_non_string_host_is_passed_through_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    """getaddrinfo's host argument can be None or bytes; only a str match
    against the alias table should ever trigger a rewrite."""
    fake, calls = _recording_getaddrinfo()
    monkeypatch.setattr(socket, "getaddrinfo", fake)

    network.install_docker_host_aliases()

    _current_getaddrinfo()(None, 80)
    assert calls[-1] is None

    _current_getaddrinfo()(b"minio", 80)
    assert calls[-1] == b"minio"  # bytes never matches the str-keyed alias table


def test_install_is_idempotent_and_does_not_rewrap_on_second_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second call must be a no-op: it should not wrap the already-patched
    function a second time. Verified by identity: socket.getaddrinfo must be
    the exact same function object after the second call as after the first."""
    marker, _ = _recording_getaddrinfo()
    monkeypatch.setattr(socket, "getaddrinfo", marker)

    network.install_docker_host_aliases()

    patched_once = _current_getaddrinfo()
    assert patched_once is not marker

    network.install_docker_host_aliases()
    assert _current_getaddrinfo() is patched_once
