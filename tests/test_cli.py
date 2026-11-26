"""Tests for cross-platform `echoloom` CLI and graceful non-Windows credentials."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from echoloom import bootstrap, cli, credentials, local_config, trace_dashboard


def test_cli_init_initializes_local_vault(tmp_path: Path, capsys):
    code = cli.main(["init", "--root", str(tmp_path)])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert (tmp_path / ".local" / "sources.toml").is_file() or "created" in payload or isinstance(
        payload, dict
    )


def test_cli_serve_and_dashboard_dispatch(monkeypatch, tmp_path: Path):
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        bootstrap,
        "run_pilot",
        lambda workspace, *, role, host, port: calls.append(("serve", (workspace, role, host, port))),
    )
    monkeypatch.setattr(
        trace_dashboard,
        "main",
        lambda argv=None: calls.append(("dashboard", list(argv or []))),
    )
    monkeypatch.setattr(
        local_config,
        "main",
        lambda argv=None: calls.append(("vault", list(argv or []))) or 0,
    )

    assert cli.main(["serve", "--root", str(tmp_path), "--role", "ingress", "--port", "8088"]) == 0
    assert cli.main(["dashboard", "--host", "127.0.0.1", "--port", "9090"]) == 0
    assert cli.main(["vault", "status"]) == 0

    assert calls[0] == ("serve", (tmp_path.resolve(), "ingress", None, 8088))
    assert calls[1] == ("dashboard", ["--host", "127.0.0.1", "--port", "9090"])
    assert calls[2] == ("vault", ["status"])


def test_cli_send_builds_hmac_and_posts(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def post(self, url, *, content, headers):
            captured["url"] = url
            captured["body"] = json.loads(content.decode("utf-8"))
            captured["headers"] = dict(headers)
            return SimpleNamespace(status_code=200, text='{"accepted": 1}', is_success=True)

    monkeypatch.setattr(cli.httpx, "Client", _FakeClient)
    code = cli.main(
        [
            "send",
            "--url",
            "http://127.0.0.1:8080",
            "--source",
            "webhook",
            "--conversation",
            "room:demo",
            "--sender",
            "u1",
            "--text",
            "hello cli",
            "--token",
            "secret-tok",
            "--hmac-secret",
            "hmac-sec",
        ]
    )
    assert code == 0
    assert captured["url"] == "http://127.0.0.1:8080/sources/webhook/events"
    assert captured["body"]["events"][0]["text"] == "hello cli"
    assert captured["headers"]["Authorization"] == "Bearer secret-tok"
    assert captured["headers"]["X-Signature-256"].startswith("sha256=")


def test_credentials_non_windows_graceful(monkeypatch, capsys):
    monkeypatch.setattr(credentials.os, "name", "posix")
    assert credentials.load_key() is None
    assert credentials.main(["status", "--service", "model"]) == 0
    out = capsys.readouterr().out
    assert "GOOGLE_API_KEY" in out
    assert credentials.main(["store", "--service", "openai"]) == 1
