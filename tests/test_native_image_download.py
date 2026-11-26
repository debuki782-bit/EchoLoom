import hashlib
import importlib.util
import io
import json
from pathlib import Path

import httpx
import pytest
from PIL import Image

from echoloom.native_image_download import NativeImageDownloader, download_spec

buffer = io.BytesIO()
Image.new("RGB", (16, 12), "purple").save(buffer, format="PNG")
PAYLOAD = buffer.getvalue()


def refs(payload=PAYLOAD):
    return [{"field": "cdnmidimgurl", "value": "native-resource"},
            {"field": "md5", "value": hashlib.md5(payload).hexdigest()},
            {"field": "length", "value": str(len(payload))}]


@pytest.fixture
def service_module():
    path = Path(__file__).resolve().parents[1] / "scripts/run-native-image-download.py"
    spec = importlib.util.spec_from_file_location("native_download_service_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_image_selection_uses_hd_only_with_its_own_hash():
    regular = refs() + [{"field": "cdnhdimgurl", "value": "sender-original"}]
    assert download_spec("bot", regular)["image_type"] == 2
    hd = regular + [{"field": "hdmd5", "value": "a" * 32}, {"field": "hdlength", "value": "100"}]
    spec = download_spec("bot", hd)
    assert (spec["image_type"], spec["fileid"], spec["expected_md5"]) == (1, "sender-original", "a" * 32)
    assert "aeskey" not in spec


def test_client_checks_actual_bytes_and_never_uses_server_supplied_path(tmp_path):
    spec = download_spec("bot", refs())
    root = tmp_path / "data/native-image-downloads"
    root.mkdir(parents=True)
    source = root / (spec["job_id"] + ".bin")
    source.write_bytes(PAYLOAD)
    seen = []

    def serve(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"state": "ready", "job_id": spec["job_id"],
                                         "path": "C:/unrelated-file"})

    client = NativeImageDownloader(tmp_path, "http://127.0.0.1:30002", transport=httpx.MockTransport(serve))
    target = tmp_path / "output"
    assert client.fetch("bot", refs(), target)
    assert target.read_bytes() == PAYLOAD
    assert seen == [spec]
    target.unlink()
    source.write_bytes(b"unrelated image")
    assert not client.fetch("bot", refs(), target)
    assert not target.exists()


def test_unknown_operation_can_supply_verified_file_without_new_identity(tmp_path):
    spec = download_spec("bot", refs())
    root = tmp_path / "data/native-image-downloads"
    root.mkdir(parents=True)
    (root / (spec["job_id"] + ".bin")).write_bytes(PAYLOAD)
    calls = []

    def serve(request):
        calls.append(json.loads(request.content)["job_id"])
        return httpx.Response(200, json={"state": "unknown", "job_id": spec["job_id"]})

    client = NativeImageDownloader(tmp_path, "http://127.0.0.1:30002", transport=httpx.MockTransport(serve))
    target = tmp_path / "output"
    assert client.fetch("bot", refs(), target)
    assert target.read_bytes() == PAYLOAD
    assert calls == [spec["job_id"]]


@pytest.mark.parametrize("url", ["http://example.com:30002", "http://127.0.0.1:30002/other",
                                "http://user@127.0.0.1:30002", "https://127.0.0.1:30002"])
def test_private_transport_rejects_other_origins(tmp_path, url):
    with pytest.raises(ValueError):
        NativeImageDownloader(tmp_path, url)


def test_download_service_matches_message_and_deduplicates(tmp_path, service_module):
    calls = []

    class Adapter:
        def submit(self, spec, path):
            calls.append(spec["job_id"])
            path.write_bytes(PAYLOAD)
            return 0

        def result(self, spec):
            return {"completed": True, "code": 0, "resource_bound": True}

    service = service_module.DownloadService(tmp_path, Adapter(), "bot", lambda: True)
    try:
        spec = download_spec("bot", refs())
        service.submit(spec)
        service.pending.join()
        assert service.get(spec["job_id"])["state"] == "ready"
        assert service.submit(spec)["state"] == "ready"
        assert calls == [spec["job_id"]]
        with pytest.raises(ValueError):
            service.submit({**spec, "fileid": "different-resource"})
        with pytest.raises(ValueError):
            service.submit({**spec, "account_id": "another-account"})
    finally:
        service.close()


def test_restarted_unknown_job_is_never_resubmitted(tmp_path, service_module):
    class Adapter:
        def submit(self, *_):
            pytest.fail("unknown native operation must not be retried")

    spec = download_spec("bot", refs())
    service = service_module.DownloadService(tmp_path, Adapter(), "bot", lambda: True)
    service.persist(spec["job_id"], "submitted")
    service.close()
    service = service_module.DownloadService(tmp_path, Adapter(), "bot", lambda: True)
    try:
        assert service.submit(spec)["state"] == "unknown"
    finally:
        service.close()


def test_account_change_blocks_native_call(tmp_path, service_module):
    class Adapter:
        def submit(self, *_):
            pytest.fail("must verify the logged-in account before submitting")

    service = service_module.DownloadService(tmp_path, Adapter(), "bot", lambda: False)
    try:
        spec = download_spec("bot", refs())
        service.submit(spec)
        service.pending.join()
        assert service.get(spec["job_id"])["reason"] == "wechat_account_changed_or_unavailable"
    finally:
        service.close()


def test_native_completion_binds_different_encoding_to_exact_resource(tmp_path, service_module):
    class Adapter:
        def submit(self, spec, path):
            path.write_bytes(PAYLOAD)
            return 0

        def result(self, spec):
            return {"completed": True, "code": 0, "resource_bound": True}

    service = service_module.DownloadService(tmp_path / "data/native-image-downloads", Adapter(), "bot", lambda: True)
    references = refs()
    references[1]["value"] = "a" * 32  # Message bytes used another encoding.
    spec = download_spec("bot", references)
    try:
        def serve(request):
            service.submit(json.loads(request.content))
            service.pending.join()
            return httpx.Response(200, json=service.get(spec["job_id"]))

        client = NativeImageDownloader(tmp_path, "http://127.0.0.1:30002", transport=httpx.MockTransport(serve))
        target = tmp_path / "archive-image"
        assert client.fetch("bot", references, target)
        assert target.read_bytes() == PAYLOAD
        proof = service.get(spec["job_id"])
        assert proof["verification"] == "native_resource"
        assert proof["native_completion"] == 0
        assert proof["resource_sha256"] == hashlib.sha256(spec["fileid"].encode()).hexdigest()

        proof["resource_sha256"] = "0" * 64
        client.transport = httpx.MockTransport(lambda _: httpx.Response(200, json=proof))
        assert not client.fetch("bot", references, tmp_path / "wrong-resource")
    finally:
        service.close()


def test_native_failure_callback_cannot_promote_download(tmp_path, service_module):
    class Adapter:
        def submit(self, spec, path):
            path.write_bytes(PAYLOAD)
            return 0

        def result(self, spec):
            return {"completed": True, "code": -7, "resource_bound": True}

    service = service_module.DownloadService(tmp_path, Adapter(), "bot", lambda: True)
    try:
        spec = download_spec("bot", refs())
        service.submit(spec)
        service.pending.join()
        assert service.get(spec["job_id"])["state"] == "failed"
    finally:
        service.close()


def test_connect_error_autostarts_service_once_and_completes_download(tmp_path):
    spec = download_spec("bot", refs())
    root = tmp_path / "data/native-image-downloads"
    root.mkdir(parents=True)
    (root / (spec["job_id"] + ".bin")).write_bytes(PAYLOAD)
    service_online = False
    launched_accounts = []

    def launch(account_id: str) -> bool:
        nonlocal service_online
        launched_accounts.append(account_id)
        service_online = True
        return True

    def serve(request: httpx.Request) -> httpx.Response:
        if not service_online:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"state": "ready", "job_id": spec["job_id"]})

    client = NativeImageDownloader(
        tmp_path,
        "http://127.0.0.1:30002",
        transport=httpx.MockTransport(serve),
        service_launcher=launch,
    )
    target = tmp_path / "downloaded-after-autostart.png"
    assert client.fetch("bot", refs(), target)
    assert target.read_bytes() == PAYLOAD
    assert launched_accounts == ["bot"]


def test_failed_autostart_respects_cooldown_and_logs_warning(tmp_path, caplog):
    launched = []

    def fail_launch(account_id: str) -> bool:
        launched.append(account_id)
        return False

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = NativeImageDownloader(
        tmp_path,
        "http://127.0.0.1:30002",
        transport=httpx.MockTransport(refuse),
        service_launcher=fail_launch,
        autostart_cooldown_seconds=60.0,
    )
    target = tmp_path / "never-written"
    assert not client.fetch("bot", refs(), target)
    assert not client.fetch("bot", refs(), target)
    assert launched == ["bot"]
    assert "event=native_image_download_unreachable" in caplog.text

