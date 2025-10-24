import json
from pathlib import Path

import pytest

from daily_messenger.tools import post_feishu


class DummyResponse:
    def __init__(self) -> None:
        self.status_code = 200
        self.headers = {"content-type": "application/json"}
        self.text = "{}"

    def json(self) -> dict:
        return {"StatusCode": 0}


def test_run_without_webhook_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"value": False}

    def fake_post(*args, **kwargs):
        called["value"] = True
        return DummyResponse()

    monkeypatch.setattr(post_feishu.requests, "post", fake_post)
    monkeypatch.delenv("FEISHU_WEBHOOK_DAILY", raising=False)
    monkeypatch.delenv("FEISHU_WEBHOOK_ALERTS", raising=False)
    monkeypatch.delenv("FEISHU_SECRET_DAILY", raising=False)
    monkeypatch.delenv("FEISHU_SECRET_ALERTS", raising=False)

    exit_code = post_feishu.run([])

    assert exit_code == 0
    assert not called["value"]


def test_run_defaults_to_interactive_when_card_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summary_path = tmp_path / "digest_summary.txt"
    summary_path.write_text("AI 总分 82\n操作：增持 AI", encoding="utf-8")

    card_payload = {
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "测试卡片"},
        },
        "config": {"wide_screen_mode": True},
        "elements": [],
    }
    card_path = tmp_path / "digest_card.json"
    card_path.write_text(json.dumps(card_payload, ensure_ascii=False), encoding="utf-8")

    captured: dict = {}

    def fake_post(url: str, json: dict | None = None, timeout: int | None = None):
        captured["url"] = url
        captured["payload"] = json
        captured["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr(post_feishu.requests, "post", fake_post)

    exit_code = post_feishu.run(
        [
            "--webhook",
            "https://example.com/hook",
            "--summary",
            str(summary_path),
            "--card",
            str(card_path),
        ]
    )

    assert exit_code == 0
    assert captured["payload"]["msg_type"] == "interactive"
    assert captured["payload"]["card"]["header"]["title"]["content"] == "测试卡片"
    assert captured["url"] == "https://example.com/hook"


def test_run_defaults_to_post_when_card_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summary_path = tmp_path / "digest_summary.txt"
    summary_path.write_text("AI 总分 60", encoding="utf-8")
    missing_card_path = tmp_path / "absent_card.json"

    captured: dict = {}

    def fake_post(url: str, json: dict | None = None, timeout: int | None = None):
        captured["payload"] = json
        return DummyResponse()

    monkeypatch.setattr(post_feishu.requests, "post", fake_post)

    exit_code = post_feishu.run(
        [
            "--webhook",
            "https://example.com/hook",
            "--summary",
            str(summary_path),
            "--card",
            str(missing_card_path),
        ]
    )

    assert exit_code == 0
    assert captured["payload"]["msg_type"] == "post"
    content_blocks = captured["payload"]["content"]["post"]["zh_cn"]["content"]
    assert any("AI 总分 60" in block[0]["text"] for block in content_blocks)


def test_post_mode_groups_sections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    summary_path = tmp_path / "digest_summary.txt"
    summary_path.write_text(
        "美股｜三点速览：\n- 指数反弹\n- 科技走强\n\n黄金｜两点：\n- 美元走弱\n- 持仓回升\n",
        encoding="utf-8",
    )
    captured: dict = {}

    def fake_post(url: str, json: dict | None = None, timeout: int | None = None):
        captured["payload"] = json
        return DummyResponse()

    monkeypatch.setattr(post_feishu.requests, "post", fake_post)

    exit_code = post_feishu.run(
        [
            "--webhook",
            "https://example.com/hook",
            "--summary",
            str(summary_path),
            "--mode",
            "post",
        ]
    )

    assert exit_code == 0
    blocks = captured["payload"]["content"]["post"]["zh_cn"]["content"]
    assert len(blocks) == 2
    assert blocks[0][0]["text"].startswith("美股｜三点速览")
    assert blocks[0][0]["text"].endswith("\n")
    assert "黄金" in blocks[1][0]["text"]


def test_channel_alerts_reads_dedicated_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict = {}

    def fake_post(url: str, json: dict | None = None, timeout: int | None = None):
        captured["url"] = url
        captured["payload"] = json
        captured["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr(post_feishu.requests, "post", fake_post)
    monkeypatch.setenv("FEISHU_WEBHOOK_ALERTS", "https://example.com/alerts")
    monkeypatch.setenv("FEISHU_SECRET_ALERTS", "alert-secret")
    summary_path = tmp_path / "summary.txt"
    summary_path.write_text("盘中提示", encoding="utf-8")

    exit_code = post_feishu.run(
        ["--channel", "alerts", "--summary", str(summary_path), "--mode", "post"]
    )

    assert exit_code == 0
    assert captured["url"] == "https://example.com/alerts"
