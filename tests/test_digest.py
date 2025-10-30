import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from daily_messenger.digest import make_daily as digest


def _theme(label: str, total: float) -> dict:
    return {
        "label": label,
        "total": total,
        "breakdown": {
            "fundamental": 70,
            "valuation": 60,
            "sentiment": 55,
            "liquidity": 65,
            "event": 50,
        },
    }


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_build_summary_lines_limits_length():
    themes = [_theme("AI", 82), _theme("BTC", 65)]
    actions = [
        {"action": "增持", "name": "AI", "reason": "总分 82 高于增持阈值"},
    ]
    lines = digest._build_summary_lines(themes, actions, degraded=True)

    assert lines[0] == "⚠️ 数据延迟，以下为中性参考。"
    assert "AI 总分" in lines[1]
    assert len(lines) <= 12


def test_build_card_payload_contains_summary_and_url():
    lines = ["AI 总分 82", "操作：增持 AI"]
    payload = digest._build_card_payload(
        "内参", lines, "https://example.com/report.html"
    )

    assert payload["header"]["title"]["content"] == "内参"
    assert payload["elements"][0]["text"]["content"].startswith("AI 总分 82")
    assert (
        payload["elements"][1]["actions"][0]["url"] == "https://example.com/report.html"
    )


def test_run_generates_digest_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(digest, "OUT_DIR", tmp_path)
    monkeypatch.setattr(digest, "TEMPLATE_DIR", tmp_path / "templates")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/daily-messenger")

    scores_payload = {
        "date": "2024-04-01",
        "degraded": False,
        "themes": [
            {
                "label": "AI",
                "name": "ai",
                "total": 82.3,
                "breakdown": {
                    "fundamental": 78.0,
                    "valuation": 65.0,
                    "sentiment": 58.0,
                    "liquidity": 62.0,
                    "event": 55.0,
                },
            }
        ],
        "events": [
            {
                "title": "收益季焦点",
                "date": "2024-04-02",
                "impact": "high",
            }
        ],
    }
    actions_payload = {
        "items": [
            {"action": "增持", "name": "AI", "reason": "总分高于增持阈值"},
        ]
    }

    _write_json(tmp_path / "scores.json", scores_payload)
    _write_json(tmp_path / "actions.json", actions_payload)

    exit_code = digest.run([])

    assert exit_code == 0
    assert (tmp_path / "index.html").exists()
    assert (tmp_path / "2024-04-01.html").exists()

    summary_text = (tmp_path / "digest_summary.txt").read_text(encoding="utf-8")
    assert "AI 总分 82" in summary_text

    news_text = (tmp_path / "digest_news.txt").read_text(encoding="utf-8")
    assert digest.NEWS_FALLBACK in news_text

    html_report = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "AI 市场资讯（GLM）" in html_report
    assert digest.NEWS_FALLBACK in html_report

    card_payload = json.loads(
        (tmp_path / "digest_card.json").read_text(encoding="utf-8")
    )
    action_element = next(
        element
        for element in card_payload["elements"]
        if element.get("tag") == "action"
    )
    assert (
        action_element["actions"][0]["url"]
        == "https://acme.github.io/daily-messenger/2024-04-01.html"
    )

    meta = json.loads((tmp_path / "run_meta.json").read_text(encoding="utf-8"))
    digest_meta = meta["steps"]["digest"]
    assert digest_meta["status"] == "completed"
    assert not digest_meta.get("degraded")


def test_run_with_degraded_flag_marks_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(digest, "OUT_DIR", tmp_path)
    monkeypatch.setattr(digest, "TEMPLATE_DIR", tmp_path / "templates")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)

    scores_payload = {
        "date": "2024-04-03",
        "degraded": False,
        "themes": [
            {
                "label": "BTC",
                "name": "btc",
                "total": 55.0,
                "breakdown": {
                    "fundamental": 50.0,
                    "valuation": 52.0,
                    "sentiment": 48.0,
                    "liquidity": 57.0,
                    "event": 45.0,
                },
            }
        ],
        "events": [],
    }
    actions_payload = {"items": []}

    _write_json(tmp_path / "scores.json", scores_payload)
    _write_json(tmp_path / "actions.json", actions_payload)

    exit_code = digest.run(["--degraded"])

    assert exit_code == 0
    assert (tmp_path / "index.html").exists()
    assert (tmp_path / "2024-04-03.html").exists()

    html = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "数据延迟" in html

    summary_text = (tmp_path / "digest_summary.txt").read_text(encoding="utf-8")
    assert summary_text.startswith("⚠️ 数据延迟")

    card_payload = json.loads(
        (tmp_path / "digest_card.json").read_text(encoding="utf-8")
    )
    assert "（数据延迟）" in card_payload["header"]["title"]["content"]

    meta = json.loads((tmp_path / "run_meta.json").read_text(encoding="utf-8"))
    digest_meta = meta["steps"]["digest"]
    assert digest_meta["status"] == "completed"
    assert digest_meta.get("degraded")


def test_run_with_frozen_clock_renders_stable_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(digest, "OUT_DIR", tmp_path)
    monkeypatch.setattr(digest, "TEMPLATE_DIR", tmp_path / "templates")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            base = datetime(2024, 4, 5, 8, 30, tzinfo=timezone.utc)
            if tz is None:
                return base
            return base.astimezone(tz)

    monkeypatch.setattr(digest, "datetime", FrozenDateTime)

    scores_payload = {
        "date": "2024-04-05",
        "degraded": False,
        "themes": [
            {
                "label": "AI",
                "name": "ai",
                "total": 82.3,
                "breakdown": {
                    "fundamental": 78.0,
                    "valuation": 65.0,
                    "sentiment": 58.0,
                    "liquidity": 62.0,
                    "event": 55.0,
                },
            }
        ],
        "events": [],
    }
    actions_payload = {"items": []}

    _write_json(tmp_path / "scores.json", scores_payload)
    _write_json(tmp_path / "actions.json", actions_payload)

    exit_code = digest.run([])

    assert exit_code == 0
    html = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "2024-04-05 08:30 UTC" in html


def test_run_writes_market_news_from_ai_updates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(digest, "OUT_DIR", tmp_path)
    monkeypatch.setattr(digest, "TEMPLATE_DIR", tmp_path / "templates")

    scores_payload = {
        "date": "2024-04-06",
        "themes": [],
        "events": [],
        "ai_updates": [
            {
                "market": "us",
                "prompt_date": "2024-04-05",
                "summary": "- 美股要点 A\n- 美股要点 B",
            },
            {
                "market": "cn",
                "prompt_date": "2024-04-05",
                "summary": "- A 股要点",
            },
        ],
    }
    actions_payload = {"items": []}

    _write_json(tmp_path / "scores.json", scores_payload)
    _write_json(tmp_path / "actions.json", actions_payload)

    exit_code = digest.run([])

    assert exit_code == 0
    news_text = (tmp_path / "digest_news.txt").read_text(encoding="utf-8")
    lines = [line for line in news_text.strip().splitlines() if line]
    assert lines[0].startswith("美股 · 2024-04-05")
    assert lines[1] == "- 美股要点 A"
    assert "A 股要点" in news_text

    html_report = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "AI 市场资讯（GLM）" in html_report
    assert "美股 · 2024-04-05" in html_report
    assert "A 股要点" in html_report
