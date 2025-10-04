import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from digest import make_daily as digest


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
    payload = digest._build_card_payload("内参", lines, "https://example.com/report.html")

    assert payload["header"]["title"]["content"] == "内参"
    assert payload["elements"][0]["text"]["content"].startswith("AI 总分 82")
    assert payload["elements"][1]["actions"][0]["url"] == "https://example.com/report.html"
