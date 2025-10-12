import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from daily_messenger.digest import make_daily as digest


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        base = datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc)
        if tz is None:
            return base
        return base.astimezone(tz)


@pytest.fixture(autouse=True)
def reset_snapshot_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DM_OVERRIDE_DATE", raising=False)
    monkeypatch.delenv("DM_DISABLE_THROTTLE", raising=False)


def test_digest_outputs_match_snapshots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_dir = Path(__file__).resolve().parent / "__snapshots__"
    monkeypatch.setattr(digest, "OUT_DIR", tmp_path)
    monkeypatch.setattr(
        digest,
        "TEMPLATE_DIR",
        Path(__file__).resolve().parents[1] / "src" / "daily_messenger" / "digest" / "templates",
    )
    monkeypatch.setattr(digest, "datetime", _FixedDatetime)
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
                "breakdown_detail": {
                    "fundamental": {"value": 78.0},
                    "valuation": {"value": 65.0},
                    "sentiment": {"value": 58.0},
                    "liquidity": {"value": 62.0},
                    "event": {"value": 55.0},
                },
                "meta": {
                    "delta": 2.5,
                    "previous_total": 79.8,
                    "weights": {
                        "fundamental": 0.3,
                        "valuation": 0.15,
                        "sentiment": 0.25,
                        "liquidity": 0.2,
                        "event": 0.1,
                    },
                    "distance_to_add": -7.3,
                    "distance_to_trim": 37.3,
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
        "thresholds": {"action_add": 85, "action_trim": 45},
        "etl_status": {"ok": True, "sources": []},
        "config_version": 2,
    }
    actions_payload = {
        "items": [
            {"action": "增持", "name": "AI", "reason": "总分高于增持阈值"},
        ]
    }

    (tmp_path / "scores.json").write_text(json.dumps(scores_payload, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "actions.json").write_text(json.dumps(actions_payload, ensure_ascii=False), encoding="utf-8")

    exit_code = digest.run([])
    assert exit_code == 0

    rendered_html = (tmp_path / "index.html").read_text(encoding="utf-8").strip()
    expected_html = (snapshot_dir / "digest_index.html").read_text(encoding="utf-8").strip()
    assert rendered_html == expected_html

    rendered_card = json.loads((tmp_path / "digest_card.json").read_text(encoding="utf-8"))
    expected_card = json.loads((snapshot_dir / "digest_card.json").read_text(encoding="utf-8"))
    assert rendered_card == expected_card
