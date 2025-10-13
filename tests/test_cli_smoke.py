import json
import os
from pathlib import Path

import pytest

from daily_messenger import cli
from daily_messenger.common import run_meta
from daily_messenger.digest import make_daily
from daily_messenger.etl import run_fetch
from daily_messenger.scoring import run_scores


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _synthetic_raw_market(trading_day: str) -> dict:
    return {
        "date": trading_day,
        "market": {
            "indices": [
                {"symbol": "SPX", "close": 4825.0, "change_pct": 0.6},
                {"symbol": "NDX", "close": 16250.0, "change_pct": 0.8},
            ],
            "sectors": [
                {"name": "AI", "performance": 1.12},
                {"name": "Defensive", "performance": 0.96},
            ],
            "themes": {
                "ai": {
                    "performance": 1.12,
                    "change_pct": 0.7,
                    "avg_pe": 32.0,
                    "avg_ps": 6.5,
                },
                "magnificent7": {
                    "change_pct": 0.55,
                    "avg_pe": 31.0,
                    "avg_ps": 6.0,
                    "market_cap": 12_500_000_000_000,
                },
            },
        },
        "btc": {
            "etf_net_inflow_musd": 35.2,
            "funding_rate": 0.012,
            "futures_basis": 0.0015,
        },
        "sentiment": {
            "put_call": {"equity": 0.72},
            "aaii": {"bull_bear_spread": -10.5},
        },
    }


def _synthetic_events(trading_day: str) -> dict:
    return {
        "events": [
            {"title": "宏观数据公布", "date": trading_day, "impact": "high"},
            {"title": "龙头财报", "date": trading_day, "impact": "medium"},
        ]
    }


def test_cli_pipeline_contract_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir = tmp_path / "out"
    state_dir = tmp_path / "state"

    monkeypatch.setattr(run_fetch, "OUT_DIR", out_dir)
    monkeypatch.setattr(run_fetch, "STATE_DIR", state_dir)
    monkeypatch.setattr(run_scores, "OUT_DIR", out_dir)
    monkeypatch.setattr(run_scores, "STATE_DIR", state_dir)
    monkeypatch.setattr(run_scores, "SENTIMENT_HISTORY_PATH", state_dir / "sentiment_history.json")
    monkeypatch.setattr(run_scores, "SCORE_HISTORY_PATH", state_dir / "score_history.json")
    monkeypatch.setattr(make_daily, "OUT_DIR", out_dir)

    monkeypatch.setenv("DM_OVERRIDE_DATE", "2024-04-01")
    monkeypatch.setenv("API_KEYS", "{}")
    monkeypatch.setenv("DM_RUN_ID", "cli-smoke")

    def fake_fetch(argv: list[str] | None = None) -> int:
        trading_day = os.getenv("DM_OVERRIDE_DATE", "2024-04-01")
        out_dir.mkdir(parents=True, exist_ok=True)
        state_dir.mkdir(parents=True, exist_ok=True)

        _write_json(out_dir / "raw_market.json", _synthetic_raw_market(trading_day))
        _write_json(out_dir / "raw_events.json", _synthetic_events(trading_day))
        _write_json(
            out_dir / "etl_status.json",
            {
                "date": trading_day,
                "ok": True,
                "sources": [
                    {"name": "market", "ok": True, "message": "synthetic dataset"},
                    {"name": "coinbase_spot", "ok": True, "message": "synthetic dataset"},
                    {"name": "okx_funding", "ok": True, "message": "synthetic dataset"},
                    {"name": "okx_basis", "ok": True, "message": "synthetic dataset"},
                ],
            },
        )
        run_meta.record_step(out_dir, "etl", "completed", trading_day=trading_day, degraded=False)
        (state_dir / f"fetch_{trading_day}").write_text(trading_day, encoding="utf-8")
        return 0

    monkeypatch.setattr(run_fetch, "run", fake_fetch)

    assert cli.main(["fetch", "--force"]) == 0
    assert cli.main(["score", "--force"]) == 0
    assert cli.main(["digest"]) == 0

    etl_status = json.loads((out_dir / "etl_status.json").read_text(encoding="utf-8"))
    assert {"date", "ok", "sources"}.issubset(etl_status.keys())
    assert etl_status["ok"] is True
    assert isinstance(etl_status["sources"], list)

    scores = json.loads((out_dir / "scores.json").read_text(encoding="utf-8"))
    assert {"date", "themes", "events", "degraded", "thresholds", "etl_status"}.issubset(scores.keys())
    assert scores["degraded"] is False
    assert scores["themes"], "theme scores should not be empty"
    for theme in scores["themes"]:
        assert {"name", "label", "total", "breakdown", "weights"}.issubset(theme.keys())
        assert isinstance(theme["breakdown"], dict)
        assert isinstance(theme["weights"], dict)

    actions = json.loads((out_dir / "actions.json").read_text(encoding="utf-8"))
    assert actions["items"], "actions payload should contain at least one recommendation"

    summary_lines = (out_dir / "digest_summary.txt").read_text(encoding="utf-8").strip().splitlines()
    assert summary_lines, "digest summary should contain content"

    card_payload = json.loads((out_dir / "digest_card.json").read_text(encoding="utf-8"))
    assert card_payload["header"]["title"]["content"].startswith("内参")
    assert card_payload["elements"], "card should include body elements"

    meta_payload = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    steps = meta_payload.get("steps", {})
    assert steps.get("etl", {}).get("status") == "completed"
    assert steps.get("scoring", {}).get("status") == "completed"
    assert steps.get("digest", {}).get("status") == "completed"
