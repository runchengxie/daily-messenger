import json
import math
import sys
from pathlib import Path

import pytest
import yaml

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scoring import run_scores as scoring


def test_score_ai_produces_weighted_total():
    config = scoring._load_config()
    weights = config["weights"]["theme_ai"]
    market = {
        "indices": [{"symbol": "NDX", "change_pct": 0.01}],
        "sectors": [{"name": "AI", "performance": 1.1}],
    }

    result = scoring._score_ai(market, weights, degraded=False)

    assert result.name == "ai"
    assert not result.degraded
    assert 0.0 <= result.total <= 100.0

    degraded_result = scoring._score_ai(market, weights, degraded=True)
    expected_total = 50 * sum(weights.values())

    assert degraded_result.degraded
    assert math.isclose(degraded_result.total, expected_total, rel_tol=1e-9)


def test_score_magnificent7_uses_theme_metrics():
    config = scoring._load_config()
    weights = config["weights"]["theme_m7"]
    market = {
        "themes": {
            "magnificent7": {
                "change_pct": 1.2,
                "avg_pe": 28.0,
                "avg_ps": 6.5,
                "market_cap": 12_000_000_000_000,
            }
        }
    }

    result = scoring._score_magnificent7(market, weights, degraded=False)

    assert result.name == "magnificent7"
    assert result.total >= 0
    assert not result.degraded


def test_build_actions_generates_expected_labels():
    thresholds = scoring._load_config()["thresholds"]
    high_theme = scoring.ThemeScore(
        name="ai",
        label="AI",
        total=thresholds["action_add"] + 5,
        breakdown={"fundamental": 80, "valuation": 70, "sentiment": 60, "liquidity": 65, "event": 55},
    )
    neutral_theme = scoring.ThemeScore(
        name="btc",
        label="BTC",
        total=(thresholds["action_add"] + thresholds["action_trim"]) / 2,
        breakdown={"fundamental": 60, "valuation": 55, "sentiment": 50, "liquidity": 45, "event": 65},
    )
    degraded_theme = scoring.ThemeScore(
        name="other",
        label="Other",
        total=40,
        breakdown={"fundamental": 50, "valuation": 50, "sentiment": 50, "liquidity": 50, "event": 50},
        degraded=True,
    )

    actions = scoring._build_actions([high_theme, neutral_theme, degraded_theme], thresholds)

    assert actions[0]["action"] == "增持"
    assert actions[1]["action"] == "观察"
    assert actions[2]["reason"] == "数据降级，保持中性"


def _setup_scoring_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, trading_day: str
) -> tuple[Path, Path, Path]:
    out_dir = tmp_path / "out"
    state_dir = tmp_path / "state"
    config_path = tmp_path / "weights.yml"
    monkeypatch.setattr(scoring, "OUT_DIR", out_dir)
    monkeypatch.setattr(scoring, "STATE_DIR", state_dir)
    monkeypatch.setattr(scoring, "CONFIG_PATH", config_path)
    monkeypatch.setattr(scoring, "SENTIMENT_HISTORY_PATH", state_dir / "sentiment_history.json")
    monkeypatch.setattr(scoring, "SCORE_HISTORY_PATH", state_dir / "score_history.json")
    monkeypatch.setattr(scoring, "_current_trading_day", lambda: trading_day)
    return out_dir, state_dir, config_path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _sample_raw_market() -> dict:
    return {
        "market": {
            "indices": [{"symbol": "NDX", "change_pct": 0.01}],
            "sectors": [{"name": "AI", "performance": 1.6}],
            "themes": {
                "ai": {"performance": 1.6, "change_pct": 1.0, "avg_pe": 50.0, "avg_ps": 10.0},
                "magnificent7": {
                    "change_pct": 0.8,
                    "avg_pe": 30.0,
                    "avg_ps": 6.0,
                    "market_cap": 12_000_000_000_000,
                },
            },
        },
        "btc": {
            "etf_net_inflow_musd": 12.0,
            "funding_rate": 0.008,
            "futures_basis": 0.015,
        },
        "sentiment": {
            "put_call": {"equity": 0.7},
            "aaii": {"bull_bear_spread": -10.0},
        },
    }


def _sample_config(weights: dict, thresholds: dict, *, version: int = 1, changed_at: str = "2024-04-01") -> dict:
    base = {
        "version": version,
        "changed_at": changed_at,
        "weights": {
            "default": {
                "fundamental": 0.3,
                "valuation": 0.25,
                "sentiment": 0.2,
                "liquidity": 0.15,
                "event": 0.1,
            },
            "theme_m7": {
                "fundamental": 0.35,
                "valuation": 0.2,
                "sentiment": 0.2,
                "liquidity": 0.15,
                "event": 0.1,
            },
            "theme_btc": {
                "fundamental": 0.1,
                "valuation": 0.15,
                "sentiment": 0.3,
                "liquidity": 0.3,
                "event": 0.15,
            },
        },
        "thresholds": thresholds,
    }
    base["weights"]["theme_ai"] = weights
    return base


def test_run_skips_when_state_exists_unless_forced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_day = "2024-04-05"
    out_dir, state_dir, config_path = _setup_scoring_environment(tmp_path, monkeypatch, trading_day)

    config = _sample_config(
        weights={
            "fundamental": 0.1,
            "valuation": 0.3,
            "sentiment": 0.25,
            "liquidity": 0.2,
            "event": 0.15,
        },
        thresholds={"action_add": 80, "action_trim": 40},
    )
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")

    _write_json(out_dir / "raw_market.json", _sample_raw_market())
    _write_json(out_dir / "raw_events.json", {"events": []})
    _write_json(out_dir / "etl_status.json", {"ok": True, "sources": []})

    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / f"done_{trading_day}"
    state_path.write_text("cached", encoding="utf-8")

    exit_skip = scoring.run([])

    assert exit_skip == 0
    assert not (out_dir / "scores.json").exists()
    assert state_path.read_text(encoding="utf-8") == "cached"

    exit_force = scoring.run(["--force"])

    assert exit_force == 0
    assert (out_dir / "scores.json").exists()
    assert state_path.read_text(encoding="utf-8") == trading_day

    meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    assert meta["steps"]["scoring"]["status"] == "completed"


def test_run_with_strict_mode_aborts_on_degraded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_day = "2024-04-07"
    out_dir, state_dir, config_path = _setup_scoring_environment(tmp_path, monkeypatch, trading_day)

    config = _sample_config(
        weights={
            "fundamental": 0.2,
            "valuation": 0.3,
            "sentiment": 0.2,
            "liquidity": 0.2,
            "event": 0.1,
        },
        thresholds={"action_add": 75, "action_trim": 40},
    )
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")

    _write_json(out_dir / "raw_market.json", _sample_raw_market())
    _write_json(out_dir / "raw_events.json", {"events": []})
    _write_json(out_dir / "etl_status.json", {"ok": False, "sources": []})

    monkeypatch.setenv("STRICT", "1")

    exit_code = scoring.run([])

    assert exit_code == 2
    assert not (out_dir / "scores.json").exists()
    assert not (state_dir / f"done_{trading_day}").exists()

    meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    assert meta["steps"]["scoring"]["status"] == "failed"


def test_config_update_adjusts_scores_and_actions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_day = "2024-04-06"
    out_dir, state_dir, config_path = _setup_scoring_environment(tmp_path, monkeypatch, trading_day)

    weights_initial = {
        "fundamental": 0.1,
        "valuation": 0.3,
        "sentiment": 0.25,
        "liquidity": 0.2,
        "event": 0.15,
    }
    thresholds_initial = {"action_add": 80, "action_trim": 40}
    config_initial = _sample_config(weights_initial, thresholds_initial, version=1, changed_at="2024-04-05")
    config_path.write_text(yaml.safe_dump(config_initial, allow_unicode=True), encoding="utf-8")

    _write_json(out_dir / "raw_market.json", _sample_raw_market())
    _write_json(
        out_dir / "raw_events.json",
        {"events": [{"date": trading_day, "title": "CPI 发布", "impact": "high"}]},
    )
    _write_json(out_dir / "etl_status.json", {"ok": True, "sources": ["alpha_vantage"]})

    first_exit = scoring.run(["--force"])
    assert first_exit == 0

    scores_path = out_dir / "scores.json"
    actions_path = out_dir / "actions.json"
    scores_initial = json.loads(scores_path.read_text(encoding="utf-8"))
    actions_initial = json.loads(actions_path.read_text(encoding="utf-8"))["items"]

    ai_theme_initial = next(theme for theme in scores_initial["themes"] if theme["name"] == "ai")
    action_ai_initial = next(item for item in actions_initial if item["name"] == "AI")

    weights_updated = {
        "fundamental": 0.5,
        "valuation": 0.2,
        "sentiment": 0.15,
        "liquidity": 0.1,
        "event": 0.05,
    }
    thresholds_updated = {"action_add": 55, "action_trim": 40}
    config_updated = _sample_config(weights_updated, thresholds_updated, version=2, changed_at="2024-04-06")
    config_path.write_text(yaml.safe_dump(config_updated, allow_unicode=True), encoding="utf-8")

    second_exit = scoring.run(["--force"])
    assert second_exit == 0

    scores_updated = json.loads(scores_path.read_text(encoding="utf-8"))
    actions_updated = json.loads(actions_path.read_text(encoding="utf-8"))["items"]

    ai_theme_updated = next(theme for theme in scores_updated["themes"] if theme["name"] == "ai")
    action_ai_updated = next(item for item in actions_updated if item["name"] == "AI")

    assert ai_theme_updated["total"] > ai_theme_initial["total"]
    assert action_ai_initial["action"] == "观察"
    assert action_ai_updated["action"] == "增持"

    meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    scoring_meta = meta["steps"]["scoring"]
    assert scoring_meta["config_version"] == 2
