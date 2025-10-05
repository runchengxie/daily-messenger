import math
import sys
from pathlib import Path

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
