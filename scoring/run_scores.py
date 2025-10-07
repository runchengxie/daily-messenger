#!/usr/bin/env python3
"""Derive scores and action recommendations from raw ETL output."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "out"
STATE_DIR = BASE_DIR / "state"
CONFIG_PATH = BASE_DIR / "config" / "weights.yml"
SENTIMENT_HISTORY_PATH = STATE_DIR / "sentiment_history.json"

PUT_CALL_HISTORY_LIMIT = 252
AAII_HISTORY_LIMIT = 104

from .adaptors import sentiment as sentiment_adaptor


@dataclass
class ThemeScore:
    name: str
    label: str
    total: float
    breakdown: Dict[str, float]
    degraded: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "label": self.label,
            "total": round(self.total, 2),
            "breakdown": {k: round(v, 2) for k, v in self.breakdown.items()},
            "degraded": self.degraded,
        }


def _current_trading_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"缺少配置文件: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _scale(value: float, midpoint: float = 0.0, sensitivity: float = 15.0) -> float:
    score = 50 + (value - midpoint) * sensitivity
    return max(0.0, min(100.0, score))


def _inverse_ratio_score(value: float | None, baseline: float, sensitivity: float) -> float:
    if value is None or value <= 0:
        return 50.0
    ratio = baseline / value
    return _scale(ratio, midpoint=1.0, sensitivity=sensitivity)


def _market_cap_score(value: float | None, baseline_trillions: float) -> float:
    if value is None or value <= 0:
        return 50.0
    trillions = value / 1_000_000_000_000
    return _scale(trillions, midpoint=baseline_trillions, sensitivity=6.0)


def _coerce_float(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _append_history(history: Dict[str, List[float]], key: str, value: float | None, limit: int) -> None:
    if value is None:
        return
    series = history.setdefault(key, [])
    series.append(value)
    if len(series) > limit:
        del series[:-limit]


def _score_ai(
    market: Dict[str, object],
    weights: Dict[str, float],
    degraded: bool,
    sentiment_score: float = 50.0,
) -> ThemeScore:
    sectors = market.get("sectors", []) if market else []
    themes = market.get("themes", {}) if market else {}
    theme_ai = themes.get("ai", {}) if isinstance(themes, dict) else {}
    ai_perf = theme_ai.get("performance")
    if ai_perf is None:
        ai_perf = next((s.get("performance") for s in sectors if s.get("name") == "AI"), 1.0)
    index_change = theme_ai.get("change_pct")
    if index_change is None:
        index_change = next((i.get("change_pct") for i in market.get("indices", []) if i.get("symbol") == "NDX"), 0.0)
    avg_pe = theme_ai.get("avg_pe") if isinstance(theme_ai, dict) else None
    avg_ps = theme_ai.get("avg_ps") if isinstance(theme_ai, dict) else None

    breakdown = {
        "fundamental": _scale(ai_perf, midpoint=1.0, sensitivity=40),
        "valuation": _inverse_ratio_score(avg_pe, baseline=35.0, sensitivity=90.0),
        "sentiment": sentiment_score,
        "liquidity": _inverse_ratio_score(avg_ps, baseline=8.0, sensitivity=70.0),
        "event": 70.0,
    }
    if degraded:
        breakdown = {k: 50.0 for k in breakdown}

    total = sum(weights[k] * breakdown[k] for k in weights)
    return ThemeScore(name="ai", label="AI", total=total, breakdown=breakdown, degraded=degraded)


def _score_btc(
    btc: Dict[str, object],
    weights: Dict[str, float],
    degraded: bool,
    sentiment_score: float = 50.0,
) -> ThemeScore:
    if not btc:
        degraded = True
        btc = {"etf_net_inflow_musd": 0.0, "funding_rate": 0.0, "futures_basis": 0.0}

    breakdown = {
        "fundamental": 50.0,
        "valuation": _scale(-abs(btc.get("futures_basis", 0.0)), midpoint=-0.01, sensitivity=250),
        "sentiment": sentiment_score,
        "liquidity": _scale(btc.get("etf_net_inflow_musd", 0.0), midpoint=0.0, sensitivity=1.5),
        "event": 65.0,
    }
    if degraded:
        breakdown = {k: 50.0 for k in breakdown}

    total = sum(weights[k] * breakdown[k] for k in weights)
    return ThemeScore(name="btc", label="BTC", total=total, breakdown=breakdown, degraded=degraded)


def _score_magnificent7(
    market: Dict[str, object],
    weights: Dict[str, float],
    degraded: bool,
    sentiment_score: float = 50.0,
) -> ThemeScore:
    themes = market.get("themes", {}) if market else {}
    theme = themes.get("magnificent7", {}) if isinstance(themes, dict) else {}
    change_pct = theme.get("change_pct", 0.0)
    avg_pe = theme.get("avg_pe")
    avg_ps = theme.get("avg_ps")
    market_cap = theme.get("market_cap")

    breakdown = {
        "fundamental": _market_cap_score(market_cap, baseline_trillions=11.5),
        "valuation": _inverse_ratio_score(avg_pe, baseline=32.0, sensitivity=85.0),
        "sentiment": sentiment_score,
        "liquidity": _inverse_ratio_score(avg_ps, baseline=7.0, sensitivity=65.0),
        "event": 68.0,
    }
    if degraded:
        breakdown = {k: 50.0 for k in breakdown}

    total = sum(weights[k] * breakdown[k] for k in weights)
    return ThemeScore(name="magnificent7", label="Magnificent 7", total=total, breakdown=breakdown, degraded=degraded)


def _build_actions(themes: List[ThemeScore], thresholds: Dict[str, float]) -> List[Dict[str, str]]:
    actions: List[Dict[str, str]] = []
    for theme in themes:
        if theme.degraded:
            actions.append({
                "name": theme.label,
                "action": "观察",
                "reason": "数据降级，保持中性"})
            continue
        if theme.total >= thresholds.get("action_add", 75):
            actions.append({
                "name": theme.label,
                "action": "增持",
                "reason": f"总分 {theme.total:.0f} 高于增持阈值"})
        elif theme.total <= thresholds.get("action_trim", 45):
            actions.append({
                "name": theme.label,
                "action": "减持",
                "reason": f"总分 {theme.total:.0f} 低于减持阈值"})
        else:
            actions.append({
                "name": theme.label,
                "action": "观察",
                "reason": f"总分 {theme.total:.0f} 处于中性区间"})
    return actions


def _save_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute theme scores")
    parser.add_argument("--force", action="store_true", help="忽略幂等标记，强制重新计算")
    args = parser.parse_args(argv)

    trading_day = _current_trading_day()
    state_path = STATE_DIR / f"done_{trading_day}"

    if state_path.exists() and not args.force:
        print(f"检测到 {trading_day} 已经生成过结果，跳过计算。")
        return 0

    config = _load_config()
    weights = config.get("weights", {})
    thresholds = config.get("thresholds", {})

    raw_market = _load_json(OUT_DIR / "raw_market.json")
    raw_events = _load_json(OUT_DIR / "raw_events.json")
    etl_status = _load_json(OUT_DIR / "etl_status.json")

    degraded = not etl_status.get("ok", False)
    if not raw_market:
        degraded = True
        print("未找到行情数据，使用降级模式。", file=sys.stderr)
    if not raw_events:
        print("未找到事件数据，将在报告中提示。", file=sys.stderr)

    if degraded and os.getenv("STRICT"):
        print("STRICT 模式启用：检测到数据降级，立即退出。", file=sys.stderr)
        return 2

    sentiment_node = raw_market.get("sentiment", {}) if isinstance(raw_market, dict) else {}

    existing_history = _load_json(SENTIMENT_HISTORY_PATH)
    if not isinstance(existing_history, dict):
        existing_history = {}

    history: Dict[str, List[float]] = {}
    for key, limit in (("put_call_equity", PUT_CALL_HISTORY_LIMIT), ("aaii_bull_bear_spread", AAII_HISTORY_LIMIT)):
        raw_values = existing_history.get(key)
        series: List[float] = []
        if isinstance(raw_values, list):
            for value in raw_values:
                number = _coerce_float(value)
                if number is not None:
                    series.append(number)
        history[key] = series[-limit:]

    if isinstance(sentiment_node, dict):
        put_call = sentiment_node.get("put_call")
        if isinstance(put_call, dict):
            equity_value = _coerce_float(put_call.get("equity"))
            _append_history(history, "put_call_equity", equity_value, PUT_CALL_HISTORY_LIMIT)
        aaii = sentiment_node.get("aaii")
        if isinstance(aaii, dict):
            spread_value = _coerce_float(aaii.get("bull_bear_spread"))
            _append_history(history, "aaii_bull_bear_spread", spread_value, AAII_HISTORY_LIMIT)

    sentiment_result = None
    if isinstance(sentiment_node, dict):
        sentiment_result = sentiment_adaptor.aggregate(sentiment_node, history)

    sentiment_score_value = sentiment_result.score if sentiment_result else 50.0

    market_payload = raw_market.get("market", {})
    btc_payload = raw_market.get("btc", {})

    theme_ai = _score_ai(
        market_payload,
        weights.get("theme_ai", weights.get("default", {})),
        degraded,
        sentiment_score=sentiment_score_value,
    )

    btc_sentiment_value = _scale(btc_payload.get("funding_rate", 0.0), midpoint=0.005, sensitivity=600)
    theme_btc = _score_btc(
        btc_payload,
        weights.get("theme_btc", weights.get("default", {})),
        degraded,
        sentiment_score=btc_sentiment_value,
    )

    theme_m7 = _score_magnificent7(
        market_payload,
        weights.get("theme_m7", weights.get("default", {})),
        degraded,
        sentiment_score=sentiment_score_value,
    )

    themes = [theme_ai, theme_m7, theme_btc]
    actions = _build_actions(themes, thresholds)

    scores_payload = {
        "date": trading_day,
        "themes": [theme.to_dict() for theme in themes],
        "events": raw_events.get("events", []),
        "degraded": degraded,
    }
    if sentiment_result:
        scores_payload["sentiment"] = sentiment_result.to_dict()

    SENTIMENT_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SENTIMENT_HISTORY_PATH.open("w", encoding="utf-8") as f_history:
        json.dump(history, f_history, ensure_ascii=False, indent=2)
    _save_json(OUT_DIR / "scores.json", scores_payload)

    actions_payload = {
        "date": trading_day,
        "items": actions,
    }
    _save_json(OUT_DIR / "actions.json", actions_payload)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path.write_text(trading_day, encoding="utf-8")

    for theme in themes:
        print(f"{theme.label} 主题总分: {theme.total:.1f}")
    print("打分计算完成，结果已写入 out/ 目录。")
    return 0


if __name__ == "__main__":
    sys.exit(run())
