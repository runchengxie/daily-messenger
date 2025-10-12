#!/usr/bin/env python3
"""Derive scores and action recommendations from raw ETL output."""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from common import run_meta
from common.logging import log, setup_logger

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
OUT_DIR = BASE_DIR / "out"
STATE_DIR = BASE_DIR / "state"
CONFIG_PATH = BASE_DIR / "config" / "weights.yml"
SENTIMENT_HISTORY_PATH = STATE_DIR / "sentiment_history.json"
SCORE_HISTORY_PATH = STATE_DIR / "score_history.json"

PUT_CALL_HISTORY_LIMIT = 252
AAII_HISTORY_LIMIT = 104

from scoring.adaptors import sentiment as sentiment_adaptor


@dataclass
class ThemeScore:
    name: str
    label: str
    total: float
    breakdown: Dict[str, float]
    degraded: bool = False
    breakdown_detail: Dict[str, Dict[str, object]] = field(default_factory=dict)
    weights: Dict[str, float] = field(default_factory=dict)
    meta: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        detail_payload: Dict[str, Dict[str, object]] = {}
        for key, detail in self.breakdown_detail.items():
            if not isinstance(detail, dict):
                continue
            item = {**detail}
            value = item.get("value")
            if isinstance(value, (int, float)):
                item["value"] = round(float(value), 2)
            raw = item.get("raw")
            if isinstance(raw, (int, float)):
                item["raw"] = round(float(raw), 4)
            detail_payload[key] = item
        return {
            "name": self.name,
            "label": self.label,
            "total": round(self.total, 2),
            "breakdown": {k: round(v, 2) for k, v in self.breakdown.items()},
            "breakdown_detail": detail_payload,
            "weights": {k: float(v) for k, v in self.weights.items()},
            "meta": self.meta,
            "degraded": self.degraded,
        }


def _current_trading_day() -> str:
    override = os.getenv("DM_OVERRIDE_DATE")
    if override:
        return override
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"缺少配置文件: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict):
        raise ValueError("配置文件格式错误，期待字典结构")
    config.setdefault("version", 0)
    return config


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


def _make_detail(
    value: float,
    *,
    fallback: bool = False,
    reason: str | None = None,
    source: str | None = None,
    raw: object | None = None,
) -> Dict[str, object]:
    detail: Dict[str, object] = {"value": value, "fallback": fallback}
    if reason:
        detail["reason"] = reason
    if source:
        detail["source"] = source
    if raw is not None:
        detail["raw"] = raw
    return detail


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
    sentiment_fallback: bool = False,
) -> ThemeScore:
    sectors = market.get("sectors", []) if market else []
    themes = market.get("themes", {}) if market else {}
    theme_ai = themes.get("ai", {}) if isinstance(themes, dict) else {}
    weights = dict(weights or {})

    perf_source = "主题行情"
    ai_perf = theme_ai.get("performance")
    if ai_perf is None:
        fallback_sector = next((s.get("performance") for s in sectors if s.get("name") == "AI"), None)
        if fallback_sector is not None:
            perf_source = "板块代理"
            ai_perf = fallback_sector
        else:
            perf_source = "默认 1.0"
            ai_perf = 1.0
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
    breakdown_detail: Dict[str, Dict[str, object]] = {}
    breakdown_detail["fundamental"] = _make_detail(
        breakdown["fundamental"],
        fallback=perf_source == "默认 1.0",
        reason="缺少主题表现，使用默认估计" if perf_source == "默认 1.0" else None,
        source=perf_source,
        raw=ai_perf,
    )
    valuation_missing = avg_pe is None or (isinstance(avg_pe, (int, float)) and avg_pe <= 0)
    breakdown_detail["valuation"] = _make_detail(
        breakdown["valuation"],
        fallback=valuation_missing,
        reason="缺少平均 PE，使用中性值" if valuation_missing else None,
        raw=avg_pe,
    )
    liquidity_missing = avg_ps is None or (isinstance(avg_ps, (int, float)) and avg_ps <= 0)
    breakdown_detail["liquidity"] = _make_detail(
        breakdown["liquidity"],
        fallback=liquidity_missing,
        reason="缺少平均 PS，使用中性值" if liquidity_missing else None,
        raw=avg_ps,
    )
    breakdown_detail["sentiment"] = _make_detail(
        breakdown["sentiment"],
        fallback=sentiment_fallback,
        reason="情绪数据缺口，使用中性值" if sentiment_fallback else None,
    )
    breakdown_detail["event"] = _make_detail(breakdown["event"], fallback=False, source="配置")

    if degraded:
        breakdown = {k: 50.0 for k in breakdown}
        breakdown_detail = {
            k: _make_detail(50.0, fallback=True, reason="数据降级")
            for k in breakdown
        }

    total = sum(weights.get(k, 0.0) * breakdown[k] for k in breakdown)
    return ThemeScore(
        name="ai",
        label="AI",
        total=total,
        breakdown=breakdown,
        degraded=degraded,
        breakdown_detail=breakdown_detail,
        weights=weights,
    )


def _score_btc(
    btc: Dict[str, object],
    weights: Dict[str, float],
    degraded: bool,
    sentiment_score: float = 50.0,
    sentiment_fallback: bool = False,
) -> ThemeScore:
    if not btc:
        degraded = True
        btc = {"etf_net_inflow_musd": 0.0, "funding_rate": 0.0, "futures_basis": 0.0}
    weights = dict(weights or {})

    basis_raw = _coerce_float(btc.get("futures_basis"))
    if basis_raw is None:
        basis_raw = 0.0
        basis_missing = True
    else:
        basis_missing = False
    inflow_raw = _coerce_float(btc.get("etf_net_inflow_musd"))
    if inflow_raw is None:
        inflow_raw = 0.0
        inflow_missing = True
    else:
        inflow_missing = False

    breakdown = {
        "fundamental": 50.0,
        "valuation": _scale(-abs(basis_raw), midpoint=-0.01, sensitivity=250),
        "sentiment": sentiment_score,
        "liquidity": _scale(inflow_raw, midpoint=0.0, sensitivity=1.5),
        "event": 65.0,
    }
    breakdown_detail: Dict[str, Dict[str, object]] = {
        "fundamental": _make_detail(50.0, fallback=False, source="配置"),
        "valuation": _make_detail(
            breakdown["valuation"],
            fallback=basis_missing,
            reason="缺少期货基差，使用中性值" if basis_missing else None,
            raw=basis_raw,
        ),
        "sentiment": _make_detail(
            breakdown["sentiment"],
            fallback=sentiment_fallback,
            reason="资金费率缺失，使用中性值" if sentiment_fallback else None,
            raw=_coerce_float(btc.get("funding_rate")),
        ),
        "liquidity": _make_detail(
            breakdown["liquidity"],
            fallback=inflow_missing,
            reason="ETF 净流入缺失，使用中性值" if inflow_missing else None,
            raw=inflow_raw,
        ),
        "event": _make_detail(65.0, fallback=False, source="配置"),
    }
    if degraded:
        breakdown = {k: 50.0 for k in breakdown}
        breakdown_detail = {
            k: _make_detail(50.0, fallback=True, reason="数据降级")
            for k in breakdown
        }

    total = sum(weights.get(k, 0.0) * breakdown[k] for k in breakdown)
    return ThemeScore(
        name="btc",
        label="BTC",
        total=total,
        breakdown=breakdown,
        degraded=degraded,
        breakdown_detail=breakdown_detail,
        weights=weights,
    )


def _score_magnificent7(
    market: Dict[str, object],
    weights: Dict[str, float],
    degraded: bool,
    sentiment_score: float = 50.0,
    sentiment_fallback: bool = False,
) -> ThemeScore:
    themes = market.get("themes", {}) if market else {}
    theme = themes.get("magnificent7", {}) if isinstance(themes, dict) else {}
    weights = dict(weights or {})
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
    breakdown_detail: Dict[str, Dict[str, object]] = {}
    fundamental_missing = market_cap is None or (isinstance(market_cap, (int, float)) and market_cap <= 0)
    breakdown_detail["fundamental"] = _make_detail(
        breakdown["fundamental"],
        fallback=fundamental_missing,
        reason="缺少总市值，使用中性值" if fundamental_missing else None,
        raw=market_cap,
    )
    valuation_missing = avg_pe is None or (isinstance(avg_pe, (int, float)) and avg_pe <= 0)
    breakdown_detail["valuation"] = _make_detail(
        breakdown["valuation"],
        fallback=valuation_missing,
        reason="缺少平均 PE，使用中性值" if valuation_missing else None,
        raw=avg_pe,
    )
    liquidity_missing = avg_ps is None or (isinstance(avg_ps, (int, float)) and avg_ps <= 0)
    breakdown_detail["liquidity"] = _make_detail(
        breakdown["liquidity"],
        fallback=liquidity_missing,
        reason="缺少平均 PS，使用中性值" if liquidity_missing else None,
        raw=avg_ps,
    )
    breakdown_detail["sentiment"] = _make_detail(
        breakdown["sentiment"],
        fallback=sentiment_fallback,
        reason="情绪数据缺口，使用中性值" if sentiment_fallback else None,
    )
    breakdown_detail["event"] = _make_detail(breakdown["event"], fallback=False, source="配置")
    if degraded:
        breakdown = {k: 50.0 for k in breakdown}
        breakdown_detail = {
            k: _make_detail(50.0, fallback=True, reason="数据降级")
            for k in breakdown
        }

    total = sum(weights.get(k, 0.0) * breakdown[k] for k in breakdown)
    return ThemeScore(
        name="magnificent7",
        label="Magnificent 7",
        total=total,
        breakdown=breakdown,
        degraded=degraded,
        breakdown_detail=breakdown_detail,
        weights=weights,
    )


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

    logger = setup_logger("scoring")
    started_at = datetime.now(timezone.utc)

    trading_day = _current_trading_day()
    logger = setup_logger("scoring", trading_day=trading_day)
    log(logger, logging.INFO, "scoring_start", force=args.force)
    run_meta.record_step(OUT_DIR, "scoring", "started", trading_day=trading_day, force=args.force)

    state_path = STATE_DIR / f"done_{trading_day}"

    if state_path.exists() and not args.force:
        log(logger, logging.INFO, "scoring_skip_cached", state_path=str(state_path))
        run_meta.record_step(OUT_DIR, "scoring", "cached", trading_day=trading_day)
        return 0

    config = _load_config()
    weights = config.get("weights", {})
    thresholds = config.get("thresholds", {})
    config_version = config.get("version")
    config_changed_at = config.get("changed_at")

    raw_market = _load_json(OUT_DIR / "raw_market.json")
    raw_events = _load_json(OUT_DIR / "raw_events.json")
    etl_status = _load_json(OUT_DIR / "etl_status.json")

    degraded = not etl_status.get("ok", False)
    if not raw_market:
        degraded = True
        log(logger, logging.WARNING, "scoring_missing_market_data")
    if not raw_events:
        log(logger, logging.WARNING, "scoring_missing_events")

    if degraded and os.getenv("STRICT"):
        log(logger, logging.ERROR, "scoring_strict_abort", strict=True)
        run_meta.record_step(
            OUT_DIR,
            "scoring",
            "failed",
            trading_day=trading_day,
            degraded=True,
            strict=True,
        )
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

    sentiment_available = sentiment_result is not None
    sentiment_score_value = sentiment_result.score if sentiment_result else 50.0

    market_payload = raw_market.get("market", {})
    btc_payload = raw_market.get("btc", {})

    theme_ai_weights = weights.get("theme_ai") or weights.get("default", {})
    theme_btc_weights = weights.get("theme_btc") or weights.get("default", {})
    theme_m7_weights = weights.get("theme_m7") or weights.get("default", {})

    theme_ai = _score_ai(
        market_payload,
        theme_ai_weights,
        degraded,
        sentiment_score=sentiment_score_value,
        sentiment_fallback=not sentiment_available,
    )

    funding_rate_raw = _coerce_float(btc_payload.get("funding_rate", 0.0))
    btc_sentiment_fallback = funding_rate_raw is None
    funding_rate_for_score = funding_rate_raw if funding_rate_raw is not None else 0.0
    btc_sentiment_value = _scale(funding_rate_for_score, midpoint=0.005, sensitivity=600)
    if degraded:
        btc_sentiment_fallback = True
    theme_btc = _score_btc(
        btc_payload,
        theme_btc_weights,
        degraded,
        sentiment_score=btc_sentiment_value,
        sentiment_fallback=btc_sentiment_fallback,
    )

    theme_m7 = _score_magnificent7(
        market_payload,
        theme_m7_weights,
        degraded,
        sentiment_score=sentiment_score_value,
        sentiment_fallback=not sentiment_available,
    )

    themes = [theme_ai, theme_m7, theme_btc]
    actions = _build_actions(themes, thresholds)

    history_payload = _load_json(SCORE_HISTORY_PATH)
    if not isinstance(history_payload, dict):
        history_payload = {}
    themes_history = history_payload.get("themes")
    if not isinstance(themes_history, dict):
        themes_history = {}
    history_payload["themes"] = themes_history

    for theme in themes:
        entries = themes_history.get(theme.name, [])
        if not isinstance(entries, list):
            entries = []
        prev_entry = None
        for entry in reversed(entries):
            if isinstance(entry, dict) and entry.get("date") != trading_day:
                prev_entry = entry
                break
        delta: float | None = None
        if prev_entry and isinstance(prev_entry.get("total"), (int, float)):
            delta = theme.total - float(prev_entry["total"])
        meta: Dict[str, object] = {
            "previous_total": round(prev_entry["total"], 2) if prev_entry else None,
            "delta": round(delta, 2) if delta is not None else None,
            "weights": {k: float(v) for k, v in theme.weights.items()},
        }
        add_threshold = thresholds.get("action_add")
        if isinstance(add_threshold, (int, float)):
            meta["distance_to_add"] = round(add_threshold - theme.total, 2)
        trim_threshold = thresholds.get("action_trim")
        if isinstance(trim_threshold, (int, float)):
            meta["distance_to_trim"] = round(theme.total - trim_threshold, 2)
        theme.meta = meta

        updated_entries = [entry for entry in entries if isinstance(entry, dict) and entry.get("date") != trading_day]
        updated_entries.append({"date": trading_day, "total": round(theme.total, 2)})
        themes_history[theme.name] = updated_entries[-30:]

    scores_payload = {
        "date": trading_day,
        "themes": [theme.to_dict() for theme in themes],
        "events": raw_events.get("events", []),
        "degraded": degraded,
        "thresholds": thresholds,
        "etl_status": etl_status,
    }
    if config_version is not None:
        scores_payload["config_version"] = config_version
    if config_changed_at:
        scores_payload["config_changed_at"] = config_changed_at
    if sentiment_result:
        scores_payload["sentiment"] = sentiment_result.to_dict()

    SENTIMENT_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SENTIMENT_HISTORY_PATH.open("w", encoding="utf-8") as f_history:
        json.dump(history, f_history, ensure_ascii=False, indent=2)
    _save_json(OUT_DIR / "scores.json", scores_payload)
    _save_json(SCORE_HISTORY_PATH, {"themes": themes_history})

    actions_payload = {
        "date": trading_day,
        "items": actions,
    }
    _save_json(OUT_DIR / "actions.json", actions_payload)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path.write_text(trading_day, encoding="utf-8")

    for theme in themes:
        log(
            logger,
            logging.INFO,
            "theme_score",
            theme=theme.name,
            label=theme.label,
            total=round(theme.total, 2),
            degraded=theme.degraded,
        )
    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    log(
        logger,
        logging.INFO,
        "scoring_complete",
        degraded=degraded,
        themes=len(themes),
        duration_seconds=round(duration, 2),
        config_version=config_version,
    )
    run_meta.record_step(
        OUT_DIR,
        "scoring",
        "completed",
        trading_day=trading_day,
        degraded=degraded,
        duration_seconds=round(duration, 2),
        config_version=config_version,
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
