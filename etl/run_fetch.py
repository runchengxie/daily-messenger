#!/usr/bin/env python3
"""Daily ETL entrypoint.

This script fetches (or simulates) the minimum viable data required for
subsequent scoring and report generation. It intentionally keeps the data
model small so it can be swapped with real data sources later.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytz

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "out"


@dataclass
class FetchStatus:
    name: str
    ok: bool
    message: str = ""


def _ensure_out_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def _current_trading_day() -> str:
    tz = pytz.timezone("America/Los_Angeles")
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%d")


def _load_api_keys() -> Dict[str, Any]:
    raw = os.getenv("API_KEYS")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        print("API_KEYS 无法解析为 JSON: %s" % exc, file=sys.stderr)
        return {}


def _simulate_market_snapshot(trading_day: str) -> Tuple[Dict[str, Any], FetchStatus]:
    # Deterministic pseudo data keyed by date to keep examples stable.
    seed = sum(ord(ch) for ch in trading_day)
    index_level = 4800 + seed % 50
    ai_sector_perf = 1.2 + (seed % 7) * 0.1
    defensive_sector_perf = 0.8 + (seed % 5) * 0.05

    market = {
        "date": trading_day,
        "indices": [
            {"symbol": "SPX", "close": round(index_level, 2), "change_pct": round((seed % 5 - 2) * 0.3, 2)},
            {"symbol": "NDX", "close": round(index_level * 1.2, 2), "change_pct": round((seed % 3 - 1) * 0.4, 2)},
        ],
        "sectors": [
            {"name": "AI", "performance": round(ai_sector_perf, 2)},
            {"name": "Defensive", "performance": round(defensive_sector_perf, 2)},
        ],
    }
    status = FetchStatus(name="market", ok=True, message="示例行情生成完毕")
    return market, status


def _simulate_btc_theme(trading_day: str) -> Tuple[Dict[str, Any], FetchStatus]:
    seed = (len(trading_day) * 37) % 11
    net_inflow = (seed - 5) * 12.5
    funding_rate = 0.01 + seed * 0.001
    basis = 0.02 - seed * 0.0015

    btc = {
        "date": trading_day,
        "etf_net_inflow_musd": round(net_inflow, 2),
        "funding_rate": round(funding_rate, 4),
        "futures_basis": round(basis, 4),
    }
    status = FetchStatus(name="btc", ok=True, message="BTC 主题示例数据已生成")
    return btc, status


def _simulate_events(trading_day: str) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    tz = pytz.timezone("America/Los_Angeles")
    today = datetime.now(tz)
    events = [
        {
            "title": "FOMC 会议纪要发布",
            "date": trading_day,
            "impact": "high",
        },
        {
            "title": "大型科技财报",
            "date": (today.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2)).strftime("%Y-%m-%d"),
            "impact": "medium",
        },
    ]
    status = FetchStatus(name="events", ok=True, message="事件日历已生成")
    return events, status


def run() -> int:
    _ensure_out_dir()
    trading_day = _current_trading_day()
    print(f"开始抓取 {trading_day} 的数据……")

    api_keys = _load_api_keys()
    if not api_keys:
        print("未提供 API 密钥，将使用示例数据。")

    statuses: List[FetchStatus] = []
    market_data, status = _simulate_market_snapshot(trading_day)
    statuses.append(status)

    btc_data, status = _simulate_btc_theme(trading_day)
    statuses.append(status)

    events, status = _simulate_events(trading_day)
    statuses.append(status)

    raw_market_path = OUT_DIR / "raw_market.json"
    raw_events_path = OUT_DIR / "raw_events.json"
    status_path = OUT_DIR / "etl_status.json"

    with raw_market_path.open("w", encoding="utf-8") as f:
        json.dump({"market": market_data, "btc": btc_data}, f, ensure_ascii=False, indent=2)

    with raw_events_path.open("w", encoding="utf-8") as f:
        json.dump({"events": events}, f, ensure_ascii=False, indent=2)

    status_payload = {
        "date": trading_day,
        "sources": [asdict(s) for s in statuses],
        "ok": all(s.ok for s in statuses),
    }
    with status_path.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, ensure_ascii=False, indent=2)

    for entry in statuses:
        prefix = "✅" if entry.ok else "⚠️"
        print(f"{prefix} {entry.name}: {entry.message}")

    if not status_payload["ok"]:
        print("部分数据抓取失败，后续流程将触发降级模式。", file=sys.stderr)

    print("原始数据已输出到 out/ 目录。")
    return 0


if __name__ == "__main__":
    sys.exit(run())
