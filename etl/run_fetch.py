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
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytz
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "out"

ALPHA_VANTAGE_SYMBOLS = {
    "SPX": "SPY",  # S&P 500 ETF proxy
    "NDX": "QQQ",  # Nasdaq 100 ETF proxy
}

SECTOR_PROXIES = {
    "AI": "BOTZ",  # Robotics & AI ETF
    "Defensive": "XLP",  # Consumer staples ETF
}

TE_GUEST_CREDENTIAL = "guest:guest"

REQUEST_TIMEOUT = 15
USER_AGENT = "daily-messenger-bot/0.1"


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


def _request_json(url: str, *, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)
    resp = requests.get(url, params=params, headers=hdrs, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


class _HTMLTableParser(HTMLParser):
    """Extract rows from a simple HTML table."""

    def __init__(self) -> None:
        super().__init__()
        self._capture = False
        self._buffer: List[str] = []
        self._current: List[str] = []
        self.rows: List[List[str]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:  # type: ignore[override]
        if tag == "tr":
            self._current = []
        elif tag in {"td", "th"}:
            self._capture = True
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag in {"td", "th"} and self._capture:
            value = "".join(self._buffer).strip()
            self._current.append(value)
            self._capture = False
        elif tag == "tr" and self._current:
            self.rows.append(self._current)

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._capture:
            self._buffer.append(data)


def _parse_number(value: str) -> Optional[float]:
    text = value.strip()
    if not text or text == "-":
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("() ")
    normalized = text.replace(",", "").replace("\u2212", "-")
    try:
        number = float(normalized)
    except ValueError:
        return None
    return -number if negative else number


def _latest_date(rows: Iterable[List[str]]) -> Optional[List[str]]:
    parsed: List[Tuple[datetime, List[str]]] = []
    for row in rows:
        if not row:
            continue
        first = row[0].strip()
        try:
            parsed_date = datetime.strptime(first, "%d %b %Y")
        except ValueError:
            continue
        parsed.append((parsed_date, row))
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0], reverse=True)
    return parsed[0][1]


def _fetch_farside_latest_flow() -> Tuple[Optional[float], FetchStatus]:
    url = "https://farside.co.uk/wp-json/wp/v2/pages"
    try:
        payload = _request_json(url, params={"slug": "bitcoin-etf-flow-all-data"})
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="btc_etf_flow", ok=False, message=f"Farside 请求失败: {exc}")

    if not payload:
        return None, FetchStatus(name="btc_etf_flow", ok=False, message="Farside 未返回内容")

    content = payload[0].get("content", {}).get("rendered", "")
    parser = _HTMLTableParser()
    parser.feed(content)
    latest = _latest_date(parser.rows)
    if not latest:
        return None, FetchStatus(name="btc_etf_flow", ok=False, message="未能解析 ETF 流入数据")

    total = latest[-1] if latest else None
    amount = _parse_number(total or "")
    if amount is None:
        return None, FetchStatus(name="btc_etf_flow", ok=False, message="ETF 流入字段为空")

    return amount, FetchStatus(name="btc_etf_flow", ok=True, message="ETF 净流入读取成功")


def _fetch_coinbase_spot() -> Tuple[Optional[float], FetchStatus]:
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        payload = _request_json(url)
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="coinbase_spot", ok=False, message=f"Coinbase 请求失败: {exc}")

    try:
        amount = float(payload["data"]["amount"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(name="coinbase_spot", ok=False, message=f"Coinbase 响应解析失败: {exc}")

    return amount, FetchStatus(name="coinbase_spot", ok=True, message="Coinbase 现货价格已获取")


def _fetch_okx_funding() -> Tuple[Optional[float], FetchStatus]:
    url = "https://www.okx.com/api/v5/public/funding-rate"
    try:
        payload = _request_json(url, params={"instId": "BTC-USD-SWAP"})
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="okx_funding", ok=False, message=f"OKX 请求失败: {exc}")

    if payload.get("code") != "0":
        return None, FetchStatus(name="okx_funding", ok=False, message=f"OKX 返回错误: {payload.get('msg')}")

    data = payload.get("data") or []
    if not data:
        return None, FetchStatus(name="okx_funding", ok=False, message="OKX 未返回资金费率")

    try:
        rate = float(data[0]["fundingRate"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(name="okx_funding", ok=False, message=f"资金费率解析失败: {exc}")

    return rate, FetchStatus(name="okx_funding", ok=True, message="OKX 资金费率已获取")


def _fetch_okx_basis(spot_price: float) -> Tuple[Optional[float], FetchStatus]:
    url = "https://www.okx.com/api/v5/market/ticker"
    try:
        payload = _request_json(url, params={"instId": "BTC-USD-SWAP"})
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="okx_basis", ok=False, message=f"OKX ticker 请求失败: {exc}")

    if payload.get("code") != "0":
        return None, FetchStatus(name="okx_basis", ok=False, message=f"OKX 返回错误: {payload.get('msg')}")

    data = payload.get("data") or []
    if not data:
        return None, FetchStatus(name="okx_basis", ok=False, message="OKX 未返回永续价格")

    try:
        last_price = float(data[0]["last"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(name="okx_basis", ok=False, message=f"永续价格解析失败: {exc}")

    if spot_price <= 0:
        return None, FetchStatus(name="okx_basis", ok=False, message="现货价格无效，无法计算基差")

    basis = (last_price - spot_price) / spot_price
    return basis, FetchStatus(name="okx_basis", ok=True, message="已计算 OKX 永续基差")


def _fetch_alpha_series(symbol: str, api_key: str) -> Dict[str, Any]:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
    }
    payload = _request_json(url, params=params)
    key = next((k for k in payload if "Time Series" in k), None)
    if not key:
        message = payload.get("Information") or payload.get("Note") or "Alpha Vantage 未返回时间序列"
        raise RuntimeError(message)
    return payload[key]


def _extract_close_change(series: Dict[str, Dict[str, str]]) -> Tuple[str, float, float]:
    dates = sorted(series.keys(), reverse=True)
    if len(dates) < 2:
        raise RuntimeError("时间序列不足以计算涨跌幅")
    latest, prev = dates[0], dates[1]
    close = float(series[latest]["4. close"])
    prev_close = float(series[prev]["4. close"])
    change_pct = (close - prev_close) / prev_close * 100
    return latest, close, change_pct


def _extract_performance(series: Dict[str, Dict[str, str]]) -> float:
    _, close, change_pct = _extract_close_change(series)
    return 1 + change_pct / 100


def _fetch_market_snapshot_real(api_keys: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], FetchStatus]:
    api_key = api_keys.get("alpha_vantage")
    if not api_key:
        return None, FetchStatus(name="market", ok=False, message="缺少 Alpha Vantage API Key")

    try:
        index_data = {
            symbol: _fetch_alpha_series(proxy, api_key) for symbol, proxy in ALPHA_VANTAGE_SYMBOLS.items()
        }
        sector_data = {
            name: _fetch_alpha_series(proxy, api_key) for name, proxy in SECTOR_PROXIES.items()
        }
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="market", ok=False, message=f"Alpha Vantage 请求失败: {exc}")

    latest_date = None
    indices: List[Dict[str, Any]] = []
    for symbol, series in index_data.items():
        day, close, change_pct = _extract_close_change(series)
        latest_date = latest_date or day
        indices.append({"symbol": symbol, "close": round(close, 2), "change_pct": round(change_pct, 2)})

    sectors: List[Dict[str, Any]] = []
    for name, series in sector_data.items():
        perf = _extract_performance(series)
        sectors.append({"name": name, "performance": round(perf, 3)})

    market = {
        "date": latest_date,
        "indices": indices,
        "sectors": sectors,
    }
    return market, FetchStatus(name="market", ok=True, message="Alpha Vantage 行情已获取")


def _fetch_events_real(trading_day: str, api_keys: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    credential = api_keys.get("trading_economics", TE_GUEST_CREDENTIAL)
    params = {"c": credential, "format": "json"}
    try:
        payload = _request_json("https://api.tradingeconomics.com/calendar", params=params)
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(name="events", ok=False, message=f"Trading Economics 请求失败: {exc}")

    base_date = datetime.strptime(trading_day, "%Y-%m-%d").date()
    window_end = base_date + timedelta(days=5)
    events: List[Dict[str, Any]] = []
    for entry in payload:
        date_str = entry.get("Date")
        event_name = entry.get("Event")
        if not date_str or not event_name:
            continue
        try:
            event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        if not (base_date <= event_date <= window_end):
            continue
        importance = str(entry.get("Importance", "medium")).lower()
        if importance not in {"low", "medium", "high"}:
            importance = "medium"
        events.append(
            {
                "title": event_name,
                "date": event_date.strftime("%Y-%m-%d"),
                "impact": importance,
                "country": entry.get("Country"),
            }
        )
        if len(events) >= 8:
            break

    if not events:
        return [], FetchStatus(name="events", ok=False, message="Trading Economics 未返回可用事件")

    return events, FetchStatus(name="events", ok=True, message="Trading Economics 事件日历已获取")


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
    overall_ok = True

    market_data, status = _fetch_market_snapshot_real(api_keys)
    statuses.append(status)
    if not status.ok:
        overall_ok = False
        fallback, fallback_status = _simulate_market_snapshot(trading_day)
        market_data = fallback
        statuses.append(fallback_status)

    spot_price, spot_status = _fetch_coinbase_spot()
    statuses.append(spot_status)

    funding_rate, funding_status = _fetch_okx_funding()
    statuses.append(funding_status)

    basis = None
    basis_status: Optional[FetchStatus] = None
    if spot_price is not None:
        basis, basis_status = _fetch_okx_basis(spot_price)
        statuses.append(basis_status)
    else:
        statuses.append(FetchStatus(name="okx_basis", ok=False, message="缺少现货价格，无法计算基差"))

    etf_flow, flow_status = _fetch_farside_latest_flow()
    statuses.append(flow_status)

    btc_data: Dict[str, Any]
    if spot_price is not None and funding_rate is not None and basis is not None and etf_flow is not None:
        btc_data = {
            "date": trading_day,
            "spot_price_usd": round(spot_price, 2),
            "perpetual_price_usd": round(spot_price * (1 + basis), 2) if basis is not None else None,
            "etf_net_inflow_musd": round(etf_flow, 2),
            "funding_rate": round(funding_rate, 6),
            "futures_basis": round(basis, 6),
        }
        statuses.append(FetchStatus(name="btc", ok=True, message="BTC 主题数据已获取"))
    else:
        overall_ok = False
        btc_data, sim_status = _simulate_btc_theme(trading_day)
        statuses.append(sim_status)

    events, events_status = _fetch_events_real(trading_day, api_keys)
    statuses.append(events_status)
    if not events_status.ok:
        fallback_events, fallback_status = _simulate_events(trading_day)
        events = fallback_events
        statuses.append(fallback_status)

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
        "ok": overall_ok,
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
