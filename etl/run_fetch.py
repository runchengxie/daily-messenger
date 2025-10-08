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
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import pytz
import requests

if __package__:
    from .fetchers import aaii_sentiment, cboe_putcall
else:  # pragma: no cover - runtime convenience for direct script execution
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from etl.fetchers import aaii_sentiment, cboe_putcall

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

HK_MARKET_SYMBOLS = [
    {"symbol": "HSI", "label": "HSI"},
]

FMP_THEME_SYMBOLS = {
    "ai": ["NVDA", "MSFT", "GOOGL", "AMD"],
    "magnificent7": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
}

MAX_EVENT_ITEMS = 12

TE_GUEST_CREDENTIAL = "guest:guest"

REQUEST_TIMEOUT = 15
USER_AGENT = "daily-messenger-bot/0.1"

DEFAULT_AI_FEEDS = [
    "https://openai.com/news/rss.xml",
    "https://deepmind.com/blog/feed/basic",
]

DEFAULT_ARXIV_PARAMS = {
    "search_query": "cat:cs.LG OR cat:cs.AI",
    "max_results": 8,
    "sort_by": "submittedDate",
    "sort_order": "descending",
}

DEFAULT_ARXIV_THROTTLE = 3.0


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


CANONICAL_API_KEYS = (
    "alpha_vantage",
    "twelve_data",
    "financial_modeling_prep",
    "trading_economics",
    "finnhub",
)


def _load_api_keys() -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    # 1) Optional JSON file referenced via API_KEYS_PATH
    path_hint = os.getenv("API_KEYS_PATH")
    if path_hint:
        candidate_paths = []
        expanded = Path(path_hint).expanduser()
        candidate_paths.append(expanded)
        if not expanded.is_absolute():
            candidate_paths.append((BASE_DIR / expanded).resolve())
        for candidate in candidate_paths:
            try:
                with open(candidate, "r", encoding="utf-8") as fh:
                    payload = json.load(fh)
            except FileNotFoundError:
                continue
            except Exception as exc:  # noqa: BLE001
                print(f"API_KEYS_PATH 读取失败: {exc}", file=sys.stderr)
                continue
            if isinstance(payload, dict):
                data.update(payload)
                break

    # 2) Inline JSON payload
    raw = os.getenv("API_KEYS")
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            print(f"API_KEYS 无法解析为 JSON: {exc}", file=sys.stderr)
        else:
            if isinstance(payload, dict):
                data.update(payload)

    # 3) Loose environment variables (case insensitive)
    env = os.environ
    for key in CANONICAL_API_KEYS:
        if key in data:
            continue
        direct = env.get(key)
        if not direct:
            direct = env.get(key.upper())
        if direct:
            data[key] = direct

    # 4) Trading Economics username/password split entries
    if "trading_economics" not in data:
        te_user = env.get("TRADING_ECONOMICS_USER") or env.get("trading_economics_user")
        te_password = env.get("TRADING_ECONOMICS_PASSWORD") or env.get("trading_economics_password")
        if te_user and te_password:
            data["trading_economics"] = f"{te_user}:{te_password}"

    return data


def _resolve_ai_feeds(config: Dict[str, Any]) -> List[str]:
    feeds = config.get("ai_feeds")
    if isinstance(feeds, list):
        normalized = [str(item).strip() for item in feeds if str(item).strip()]
        return normalized
    return list(DEFAULT_AI_FEEDS)


def _resolve_arxiv_config(config: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
    section = config.get("arxiv")
    params = dict(DEFAULT_ARXIV_PARAMS)
    throttle = DEFAULT_ARXIV_THROTTLE
    if isinstance(section, dict):
        search_query = section.get("search_query")
        if isinstance(search_query, str) and search_query.strip():
            params["search_query"] = search_query.strip()
        max_results = section.get("max_results")
        if isinstance(max_results, int) and max_results > 0:
            params["max_results"] = max_results
        sort_by = section.get("sort_by")
        if isinstance(sort_by, str) and sort_by.strip():
            params["sort_by"] = sort_by.strip()
        sort_order = section.get("sort_order")
        if isinstance(sort_order, str) and sort_order.strip():
            params["sort_order"] = sort_order.strip()
        throttle_val = section.get("throttle_seconds")
        if isinstance(throttle_val, (int, float)) and throttle_val >= 0:
            throttle = float(throttle_val)
    # arXiv expects camelCase keys
    request_params = {
        "search_query": params["search_query"],
        "max_results": params["max_results"],
        "sortBy": params["sort_by"],
        "sortOrder": params["sort_order"],
    }
    return request_params, throttle


def _load_configuration() -> Tuple[Dict[str, Any], List[str], Dict[str, Any], float]:
    api_keys = _load_api_keys()
    ai_feeds = _resolve_ai_feeds(api_keys)
    arxiv_params, arxiv_throttle = _resolve_arxiv_config(api_keys)
    return api_keys, ai_feeds, arxiv_params, arxiv_throttle


API_KEYS_CACHE, AI_NEWS_FEEDS, ARXIV_QUERY_PARAMS, ARXIV_THROTTLE = _load_configuration()


def _request_json(url: str, *, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)
    resp = requests.get(url, params=params, headers=hdrs, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rss_text(node: ET.Element, *paths: str) -> str:
    for path in paths:
        text = node.findtext(path)
        if text:
            return text.strip()
    return ""


def _normalize_rss_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        parsed = parsedate_to_datetime(text)
    except Exception:  # noqa: BLE001
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return parsed.date().isoformat()


def _fetch_ai_rss_events(feeds: List[str]) -> Tuple[List[Dict[str, Any]], List[FetchStatus]]:
    events: List[Dict[str, Any]] = []
    statuses: List[FetchStatus] = []
    for idx, url in enumerate(feeds, start=1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            statuses.append(
                FetchStatus(
                    name=f"ai_rss_{idx}",
                    ok=False,
                    message=f"RSS 请求失败: {url} ({exc})",
                )
            )
            continue
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as exc:
            statuses.append(
                FetchStatus(
                    name=f"ai_rss_{idx}",
                    ok=False,
                    message=f"RSS 解析失败: {url} ({exc})",
                )
            )
            continue

        items = root.findall(".//item") or root.findall(".//{*}entry")
        feed_events: List[Dict[str, Any]] = []
        for item in items[:5]:
            title = _rss_text(item, "title", "{*}title") or "更新"
            date_text = _rss_text(
                item,
                "pubDate",
                "{*}updated",
                "{*}published",
                "{*}lastBuildDate",
            )
            normalized_date = _normalize_rss_date(date_text)
            if not normalized_date:
                normalized_date = datetime.utcnow().strftime("%Y-%m-%d")
            feed_events.append(
                {
                    "title": title,
                    "date": normalized_date,
                    "impact": "medium",
                    "source": url,
                }
            )
        events.extend(feed_events)
        statuses.append(
            FetchStatus(
                name=f"ai_rss_{idx}",
                ok=True,
                message=f"RSS 获取成功: {url}（{len(feed_events)} 条）",
            )
        )
    return events, statuses


def _fetch_arxiv_events(params: Dict[str, Any], throttle: float) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    url = "https://export.arxiv.org/api/query"
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(name="arxiv", ok=False, message=f"arXiv 请求失败: {exc}")

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        return [], FetchStatus(name="arxiv", ok=False, message=f"arXiv 响应解析失败: {exc}")

    events: List[Dict[str, Any]] = []
    for entry in root.findall(".//{*}entry"):
        title = (_rss_text(entry, "{*}title") or "").replace("\n", " ").strip()
        if not title:
            title = "arXiv 更新"
        date_text = _rss_text(entry, "{*}updated", "{*}published")
        normalized_date = None
        if date_text:
            try:
                normalized_date = datetime.fromisoformat(date_text.replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                normalized_date = _normalize_rss_date(date_text)
        if not normalized_date:
            normalized_date = datetime.utcnow().strftime("%Y-%m-%d")
        events.append(
            {
                "title": f"arXiv: {title}",
                "date": normalized_date,
                "impact": "low",
                "source": "arxiv",
            }
        )

    if throttle > 0:
        time.sleep(throttle)

    return events, FetchStatus(name="arxiv", ok=True, message=f"arXiv 返回 {len(events)} 篇文章")


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


def _fetch_twelvedata_series(symbol: str, api_key: str) -> List[Dict[str, Any]]:
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 2,
        "apikey": api_key,
    }
    payload = _request_json(url, params=params)
    if payload.get("status") == "error":
        raise RuntimeError(payload.get("message", "Twelve Data 返回错误"))
    values = payload.get("values")
    if not values or len(values) < 2:
        raise RuntimeError("Twelve Data 未返回足够的时间序列")
    return values


def _extract_twelve_close_change(values: List[Dict[str, Any]]) -> Tuple[str, float, float]:
    latest, prev = values[0], values[1]
    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(prev.get("close"))
    if close is None or prev_close is None or prev_close == 0:
        raise RuntimeError("无法计算 Twelve Data 涨跌幅")
    change_pct = (close - prev_close) / prev_close * 100
    return latest.get("datetime", ""), close, change_pct


def _fetch_hk_market_snapshot(api_keys: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    api_key = api_keys.get("twelve_data")
    if not api_key:
        return [], FetchStatus(name="hongkong_market", ok=False, message="缺少 Twelve Data API Key")

    rows: List[Dict[str, Any]] = []
    for item in HK_MARKET_SYMBOLS:
        symbol = item["symbol"]
        label = item.get("label", symbol)
        try:
            series = _fetch_twelvedata_series(symbol, api_key)
            _, close, change_pct = _extract_twelve_close_change(series)
        except Exception as exc:  # noqa: BLE001
            return [], FetchStatus(name=f"hongkong_{label}", ok=False, message=f"Twelve Data 请求失败: {exc}")

        rows.append({"symbol": label, "close": round(close, 2), "change_pct": round(change_pct, 2)})

    if not rows:
        return [], FetchStatus(name="hongkong_market", ok=False, message="未获取到港股行情")

    return rows, FetchStatus(name="hongkong_market", ok=True, message="港股行情已获取")


def _fetch_fmp_quotes(symbols: List[str], api_key: str) -> Dict[str, Dict[str, Any]]:
    joined = ",".join(sorted(set(symbols)))
    url = f"https://financialmodelingprep.com/api/v3/quote/{joined}"
    payload = _request_json(url, params={"apikey": api_key})
    if not isinstance(payload, list):
        raise RuntimeError("FMP 响应格式异常")
    results: Dict[str, Dict[str, Any]] = {}
    for item in payload:
        symbol = item.get("symbol")
        if symbol:
            results[symbol] = item
    return results


def _mean(values: List[Optional[float]]) -> Optional[float]:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _fetch_theme_metrics_from_fmp(api_keys: Dict[str, Any]) -> Tuple[Dict[str, Any], FetchStatus]:
    api_key = api_keys.get("financial_modeling_prep")
    if not api_key:
        return {}, FetchStatus(name="fmp_theme", ok=False, message="缺少 FMP API Key")

    all_symbols: List[str] = []
    for symbols in FMP_THEME_SYMBOLS.values():
        all_symbols.extend(symbols)

    try:
        quotes = _fetch_fmp_quotes(all_symbols, api_key)
    except Exception as exc:  # noqa: BLE001
        return {}, FetchStatus(name="fmp_theme", ok=False, message=f"FMP 请求失败: {exc}")

    themes: Dict[str, Any] = {}
    for theme, symbols in FMP_THEME_SYMBOLS.items():
        entries = [quotes[s] for s in symbols if s in quotes]
        if not entries:
            continue
        change_avg = _mean([_safe_float(e.get("changesPercentage")) for e in entries])
        pe_avg = _mean([_safe_float(e.get("pe")) for e in entries])
        ps_avg = _mean(
            [
                _safe_float(e.get("priceToSalesRatioTTM"))
                if e.get("priceToSalesRatioTTM") is not None
                else _safe_float(e.get("priceToSalesRatio"))
                for e in entries
            ]
        )
        market_cap_total = sum(_safe_float(e.get("marketCap")) or 0.0 for e in entries)
        themes[theme] = {
            "change_pct": round(change_avg, 2) if change_avg is not None else None,
            "avg_pe": round(pe_avg, 2) if pe_avg is not None else None,
            "avg_ps": round(ps_avg, 2) if ps_avg is not None else None,
            "market_cap": round(market_cap_total, 2) if market_cap_total else None,
        }

    if not themes:
        return {}, FetchStatus(name="fmp_theme", ok=False, message="FMP 未返回主题数据")

    return themes, FetchStatus(name="fmp_theme", ok=True, message="FMP 主题估值已获取")


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


def _fetch_finnhub_earnings(trading_day: str, api_keys: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    api_key = api_keys.get("finnhub")
    if not api_key:
        return [], FetchStatus(name="finnhub_earnings", ok=False, message="缺少 Finnhub API Key")

    start = datetime.strptime(trading_day, "%Y-%m-%d").date()
    end = start + timedelta(days=5)
    params = {
        "from": trading_day,
        "to": end.strftime("%Y-%m-%d"),
        "token": api_key,
    }
    try:
        payload = _request_json("https://finnhub.io/api/v1/calendar/earnings", params=params)
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(name="finnhub_earnings", ok=False, message=f"Finnhub 请求失败: {exc}")

    items = payload.get("earningsCalendar") or []
    events: List[Dict[str, Any]] = []
    for item in items:
        date_str = item.get("date")
        symbol = item.get("symbol")
        if not date_str or not symbol:
            continue
        try:
            event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if not (start <= event_date <= end):
            continue
        eps_est = _safe_float(item.get("epsEstimate"))
        eps_actual = _safe_float(item.get("epsActual"))
        surprise_text = ""
        if eps_actual is not None and eps_est is not None:
            surprise = eps_actual - eps_est
            surprise_text = f" EPS {eps_actual:.2f}/{eps_est:.2f} ({surprise:+.2f})"
        session = str(item.get("time", "")).upper()
        if session in {"AMC", "POSTMARKET"}:
            session_label = "盘后"
        elif session in {"BMO", "PREMARKET"}:
            session_label = "盘前"
        else:
            session_label = ""
        title = f"{symbol} 财报"
        if session_label:
            title += f"（{session_label}）"
        if surprise_text:
            title += surprise_text
        market_cap = _safe_float(item.get("marketCapitalization"))
        impact = "high" if market_cap and market_cap >= 200_000 else "medium"
        events.append(
            {
                "title": title,
                "date": event_date.strftime("%Y-%m-%d"),
                "impact": impact,
                "country": "US",
            }
        )

    if not events:
        return [], FetchStatus(name="finnhub_earnings", ok=False, message="Finnhub 未返回财报事件")

    events.sort(key=lambda item: item["date"])
    return events, FetchStatus(name="finnhub_earnings", ok=True, message="Finnhub 财报日历已获取")


def _simulate_market_snapshot(trading_day: str) -> Tuple[Dict[str, Any], FetchStatus]:
    # Deterministic pseudo data keyed by date to keep examples stable.
    seed = sum(ord(ch) for ch in trading_day)
    index_level = 4800 + seed % 50
    ai_sector_perf = 1.2 + (seed % 7) * 0.1
    defensive_sector_perf = 0.8 + (seed % 5) * 0.05
    hk_change = ((seed % 9) - 4) * 0.2
    mag7_change = ((seed % 11) - 5) * 0.3

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
        "hk_indices": [
            {"symbol": "HSI", "close": 18000 + seed % 200, "change_pct": round(hk_change, 2)},
        ],
        "themes": {
            "ai": {
                "performance": round(ai_sector_perf, 2),
                "change_pct": round((seed % 5 - 2) * 0.5, 2),
                "avg_pe": 32.5,
                "avg_ps": 7.5,
            },
            "magnificent7": {
                "change_pct": round(mag7_change, 2),
                "avg_pe": 30.0,
                "avg_ps": 6.2,
                "market_cap": 12_000_000_000_000,
            },
        },
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

    api_keys, ai_feeds, arxiv_params, arxiv_throttle = _load_configuration()
    global API_KEYS_CACHE, AI_NEWS_FEEDS, ARXIV_QUERY_PARAMS, ARXIV_THROTTLE
    API_KEYS_CACHE = api_keys
    AI_NEWS_FEEDS = ai_feeds
    ARXIV_QUERY_PARAMS = arxiv_params
    ARXIV_THROTTLE = arxiv_throttle

    if not api_keys:
        print("未提供 API 密钥，将使用示例数据。")

    statuses: List[FetchStatus] = []
    overall_ok = True

    raw_market_path = OUT_DIR / "raw_market.json"
    previous_sentiment: Dict[str, Any] = {}
    if raw_market_path.exists():
        try:
            with raw_market_path.open("r", encoding="utf-8") as f_prev:
                previous_payload = json.load(f_prev)
        except json.JSONDecodeError:
            previous_payload = {}
        if isinstance(previous_payload, dict):
            prev_sent = previous_payload.get("sentiment")
            if isinstance(prev_sent, dict):
                previous_sentiment = prev_sent

    market_data, status = _fetch_market_snapshot_real(api_keys)
    statuses.append(status)
    if not status.ok:
        overall_ok = False
        fallback, fallback_status = _simulate_market_snapshot(trading_day)
        market_data = fallback
        statuses.append(fallback_status)
    else:
        market_data = market_data or {}

    hk_status: Optional[FetchStatus] = None
    if api_keys.get("twelve_data"):
        hk_rows, hk_status = _fetch_hk_market_snapshot(api_keys)
        statuses.append(hk_status)
        if hk_status.ok and market_data is not None:
            market_data.setdefault("hk_indices", hk_rows)
    elif api_keys:
        statuses.append(FetchStatus(name="hongkong_market", ok=False, message="缺少 Twelve Data API Key"))

    theme_metrics: Dict[str, Any] = {}
    if market_data:
        sectors = market_data.get("sectors", []) if isinstance(market_data, dict) else []
        ai_perf = next((s.get("performance") for s in sectors if s.get("name") == "AI"), None)
        if ai_perf is not None:
            theme_metrics.setdefault("ai", {})["performance"] = ai_perf

    if api_keys.get("financial_modeling_prep"):
        themes, theme_status = _fetch_theme_metrics_from_fmp(api_keys)
        statuses.append(theme_status)
        if theme_status.ok:
            for name, metrics in themes.items():
                theme_metrics.setdefault(name, {}).update(metrics)
    elif api_keys:
        statuses.append(FetchStatus(name="fmp_theme", ok=False, message="缺少 FMP API Key"))

    if market_data is not None and theme_metrics:
        market_data.setdefault("themes", {}).update(theme_metrics)

    sentiment_data: Dict[str, Any] = {}

    put_call_payload, put_call_status = cboe_putcall.fetch()
    statuses.append(put_call_status)
    if getattr(put_call_status, "ok", False) and put_call_payload:
        sentiment_data.update(put_call_payload)
    else:
        overall_ok = False
        previous_put_call = previous_sentiment.get("put_call") if isinstance(previous_sentiment, dict) else None
        if isinstance(previous_put_call, dict):
            sentiment_data["put_call"] = previous_put_call
            statuses.append(FetchStatus(name="cboe_put_call_fallback", ok=True, message="使用上一期 Put/Call 数据"))

    aaii_payload, aaii_status = aaii_sentiment.fetch()
    statuses.append(aaii_status)
    if getattr(aaii_status, "ok", False) and aaii_payload:
        sentiment_data.update(aaii_payload)
    else:
        overall_ok = False
        previous_aaii = previous_sentiment.get("aaii") if isinstance(previous_sentiment, dict) else None
        if isinstance(previous_aaii, dict):
            sentiment_data["aaii"] = previous_aaii
            statuses.append(FetchStatus(name="aaii_sentiment_fallback", ok=True, message="使用上一期 AAII 数据"))

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
    else:
        finnhub_events: List[Dict[str, Any]] = []
        if api_keys.get("finnhub"):
            finnhub_events, finnhub_status = _fetch_finnhub_earnings(trading_day, api_keys)
            statuses.append(finnhub_status)
            if finnhub_status.ok:
                events.extend(finnhub_events)
        elif api_keys:
            statuses.append(FetchStatus(name="finnhub_earnings", ok=False, message="缺少 Finnhub API Key"))

    if ai_feeds:
        ai_events, feed_statuses = _fetch_ai_rss_events(ai_feeds)
        statuses.extend(feed_statuses)
        if ai_events:
            events.extend(ai_events)

    arxiv_events, arxiv_status = _fetch_arxiv_events(arxiv_params, arxiv_throttle)
    statuses.append(arxiv_status)
    if arxiv_status.ok and arxiv_events:
        events.extend(arxiv_events)

    if events:
        events.sort(key=lambda item: (item.get("date"), item.get("impact", "")))
        events = events[:MAX_EVENT_ITEMS]

    raw_market_path = OUT_DIR / "raw_market.json"
    raw_events_path = OUT_DIR / "raw_events.json"
    status_path = OUT_DIR / "etl_status.json"

    market_payload: Dict[str, Any] = {
        "market": market_data,
        "btc": btc_data,
        "sentiment": sentiment_data,
    }

    with raw_market_path.open("w", encoding="utf-8") as f:
        json.dump(market_payload, f, ensure_ascii=False, indent=2)

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
