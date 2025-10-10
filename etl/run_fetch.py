#!/usr/bin/env python3
"""Daily ETL entrypoint.

This script fetches (or simulates) the minimum viable data required for
subsequent scoring and report generation. It intentionally keeps the data
model small so it can be swapped with real data sources later.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote
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
STATE_DIR = BASE_DIR / "state"

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

HK_PROXY_SYMBOLS = [
    "2800.HK",
    "2828.HK",
]

FMP_THEME_SYMBOLS = {
    "ai": ["NVDA", "MSFT", "GOOGL", "AMD"],
    "magnificent7": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
}

MAX_EVENT_ITEMS = 12

TE_GUEST_CREDENTIAL = "guest:guest"

REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (compatible; daily-messenger/0.1)"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

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


@dataclass
class _QuoteSnapshot:
    day: str
    close: float
    change_pct: float
    source: str


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
    "sosovalue",
    "coinglass",
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


def _coerce_api_key(value: Any) -> Optional[str]:
    if isinstance(value, str):
        token = value.strip()
        return token or None
    if isinstance(value, dict):
        for field in ("api_key", "key", "token", "secret"):
            candidate = value.get(field)
            if isinstance(candidate, str):
                token = candidate.strip()
                if token:
                    return token
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


SOSOVALUE_INFLOW_URL = "https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart"
COINGLASS_ETF_ENDPOINTS = (
    "https://open-api-v4.coinglass.com/api/bitcoin/etf/flow-history",
    "https://open-api-v1.coinglass.com/api/bitcoin/etf/flow-history",
)


def _fetch_sosovalue_latest_flow(api_key: str) -> Tuple[Optional[float], FetchStatus]:
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-soso-api-key": api_key,
    }
    body = {"type": "us-btc-spot"}
    try:
        response = requests.post(
            SOSOVALUE_INFLOW_URL,
            json=body,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(name="btc_etf_flow_sosovalue", ok=False, message=f"SoSoValue 请求失败: {exc}")

    code = payload.get("code")
    if code not in (0, "0", 200, "200", None):
        message = payload.get("msg") or payload.get("message") or f"code={code}"
        return None, FetchStatus(name="btc_etf_flow_sosovalue", ok=False, message=f"SoSoValue 返回错误: {message}")

    data = payload.get("data")
    records: List[Dict[str, Any]] = []
    if isinstance(data, list):
        records = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        for key in ("result", "list", "items", "data", "rows"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                records = [item for item in candidate if isinstance(item, dict)]
                break
        if not records and data:
            records = [data]

    latest_amount: Optional[float] = None
    latest_day = ""
    latest_ts: Optional[datetime] = None
    for item in records:
        day_value = item.get("date") or item.get("day")
        amount_value = (
            item.get("totalNetInflow")
            or item.get("netInflow")
            or item.get("netflow")
            or item.get("netFlow")
        )
        amount = _safe_float(amount_value)
        if not day_value or amount is None:
            continue
        day_text = str(day_value)[:10]
        try:
            parsed_day = datetime.strptime(day_text, "%Y-%m-%d")
        except ValueError:
            continue
        if latest_ts is None or parsed_day > latest_ts:
            latest_ts = parsed_day
            latest_day = parsed_day.strftime("%Y-%m-%d")
            latest_amount = amount

    if latest_amount is None or not latest_day:
        return None, FetchStatus(name="btc_etf_flow_sosovalue", ok=False, message="SoSoValue 响应缺少有效数据")

    net_musd = latest_amount / 1_000_000.0
    return net_musd, FetchStatus(name="btc_etf_flow_sosovalue", ok=True, message=f"SoSoValue ETF 净流入已获取（{latest_day}）")


def _fetch_coinglass_latest_flow(api_key: str) -> Tuple[Optional[float], FetchStatus]:
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json",
        "coinglassSecret": api_key,
    }
    params = {"page": 1, "size": 10}
    errors: List[str] = []
    for url in COINGLASS_ETF_ENDPOINTS:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
            continue

        code = payload.get("code")
        if code not in (0, "0", 200, "200", None):
            message = payload.get("msg") or payload.get("message") or f"code={code}"
            errors.append(f"{url}: {message}")
            continue

        data = payload.get("data")
        records: List[Dict[str, Any]] = []
        if isinstance(data, list):
            records = [item for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            for key in ("list", "rows", "result", "items", "data"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    records = [item for item in candidate if isinstance(item, dict)]
                    break
            if not records and data:
                records = [data]

        latest_amount: Optional[float] = None
        latest_day = ""
        latest_ts: Optional[datetime] = None
        for item in records:
            day_value = item.get("date") or item.get("day")
            amount_value = (
                item.get("netFlow")
                or item.get("netflow")
                or item.get("net_inflow")
                or item.get("netInflow")
                or item.get("totalNetInflow")
                or item.get("totalNetFlow")
            )
            amount = _safe_float(amount_value)
            if not day_value or amount is None:
                continue
            day_text = str(day_value)[:10]
            try:
                parsed_day = datetime.strptime(day_text, "%Y-%m-%d")
            except ValueError:
                continue
            if latest_ts is None or parsed_day > latest_ts:
                latest_ts = parsed_day
                latest_day = parsed_day.strftime("%Y-%m-%d")
                latest_amount = amount

        if latest_amount is None or not latest_day:
            errors.append(f"{url}: 缺少有效数据")
            continue

        net_amount = latest_amount / 1_000_000.0 if abs(latest_amount) > 100000 else latest_amount
        return net_amount, FetchStatus(name="btc_etf_flow_coinglass", ok=True, message=f"CoinGlass ETF 净流入已获取（{latest_day}）")

    detail = "; ".join(errors) if errors else "未知原因"
    return None, FetchStatus(name="btc_etf_flow_coinglass", ok=False, message=f"CoinGlass 请求失败: {detail}")


FARSIDE_PAGE_URL = "https://farside.co.uk/bitcoin-etf-flow-all-data/"


def _fetch_farside_flow_from_html(session: requests.Session) -> float:
    response = session.get(FARSIDE_PAGE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    parser = _HTMLTableParser()
    parser.feed(response.text)
    latest = _latest_date(parser.rows)
    if not latest:
        raise RuntimeError("未能解析 ETF 流入数据")
    total = latest[-1] if latest else None
    amount = _parse_number(total or "")
    if amount is None:
        raise RuntimeError("ETF 流入字段为空")
    return amount


def _fetch_farside_flow_from_api(session: requests.Session) -> float:
    response = session.get(
        "https://farside.co.uk/wp-json/wp/v2/pages",
        params={"slug": "bitcoin-etf-flow-all-data"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        raise RuntimeError("Farside 未返回内容")
    content = payload[0].get("content", {}).get("rendered", "")
    parser = _HTMLTableParser()
    parser.feed(content)
    latest = _latest_date(parser.rows)
    if not latest:
        raise RuntimeError("未能解析 ETF 流入数据")
    amount = _parse_number((latest[-1] if latest else "") or "")
    if amount is None:
        raise RuntimeError("ETF 流入字段为空")
    return amount


def _fetch_farside_latest_flow() -> Tuple[Optional[float], FetchStatus]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": BROWSER_USER_AGENT,
            "Referer": "https://farside.co.uk/",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
        }
    )
    cookie = os.getenv("FARSIDE_COOKIES")
    if cookie:
        session.headers.update({"Cookie": cookie})
    errors: List[str] = []
    for fetcher, label in ((
        _fetch_farside_flow_from_html,
        "html",
    ), (_fetch_farside_flow_from_api, "api")):
        try:
            amount = fetcher(session)
            return amount, FetchStatus(name="btc_etf_flow", ok=True, message=f"ETF 净流入读取成功（{label}）")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{label}: {exc}")
    detail = "; ".join(errors) if errors else "未知原因"
    return None, FetchStatus(name="btc_etf_flow", ok=False, message=f"Farside 请求失败: {detail}")


def _fetch_btc_etf_flow(api_keys: Dict[str, Any]) -> Tuple[Optional[float], FetchStatus]:
    attempts: List[str] = []

    sosovalue_key = _coerce_api_key(api_keys.get("sosovalue"))
    if sosovalue_key:
        amount, status = _fetch_sosovalue_latest_flow(sosovalue_key)
        if status.ok and amount is not None:
            message = status.message
            if attempts:
                message += f"；此前失败: {', '.join(attempts)}"
            return amount, FetchStatus(name="btc_etf_flow", ok=True, message=message)
        attempts.append(status.message or "SoSoValue 获取失败")

    coinglass_key = _coerce_api_key(api_keys.get("coinglass"))
    if coinglass_key:
        amount, status = _fetch_coinglass_latest_flow(coinglass_key)
        if status.ok and amount is not None:
            message = status.message
            if attempts:
                message += f"；此前失败: {', '.join(attempts)}"
            return amount, FetchStatus(name="btc_etf_flow", ok=True, message=message)
        attempts.append(status.message or "CoinGlass 获取失败")

    amount, status = _fetch_farside_latest_flow()
    if status.ok and amount is not None:
        message = status.message
        if attempts:
            message += f"；已跳过 {', '.join(attempts)}"
        return amount, FetchStatus(name="btc_etf_flow", ok=True, message=message)

    if status.message:
        attempts.append(status.message)
    detail = "；".join(filter(None, attempts))
    message = detail or "ETF 净流入获取失败"
    return None, FetchStatus(name="btc_etf_flow", ok=False, message=message)


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


def _stooq_symbol_candidates(symbol: str) -> List[str]:
    base = symbol.lower()
    candidates = [base]
    if "." not in base and not base.startswith("^"):
        candidates.append(f"{base}.us")
    return list(dict.fromkeys(candidates))


def _fetch_stooq_series(symbol: str) -> List[Dict[str, Any]]:
    params = {"s": symbol.lower(), "i": "d"}
    resp = requests.get("https://stooq.com/q/d/l/", params=params, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    reader = csv.DictReader(resp.text.splitlines())
    rows: List[Dict[str, Any]] = []
    for row in reader:
        normalized = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in row.items() if k}
        if normalized.get("date"):
            rows.append(normalized)
    if len(rows) < 2:
        raise RuntimeError("Stooq 未返回足够的时间序列")
    return rows


def _extract_latest_change(rows: List[Dict[str, Any]], *, close_key: str = "close") -> Tuple[str, float, float]:
    ordered = sorted(rows, key=lambda item: item.get("date"))
    latest, prev = ordered[-1], ordered[-2]
    latest_close = _safe_float(latest.get(close_key))
    prev_close = _safe_float(prev.get(close_key))
    if latest_close is None or prev_close is None or prev_close == 0:
        raise RuntimeError("无法计算涨跌幅")
    change_pct = (latest_close - prev_close) / prev_close * 100
    return str(latest.get("date", "")), latest_close, change_pct


def _fetch_hsi_from_stooq() -> Tuple[List[Dict[str, Any]], str]:
    rows = _fetch_stooq_series("^hsi")
    day, close, change_pct = _extract_latest_change(rows)
    payload = [
        {
            "symbol": HK_MARKET_SYMBOLS[0]["label"],
            "close": round(close, 2),
            "change_pct": round(change_pct, 2),
        }
    ]
    message = f"使用 Stooq (^HSI) 数据（{day}）"
    return payload, message


def _fetch_yahoo_chart(symbol: str) -> Dict[str, Any]:
    encoded = quote(symbol, safe="")
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{encoded}"
    params = {"interval": "1d", "range": "5d"}
    payload = _request_json(url, params=params)
    chart = (payload.get("chart") or {}).get("result") or []
    if not chart:
        raise RuntimeError("Yahoo Finance 未返回行情")
    return chart[0]


def _extract_yahoo_change(chart: Dict[str, Any]) -> Tuple[str, float, float]:
    timestamps = chart.get("timestamp") or []
    quotes = (chart.get("indicators") or {}).get("quote") or []
    if not timestamps or not quotes:
        raise RuntimeError("Yahoo Finance 响应缺少时间序列")
    closes = quotes[0].get("close") or []
    pairs = [(ts, close) for ts, close in zip(timestamps, closes) if close is not None]
    if len(pairs) < 2:
        raise RuntimeError("Yahoo Finance 未返回足够的收盘价")
    pairs.sort(key=lambda item: item[0])
    prev_ts, prev_close = pairs[-2]
    latest_ts, latest_close = pairs[-1]
    if not prev_close:
        raise RuntimeError("Yahoo Finance 前一日收盘价无效")
    change_pct = (latest_close - prev_close) / prev_close * 100
    day = datetime.fromtimestamp(latest_ts, datetime.UTC).date().isoformat()
    return day, latest_close, change_pct


def _attempt_quote(fetchers: Iterable[Tuple[str, Callable[[], _QuoteSnapshot]]]) -> _QuoteSnapshot:
    errors: List[str] = []
    for label, fetcher in fetchers:
        try:
            return fetcher()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{label}: {exc}")
    detail = "; ".join(errors) if errors else "无可用行情来源"
    raise RuntimeError(detail)


def _fetch_quote_from_stooq(symbol: str) -> _QuoteSnapshot:
    errors: List[str] = []
    for candidate in _stooq_symbol_candidates(symbol):
        try:
            rows = _fetch_stooq_series(candidate)
            day, close, change_pct = _extract_latest_change(rows)
            return _QuoteSnapshot(day=day, close=round(close, 4), change_pct=round(change_pct, 4), source=f"stooq:{candidate}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{candidate}: {exc}")
    detail = "; ".join(errors) if errors else "Stooq 未返回数据"
    raise RuntimeError(detail)


def _fetch_quote_from_yahoo(symbol: str) -> _QuoteSnapshot:
    chart = _fetch_yahoo_chart(symbol)
    day, close, change_pct = _extract_yahoo_change(chart)
    return _QuoteSnapshot(day=day, close=round(close, 4), change_pct=round(change_pct, 4), source=f"yahoo:{symbol}")


def _fetch_quote_from_fmp(symbol: str, api_key: str) -> _QuoteSnapshot:
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{quote(symbol)}"
    params = {"timeseries": 2, "apikey": api_key}
    payload = _request_json(url, params=params)
    history = payload.get("historical") or []
    if len(history) < 2:
        raise RuntimeError("FMP 未返回足够的历史数据")
    latest, prev = history[0], history[1]
    day = latest.get("date")
    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(prev.get("close"))
    if not day or close is None or prev_close in (None, 0):
        raise RuntimeError("FMP 历史数据缺字段")
    change_pct = (close - prev_close) / prev_close * 100
    return _QuoteSnapshot(day=day, close=round(close, 4), change_pct=round(change_pct, 4), source="fmp")


def _fetch_quote_from_twelve_data(symbol: str, api_key: str) -> _QuoteSnapshot:
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 2,
        "apikey": api_key,
    }
    payload = _request_json("https://api.twelvedata.com/time_series", params=params)
    if isinstance(payload, dict) and payload.get("status") == "error":
        raise RuntimeError(payload.get("message") or "Twelve Data 返回错误")
    values = payload.get("values") if isinstance(payload, dict) else None
    if not values or len(values) < 2:
        raise RuntimeError("Twelve Data 未返回足够的时间序列")
    latest, prev = values[0], values[1]
    day = latest.get("datetime")
    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(prev.get("close"))
    if not day or close is None or prev_close in (None, 0):
        raise RuntimeError("Twelve Data 时间序列缺字段")
    change_pct = (close - prev_close) / prev_close * 100
    normalized_day = day.split(" ")[0] if isinstance(day, str) else str(day)
    return _QuoteSnapshot(day=normalized_day, close=round(close, 4), change_pct=round(change_pct, 4), source="twelve_data")


def _fetch_quote_from_alpha(symbol: str, api_key: str) -> _QuoteSnapshot:
    series = _fetch_alpha_series(symbol, api_key)
    day, close, change_pct = _extract_close_change(series)
    return _QuoteSnapshot(day=day, close=round(close, 4), change_pct=round(change_pct, 4), source="alpha_vantage")


def _fetch_hsi_from_yahoo() -> Tuple[List[Dict[str, Any]], str]:
    chart = _fetch_yahoo_chart("^HSI")
    day, close, change_pct = _extract_yahoo_change(chart)
    payload = [
        {
            "symbol": HK_MARKET_SYMBOLS[0]["label"],
            "close": round(close, 2),
            "change_pct": round(change_pct, 2),
        }
    ]
    message = f"使用 Yahoo Finance ^HSI 数据（{day}）"
    return payload, message


def _fetch_hk_proxy_from_yahoo(symbol: str) -> Tuple[List[Dict[str, Any]], str]:
    chart = _fetch_yahoo_chart(symbol)
    day, close, change_pct = _extract_yahoo_change(chart)
    payload = [
        {
            "symbol": HK_MARKET_SYMBOLS[0]["label"],
            "close": round(close, 2),
            "change_pct": round(change_pct, 2),
        }
    ]
    message = f"使用 Yahoo Finance {symbol} 代理（{day}）"
    return payload, message


def _fetch_hk_market_snapshot(api_keys: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    errors: List[str] = []

    for fetcher in (_fetch_hsi_from_stooq, _fetch_hsi_from_yahoo):
        try:
            rows, message = fetcher()
            return rows, FetchStatus(name="hongkong_HSI", ok=True, message=message)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{fetcher.__name__}: {exc}")

    for proxy in HK_PROXY_SYMBOLS:
        try:
            rows, message = _fetch_hk_proxy_from_yahoo(proxy)
            return rows, FetchStatus(name="hongkong_HSI", ok=True, message=message)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{proxy}: {exc}")

    detail = "; ".join(errors) if errors else "未知原因"
    return [], FetchStatus(name="hongkong_HSI", ok=False, message=f"港股行情获取失败: {detail}")


FMP_STABLE_URL = "https://financialmodelingprep.com/stable/quote"
FMP_V3_URL = "https://financialmodelingprep.com/api/v3/quote"
FMP_WARMUP_URL = "https://financialmodelingprep.com/"
FMP_BATCH_SIZE = 50
FMP_MAX_RETRIES = 3
FMP_RETRY_DELAY = 0.8


class _FMPThrottledError(RuntimeError):
    """Raised when FMP signals throttling or access denial."""


def _normalize_fmp_payload(payload: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, list):
        raise RuntimeError("FMP 响应格式异常")
    results: Dict[str, Dict[str, Any]] = {}
    for item in payload:
        symbol = item.get("symbol") if isinstance(item, dict) else None
        if symbol:
            results[symbol] = item
    if not results:
        raise RuntimeError("FMP 返回为空")
    return results


def _chunk_symbols(symbols: List[str], size: int) -> Iterable[List[str]]:
    unique = sorted({s for s in symbols if s})
    for idx in range(0, len(unique), size):
        yield unique[idx : idx + size]


def _init_fmp_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json",
    })
    try:
        session.get(FMP_WARMUP_URL, timeout=REQUEST_TIMEOUT)
    except Exception:  # noqa: BLE001 - warm-up best effort
        pass
    return session


def _request_fmp_chunk(session: requests.Session, symbols: List[str], api_key: str) -> Dict[str, Dict[str, Any]]:
    params = {"symbol": ",".join(symbols), "apikey": api_key}
    response = session.get(FMP_STABLE_URL, params=params, timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        try:
            return _normalize_fmp_payload(response.json())
        except ValueError as exc:  # JSON decode error
            raise RuntimeError("FMP 响应解析失败") from exc
    if response.status_code == 429:
        raise _FMPThrottledError(f"FMP 限流: HTTP {response.status_code}")
    if response.status_code in {402, 403}:
        fallback = session.get(FMP_V3_URL, params=params, timeout=REQUEST_TIMEOUT)
        try:
            fallback.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"FMP v3 请求失败: {exc}") from exc
        try:
            return _normalize_fmp_payload(fallback.json())
        except ValueError as exc:  # JSON decode error
            raise RuntimeError("FMP v3 响应解析失败") from exc
    try:
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"FMP 请求失败: {exc}") from exc
    raise RuntimeError(f"FMP 未知状态码: {response.status_code}")


def _fetch_fmp_quotes_stable(symbols: List[str], api_key: str) -> Dict[str, Dict[str, Any]]:
    session = _init_fmp_session()
    results: Dict[str, Dict[str, Any]] = {}
    for batch in _chunk_symbols(symbols, FMP_BATCH_SIZE):
        delay = FMP_RETRY_DELAY
        for attempt in range(1, FMP_MAX_RETRIES + 1):
            try:
                payload = _request_fmp_chunk(session, batch, api_key)
            except _FMPThrottledError as exc:
                raise RuntimeError(str(exc)) from exc
            except Exception as exc:  # noqa: BLE001
                if attempt == FMP_MAX_RETRIES:
                    raise RuntimeError(f"{','.join(batch)}: {exc}") from exc
                time.sleep(delay)
                delay *= 2
                continue
            results.update(payload)
            time.sleep(0.3)
            break
    missing = sorted(set(symbols) - set(results))
    if missing:
        raise RuntimeError(f"FMP 缺少符号: {', '.join(missing)}")
    if not results:
        raise RuntimeError("FMP 稳定端点未返回数据")
    return results


def _fetch_fmp_quotes(symbols: List[str], api_key: str) -> Tuple[Dict[str, Dict[str, Any]], str]:
    try:
        return _fetch_fmp_quotes_stable(symbols, api_key), "fmp_stable"
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"fmp_stable: {exc}") from exc


def _fetch_yahoo_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    joined = ",".join(sorted(set(symbols)))
    urls = [
        "https://query2.finance.yahoo.com/v10/finance/quote",
        "https://query1.finance.yahoo.com/v10/finance/quote",
        "https://query2.finance.yahoo.com/v7/finance/quote",
        "https://query1.finance.yahoo.com/v7/finance/quote",
        "https://query2.finance.yahoo.com/v6/finance/quote",
    ]
    headers = {
        "Accept": "application/json",
        "User-Agent": BROWSER_USER_AGENT,
        "Referer": "https://finance.yahoo.com/",
        "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
        "Connection": "keep-alive",
    }
    last_error: Optional[Exception] = None
    for url in urls:
        try:
            payload = _request_json(url, params={"symbols": joined}, headers=headers)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue

        items = (payload.get("quoteResponse") or {}).get("result") or []
        if not items:
            last_error = RuntimeError("empty response")
            continue

        results: Dict[str, Dict[str, Any]] = {}
        for item in items:
            symbol = item.get("symbol") if isinstance(item, dict) else None
            if not symbol:
                continue
            results[symbol] = {
                "changesPercentage": item.get("regularMarketChangePercent"),
                "pe": item.get("trailingPE"),
                "priceToSalesRatioTTM": item.get("priceToSalesTrailing12Months"),
                "marketCap": item.get("marketCap"),
            }
        if results:
            return results
    raise RuntimeError(f"Yahoo Finance 未返回报价: {last_error or 'empty'}")


def _fetch_price_only_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        snapshot: Optional[_QuoteSnapshot] = None
        try:
            snapshot = _fetch_quote_from_yahoo(symbol)
        except Exception:  # noqa: BLE001
            try:
                snapshot = _fetch_quote_from_stooq(symbol)
            except Exception:  # noqa: BLE001
                snapshot = None
        if not snapshot:
            continue
        results[symbol] = {
            "changesPercentage": snapshot.change_pct,
            "pe": None,
            "priceToSalesRatioTTM": None,
            "marketCap": None,
        }
        time.sleep(0.2)
    if not results:
        raise RuntimeError("price-only fallback 无法获取任何报价")
    return results


def _mean(values: List[Optional[float]]) -> Optional[float]:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _fetch_theme_metrics_from_fmp(api_keys: Dict[str, Any]) -> Tuple[Dict[str, Any], FetchStatus]:
    all_symbols: List[str] = []
    for symbols in FMP_THEME_SYMBOLS.values():
        all_symbols.extend(symbols)

    quotes: Dict[str, Dict[str, Any]] = {}
    source = ""
    errors: List[str] = []

    api_key = api_keys.get("financial_modeling_prep")
    if api_key:
        try:
            quotes, source = _fetch_fmp_quotes(all_symbols, api_key)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"FMP: {exc}")

    if not quotes:
        try:
            quotes = _fetch_yahoo_quotes(all_symbols)
            source = "yahoo"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Yahoo: {exc}")
            try:
                quotes = _fetch_price_only_quotes(all_symbols)
                source = "price_only"
            except Exception as fallback_exc:  # noqa: BLE001
                errors.append(f"price_only: {fallback_exc}")
                detail = "; ".join(errors) if errors else "未知原因"
                return {}, FetchStatus(name="fmp_theme", ok=False, message=f"主题估值获取失败: {detail}")

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
        return {}, FetchStatus(name="fmp_theme", ok=False, message="主题估值数据为空")

    if source == "yahoo":
        message = "主题估值使用 Yahoo Finance 兜底"
    elif source == "price_only":
        message = "主题估值使用价格兜底，PE/PS/市值为空"
    elif source:
        message = "FMP 主题估值已获取"
    else:
        message = "主题估值已获取"

    return themes, FetchStatus(name="fmp_theme", ok=True, message=message)


def _resolve_index_quote(symbol: str, api_keys: Dict[str, Any]) -> _QuoteSnapshot:
    fetchers: List[Tuple[str, Callable[[], _QuoteSnapshot]]] = [
        ("stooq", lambda: _fetch_quote_from_stooq(symbol)),
        ("yahoo", lambda: _fetch_quote_from_yahoo(symbol)),
    ]
    fmp_key = api_keys.get("financial_modeling_prep")
    if fmp_key:
        fetchers.append(("fmp", lambda: _fetch_quote_from_fmp(symbol, fmp_key)))
    twelve_key = api_keys.get("twelve_data")
    if twelve_key:
        fetchers.append(("twelve_data", lambda: _fetch_quote_from_twelve_data(symbol, twelve_key)))
    alpha_key = api_keys.get("alpha_vantage")
    if alpha_key:
        fetchers.append(("alpha_vantage", lambda: _fetch_quote_from_alpha(symbol, alpha_key)))
    return _attempt_quote(fetchers)


def _resolve_equity_quote(symbol: str, api_keys: Dict[str, Any]) -> _QuoteSnapshot:
    fetchers: List[Tuple[str, Callable[[], _QuoteSnapshot]]] = []
    fmp_key = api_keys.get("financial_modeling_prep")
    if fmp_key:
        fetchers.append(("fmp", lambda: _fetch_quote_from_fmp(symbol, fmp_key)))
    twelve_key = api_keys.get("twelve_data")
    if twelve_key:
        fetchers.append(("twelve_data", lambda: _fetch_quote_from_twelve_data(symbol, twelve_key)))
    fetchers.append(("yahoo", lambda: _fetch_quote_from_yahoo(symbol)))
    fetchers.append(("stooq", lambda: _fetch_quote_from_stooq(symbol)))
    alpha_key = api_keys.get("alpha_vantage")
    if alpha_key:
        fetchers.append(("alpha_vantage", lambda: _fetch_quote_from_alpha(symbol, alpha_key)))
    return _attempt_quote(fetchers)


def _fetch_market_snapshot_real(api_keys: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], FetchStatus]:
    errors: List[str] = []
    indices: List[Dict[str, Any]] = []
    index_sources: Dict[str, str] = {}
    latest_date: Optional[str] = None

    for label, proxy in ALPHA_VANTAGE_SYMBOLS.items():
        try:
            snapshot = _resolve_index_quote(proxy, api_keys)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{label}: {exc}")
            continue
        latest_date = latest_date or snapshot.day
        indices.append({"symbol": label, "close": round(snapshot.close, 2), "change_pct": round(snapshot.change_pct, 2)})
        index_sources[label] = snapshot.source

    sectors: List[Dict[str, Any]] = []
    sector_sources: Dict[str, str] = {}
    for name, proxy in SECTOR_PROXIES.items():
        try:
            snapshot = _resolve_equity_quote(proxy, api_keys)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")
            continue
        sectors.append({"name": name, "performance": round(1 + snapshot.change_pct / 100, 3)})
        sector_sources[name] = snapshot.source

    if not indices:
        detail = "; ".join(errors) if errors else "无可用行情来源"
        return None, FetchStatus(name="market", ok=False, message=f"美股行情获取失败: {detail}")

    market = {
        "date": latest_date or _current_trading_day(),
        "indices": indices,
        "sectors": sectors,
    }

    parts: List[str] = []
    if index_sources:
        formatted = ", ".join(f"{symbol}:{src}" for symbol, src in index_sources.items())
        parts.append(f"指数来源 {formatted}")
    if sector_sources:
        formatted = ", ".join(f"{name}:{src}" for name, src in sector_sources.items())
        parts.append(f"板块来源 {formatted}")
    if errors:
        parts.append(f"降级 {len(errors)} 项")
    message = "；".join(parts) if parts else "市场行情已获取"
    return market, FetchStatus(name="market", ok=True, message=message)


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


def run(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制刷新当日数据")
    args = parser.parse_args(argv)

    _ensure_out_dir()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    trading_day = _current_trading_day()

    raw_market_path = OUT_DIR / "raw_market.json"
    raw_events_path = OUT_DIR / "raw_events.json"
    status_path = OUT_DIR / "etl_status.json"
    marker = STATE_DIR / f"fetch_{trading_day}"

    if not args.force:
        skip_run = marker.exists()
        if skip_run and not (raw_market_path.exists() and raw_events_path.exists() and status_path.exists()):
            skip_run = False
        if skip_run and status_path.exists():
            try:
                with status_path.open("r", encoding="utf-8") as fh:
                    status_payload = json.load(fh)
            except Exception:  # noqa: BLE001 - ignore corrupt status cache
                skip_run = False
            else:
                if status_payload.get("date") != trading_day:
                    skip_run = False
        if skip_run:
            print("检测到当日数据，跳过抓取。用 --force 可强制刷新。")
            return 0

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

    previous_sentiment: Dict[str, Any] = {}
    previous_btc: Dict[str, Any] = {}
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
            prev_btc = previous_payload.get("btc")
            if isinstance(prev_btc, dict):
                previous_btc = prev_btc

    market_data, status = _fetch_market_snapshot_real(api_keys)
    statuses.append(status)
    if not status.ok:
        overall_ok = False
        fallback, fallback_status = _simulate_market_snapshot(trading_day)
        market_data = fallback
        statuses.append(fallback_status)
    else:
        market_data = market_data or {}

    hk_rows, hk_status = _fetch_hk_market_snapshot(api_keys)
    statuses.append(hk_status)
    if hk_status.ok and market_data is not None:
        market_data.setdefault("hk_indices", hk_rows)
    elif not hk_status.ok:
        overall_ok = False

    theme_metrics: Dict[str, Any] = {}
    if market_data:
        sectors = market_data.get("sectors", []) if isinstance(market_data, dict) else []
        ai_perf = next((s.get("performance") for s in sectors if s.get("name") == "AI"), None)
        if ai_perf is not None:
            theme_metrics.setdefault("ai", {})["performance"] = ai_perf

    if api_keys:
        themes, theme_status = _fetch_theme_metrics_from_fmp(api_keys)
        statuses.append(theme_status)
        if theme_status.ok:
            for name, metrics in themes.items():
                theme_metrics.setdefault(name, {}).update(metrics)

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

    etf_flow, flow_status = _fetch_btc_etf_flow(api_keys)
    statuses.append(flow_status)
    if not flow_status.ok:
        previous_flow = None
        if isinstance(previous_btc, dict):
            previous_flow = _safe_float(previous_btc.get("etf_net_inflow_musd"))
        if previous_flow is not None:
            etf_flow = previous_flow
            statuses.append(
                FetchStatus(
                    name="btc_etf_flow_fallback",
                    ok=True,
                    message="使用上一期 ETF 净流入",
                )
            )
            overall_ok = False

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

    marker.touch(exist_ok=True)

    print("原始数据已输出到 out/ 目录。")
    return 0


if __name__ == "__main__":
    sys.exit(run())
