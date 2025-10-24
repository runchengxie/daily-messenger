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
import logging
import os
import random
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote
from xml.etree import ElementTree as ET

import requests
from zoneinfo import ZoneInfo

from daily_messenger.common import run_meta
from daily_messenger.common.logging import log, setup_logger

if __package__:
    from .fetchers import aaii_sentiment, cboe_putcall
else:  # pragma: no cover - runtime convenience for direct script execution
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from daily_messenger.etl.fetchers import aaii_sentiment, cboe_putcall

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = PROJECT_ROOT / "out"
STATE_DIR = PROJECT_ROOT / "state"
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
CHINA_TZ = ZoneInfo("Asia/Shanghai")

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

THROTTLE_DISABLED = os.getenv("DM_DISABLE_THROTTLE", "").lower() in {"1", "true", "yes"}


@dataclass(frozen=True)
class _MarketNewsSpec:
    market: str
    label: str
    timezone: ZoneInfo
    close_hour: int
    close_minute: int
    scope: str


@dataclass
class _GeminiSettings:
    model: str
    keys: List[Tuple[str, str]]
    enable_network: bool
    timeout: float
    extra_instructions: str = ""

DEFAULT_GEMINI_MODEL = "gemini-2.5-pro"
DEFAULT_GEMINI_TIMEOUT = 45.0
DEFAULT_GEMINI_ENABLE_NETWORK = True
NEWS_TAG_PATTERN = re.compile(r"<news>(.*?)</news>", re.IGNORECASE | re.DOTALL)

GEMINI_MARKET_SPECS: Tuple[_MarketNewsSpec, ...] = (
    _MarketNewsSpec(
        market="us",
        label="美股",
        timezone=ZoneInfo("America/New_York"),
        close_hour=16,
        close_minute=0,
        scope="美国股市",
    ),
    _MarketNewsSpec(
        market="jp",
        label="日股",
        timezone=ZoneInfo("Asia/Tokyo"),
        close_hour=15,
        close_minute=0,
        scope="日本股市",
    ),
    _MarketNewsSpec(
        market="hk",
        label="港股",
        timezone=ZoneInfo("Asia/Hong_Kong"),
        close_hour=16,
        close_minute=0,
        scope="香港股市",
    ),
    _MarketNewsSpec(
        market="cn",
        label="A 股",
        timezone=ZoneInfo("Asia/Shanghai"),
        close_hour=15,
        close_minute=0,
        scope="中国内地 A 股市场",
    ),
    _MarketNewsSpec(
        market="gold",
        label="黄金",
        timezone=ZoneInfo("America/New_York"),
        close_hour=17,
        close_minute=0,
        scope="国际黄金市场",
    ),
)


def _sleep(seconds: float) -> None:
    if seconds <= 0 or THROTTLE_DISABLED:
        return
    time.sleep(seconds)


def _business_day_on_or_before(day: datetime) -> datetime:
    candidate = day
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _resolve_market_trading_date(
    now_utc: datetime, spec: _MarketNewsSpec
) -> datetime:
    local_now = now_utc.astimezone(spec.timezone)
    candidate = datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        0,
        0,
        0,
        tzinfo=spec.timezone,
    )
    close_dt = datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        spec.close_hour,
        spec.close_minute,
        0,
        tzinfo=spec.timezone,
    )
    if local_now < close_dt:
        candidate -= timedelta(days=1)
    candidate = _business_day_on_or_before(candidate)
    return candidate


def _format_cn_date(day: datetime) -> str:
    return day.strftime("%Y年%m月%d日")


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
    override = os.getenv("DM_OVERRIDE_DATE")
    if override:
        return override
    now = datetime.now(PACIFIC_TZ)
    return now.strftime("%Y-%m-%d")


CANONICAL_API_KEYS = (
    "alpha_vantage",
    "twelve_data",
    "financial_modeling_prep",
    "trading_economics",
    "finnhub",
    "sosovalue",
    "coinglass",
    "alpaca_key_id",
    "alpaca_secret",
)


class ApiKeyValidationError(Exception):
    def __init__(self, errors: Dict[str, str]) -> None:
        super().__init__("API key validation failed")
        self.errors = errors


def _normalize_api_keys(payload: Dict[str, Any]) -> Dict[str, str]:
    errors: Dict[str, str] = {}
    normalized: Dict[str, str] = {}
    extras: Dict[str, Any] = {}
    canonical = set(CANONICAL_API_KEYS)
    for key, value in payload.items():
        if key in canonical:
            if value is None:
                continue
            if not isinstance(value, str):
                errors[key] = "expected string value"
                continue
            stripped = value.strip()
            if not stripped:
                errors[key] = "empty string"
                continue
            normalized[key] = stripped
        else:
            extras[key] = value
    if errors:
        raise ApiKeyValidationError(errors)
    result: Dict[str, Any] = dict(normalized)
    result.update(extras)
    return result


def _load_api_keys(logger: logging.Logger | None) -> Dict[str, str]:
    data: Dict[str, Any] = {}

    # 1) Optional JSON file referenced via API_KEYS_PATH
    path_hint = os.getenv("API_KEYS_PATH")
    if path_hint:
        candidate_paths = []
        expanded = Path(path_hint).expanduser()
        candidate_paths.append(expanded)
        if not expanded.is_absolute():
            candidate_paths.append((PROJECT_ROOT / expanded).resolve())
        for candidate in candidate_paths:
            try:
                with open(candidate, "r", encoding="utf-8") as fh:
                    payload = json.load(fh)
            except FileNotFoundError:
                continue
            except Exception as exc:  # noqa: BLE001
                if logger:
                    log(
                        logger,
                        logging.WARNING,
                        "api_keys_path_failed",
                        path=str(candidate),
                        error=str(exc),
                    )
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
            if logger:
                log(
                    logger,
                    logging.ERROR,
                    "api_keys_inline_invalid_json",
                    error=str(exc),
                )
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
        te_password = env.get("TRADING_ECONOMICS_PASSWORD") or env.get(
            "trading_economics_password"
        )
        if te_user and te_password:
            data["trading_economics"] = f"{te_user}:{te_password}"

    try:
        normalized = _normalize_api_keys(data)
    except ApiKeyValidationError as exc:
        for key, reason in exc.errors.items():
            if logger:
                log(
                    logger,
                    logging.WARNING,
                    "api_key_invalid_entry",
                    key=key,
                    reason=reason,
                )
        normalized = {k: v for k, v in data.items() if isinstance(v, str) and v.strip()}

    extra_keys = sorted(set(normalized) - set(CANONICAL_API_KEYS))
    if extra_keys:
        if logger:
            log(logger, logging.INFO, "api_key_extra_entries", keys=extra_keys)
    return normalized


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


def _collect_gemini_keys(config: Dict[str, Any]) -> List[Tuple[str, str]]:
    keys: List[Tuple[str, str]] = []
    seen: set[str] = set()

    def _push(value: Any, label: str) -> None:
        if not isinstance(value, str):
            return
        token = value.strip()
        if not token or token in seen:
            return
        keys.append((label, token))
        seen.add(token)

    raw_keys = config.get("keys")
    if isinstance(raw_keys, list):
        for idx, entry in enumerate(raw_keys, start=1):
            if isinstance(entry, str):
                _push(entry, f"key_{idx}")
            elif isinstance(entry, dict):
                token = entry.get("value") or entry.get("key") or entry.get("api_key")
                label = str(entry.get("label") or entry.get("name") or f"key_{idx}")
                _push(token, label)
    else:
        single_candidate = (
            config.get("key")
            or config.get("api_key")
            or config.get("token")
            or config.get("value")
        )
        if isinstance(single_candidate, str):
            _push(single_candidate, "primary")

    # Allow arbitrary extra fields to serve as keys when prefixed with gemini
    for field, value in config.items():
        if not isinstance(field, str):
            continue
        lowered = field.lower()
        if lowered.startswith("gemini") and lowered not in {"gemini_model"}:
            _push(value, field)

    return keys


def _resolve_gemini_settings(api_keys: Dict[str, Any]) -> Optional[_GeminiSettings]:
    section = api_keys.get("ai_news")
    if isinstance(section, str):
        # Attempt to parse JSON if provided as a string
        try:
            section = json.loads(section)
        except json.JSONDecodeError:
            section = None

    if not isinstance(section, dict):
        # Fallback: gather top-level keys with gemini prefix
        raw_keys: List[Tuple[str, str]] = []
        for field, value in api_keys.items():
            if not isinstance(field, str):
                continue
            lowered = field.lower()
            if lowered.startswith("gemini"):
                if isinstance(value, str):
                    token = value.strip()
                    if token:
                        raw_keys.append((field, token))
        if not raw_keys:
            return None
        return _GeminiSettings(
            model=DEFAULT_GEMINI_MODEL,
            keys=raw_keys,
            enable_network=DEFAULT_GEMINI_ENABLE_NETWORK,
            timeout=DEFAULT_GEMINI_TIMEOUT,
        )

    model_candidate = section.get("model") or section.get("gemini_model")
    model = (
        str(model_candidate).strip()
        if isinstance(model_candidate, str) and model_candidate.strip()
        else DEFAULT_GEMINI_MODEL
    )

    enable_network_candidate = section.get("enable_network")
    if enable_network_candidate is None:
        enable_network_candidate = section.get("google_search")
    enable_network = (
        bool(enable_network_candidate)
        if isinstance(enable_network_candidate, bool)
        else _env_truthy(str(enable_network_candidate))
        if isinstance(enable_network_candidate, str)
        else DEFAULT_GEMINI_ENABLE_NETWORK
    )

    timeout_candidate = section.get("timeout_seconds") or section.get("timeout")
    if isinstance(timeout_candidate, (int, float)) and timeout_candidate > 0:
        timeout = float(timeout_candidate)
    else:
        timeout = DEFAULT_GEMINI_TIMEOUT

    extra_prompt_candidate = section.get("extra_prompt") or section.get(
        "extra_instructions"
    )
    extra_instructions = (
        str(extra_prompt_candidate).strip()
        if isinstance(extra_prompt_candidate, str)
        else ""
    )

    keys = _collect_gemini_keys(section)
    if not keys:
        # Permit fallback to top-level entries if nested keys missing
        for field, value in api_keys.items():
            if not isinstance(field, str):
                continue
            lowered = field.lower()
            if lowered.startswith("gemini"):
                if isinstance(value, str):
                    token = value.strip()
                    if token:
                        keys.append((field, token))
    if not keys:
        return None

    return _GeminiSettings(
        model=model,
        keys=keys,
        enable_network=enable_network,
        timeout=timeout,
        extra_instructions=extra_instructions,
    )


def _call_gemini_generate_content(
    model: str,
    api_key: str,
    prompt: str,
    enable_network: bool,
    timeout: float,
) -> Dict[str, Any]:
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    params = {"key": api_key}
    body: Dict[str, Any] = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt,
                    }
                ],
            }
        ]
    }
    if enable_network:
        body["tools"] = [{"googleSearchRetrieval": {}}]
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
    }
    resp = requests.post(
        url,
        params=params,
        headers=headers,
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    try:
        payload = resp.json()
    except ValueError as exc:  # noqa: BLE001
        raise ValueError("Gemini 响应不是 JSON") from exc
    return payload


def _extract_gemini_text(payload: Dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return ""
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    return text.strip()
    return ""


def _extract_news_section(text: str) -> str:
    if not text:
        return ""
    sections = [match.strip() for match in NEWS_TAG_PATTERN.findall(text) if match]
    if sections:
        combined = "\n".join(section.strip() for section in sections if section.strip())
        return combined.strip()
    # Fallback: strip any outer tags and return trimmed text
    return text.strip()


def _build_market_prompt(
    spec: _MarketNewsSpec,
    target_day: datetime,
    now_beijing: datetime,
    settings: _GeminiSettings,
) -> str:
    target_iso = target_day.date().isoformat()
    target_cn = _format_cn_date(target_day.date())
    now_cn = now_beijing.strftime("%Y年%m月%d日 %H:%M")
    extra = f"\n{settings.extra_instructions.strip()}" if settings.extra_instructions else ""
    return (
        f"今天的日期是北京时间 {now_cn}。"
        f"请联网搜索并总结 {target_cn}（交易日 {target_iso}）{spec.scope}的主要资讯。"
        "重点包括：核心指数或价格的收盘表现与涨跌幅、盘面主题或板块亮点、以及可能影响市场的重大公司事件或宏观新闻。"
        "如果查询结果显示该日期尚未结束或被视为未来时间，请自动回退到最近一个已经结束的交易日，并在摘要开头注明实际覆盖的日期与原因。"
        "输出需要使用中文、保持客观中性语气；涉及新疆、香港、台湾等敏感议题时请遵循国内审核要求并避免额外延展。"
        "请将完整内容放在单个 <news> 标签中，标签内使用 Markdown 列出 3-5 条重点，每条最好附带来源或链接。"
        "除 <news>...</news> 外不要输出其它文本。"
        f"{extra}"
    )


def _fetch_gemini_market_news(
    now_utc: datetime,
    api_keys: Dict[str, Any],
    logger: Optional[logging.Logger],
) -> Tuple[List[Dict[str, Any]], List[FetchStatus]]:
    settings = _resolve_gemini_settings(api_keys)
    if settings is None:
        return (
            [],
            [
                FetchStatus(
                    name="gemini_news",
                    ok=True,
                    message="Gemini AI 未配置，跳过市场资讯",
                )
            ],
        )

    updates: List[Dict[str, Any]] = []
    statuses: List[FetchStatus] = []

    if not settings.keys:
        return (
            updates,
            [
                FetchStatus(
                    name="gemini_news",
                    ok=False,
                    message="Gemini AI 未提供可用的 API key",
                )
            ],
        )

    key_queue: deque[Tuple[str, str]] = deque(settings.keys)
    if len(key_queue) > 1:
        key_queue.rotate(-random.randint(0, len(key_queue) - 1))
    beijing_now = now_utc.astimezone(CHINA_TZ)

    for spec in GEMINI_MARKET_SPECS:
        if not key_queue:
            statuses.append(
                FetchStatus(
                    name=f"gemini_news_{spec.market}",
                    ok=False,
                    message="缺少 Gemini API key",
                )
            )
            continue

        target_day = _resolve_market_trading_date(now_utc, spec)
        prompt = _build_market_prompt(spec, target_day, beijing_now, settings)

        response_text = ""
        error_messages: List[str] = []

        for _ in range(len(key_queue)):
            label, token = key_queue[0]
            try:
                payload = _call_gemini_generate_content(
                    settings.model,
                    token,
                    prompt,
                    settings.enable_network,
                    settings.timeout,
                )
            except requests.HTTPError as exc:
                error_messages.append(f"{label}: HTTP {exc}")
                key_queue.rotate(-1)
                continue
            except Exception as exc:  # noqa: BLE001
                error_messages.append(f"{label}: {exc}")
                key_queue.rotate(-1)
                continue

            text = _extract_gemini_text(payload)
            if not text:
                error_messages.append(f"{label}: 空响应")
                key_queue.rotate(-1)
                continue

            response_text = text
            key_queue.rotate(-1)
            break

        if not response_text:
            detail = "；".join(error_messages[-3:]) if error_messages else "未知错误"
            statuses.append(
                FetchStatus(
                    name=f"gemini_news_{spec.market}",
                    ok=False,
                    message=f"{spec.label} 摘要生成失败（{detail}）",
                )
            )
            continue

        news_section = _extract_news_section(response_text)
        if not news_section:
            statuses.append(
                FetchStatus(
                    name=f"gemini_news_{spec.market}",
                    ok=False,
                    message=f"{spec.label} 响应缺少 <news> 内容",
                )
            )
            continue

        summary_lines = [
            line.rstrip()
            for line in news_section.splitlines()
            if line.strip()
        ]
        summary = "\n".join(summary_lines)
        update = {
            "title": f"{spec.label} {target_day.date().isoformat()} 交易日资讯",
            "market": spec.market,
            "date": target_day.date().isoformat(),
            "summary": summary,
            "source": "gemini",
            "provider": "google_gemini",
            "model": settings.model,
            "prompt_scope": spec.scope,
            "prompt_date": target_day.date().isoformat(),
            "requested_beijing": beijing_now.isoformat(),
            "raw_text": news_section,
        }
        updates.append(update)
        statuses.append(
            FetchStatus(
                name=f"gemini_news_{spec.market}",
                ok=True,
                message=f"{spec.label} 摘要生成成功",
            )
        )
        if logger:
            log(
                logger,
                logging.INFO,
                "gemini_news_generated",
                market=spec.market,
                prompt_date=update["prompt_date"],
            )

    return updates, statuses


def _load_configuration(
    logger: logging.Logger | None = None,
) -> Tuple[Dict[str, Any], List[str], Dict[str, Any], float]:
    api_keys = _load_api_keys(logger)
    ai_feeds = _resolve_ai_feeds(api_keys)
    arxiv_params, arxiv_throttle = _resolve_arxiv_config(api_keys)
    return api_keys, ai_feeds, arxiv_params, arxiv_throttle


API_KEYS_CACHE: Dict[str, str] = {}
AI_NEWS_FEEDS: List[str] = list(DEFAULT_AI_FEEDS)
ARXIV_QUERY_PARAMS: Dict[str, Any] = dict(DEFAULT_ARXIV_PARAMS)
ARXIV_THROTTLE: float = DEFAULT_ARXIV_THROTTLE

# Preload configuration once at import so module globals reflect runtime hints.
API_KEYS_CACHE, AI_NEWS_FEEDS, ARXIV_QUERY_PARAMS, ARXIV_THROTTLE = (
    _load_configuration()
)


def _respect_retry_after(resp: requests.Response) -> Optional[float]:
    retry_after = resp.headers.get("Retry-After")
    if not retry_after:
        return None
    try:
        return float(retry_after)
    except ValueError:
        return None


def _sleep_exact(seconds: float) -> None:
    if seconds and seconds > 0:
        time.sleep(seconds)


class RetryPolicy:
    def __init__(
        self,
        retries: int = 3,
        backoff_start: float = 0.6,
        backoff_factor: float = 2.0,
        jitter: float = 0.3,
        status_forcelist: Iterable[int] = (408, 409, 425, 429, 500, 502, 503, 504),
        max_sleep: float = 8.0,
        per_request_timeout: float = REQUEST_TIMEOUT,
        hard_deadline: Optional[float] = 20.0,
    ):
        self.retries = retries
        self.backoff_start = backoff_start
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.status_forcelist = set(status_forcelist)
        self.max_sleep = max_sleep
        self.per_request_timeout = per_request_timeout
        self.hard_deadline = hard_deadline


RETRY_DEFAULT = RetryPolicy()
RETRY_EDGAR = RetryPolicy(retries=3, backoff_start=0.6, backoff_factor=2.0, jitter=0.25)


def _request_json(
    url: str,
    *,
    method: str = "GET",
    session: Optional[requests.Session] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    headers: Optional[Dict[str, str]] = None,
    policy: RetryPolicy = RETRY_DEFAULT,
    after_each_sleep: float = 0.0,
) -> Any:
    method = method.upper()
    own_session = session or requests.Session()
    close_session = session is None
    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)
    attempt = 0
    delay = policy.backoff_start
    start = time.monotonic()
    try:
        while True:
            attempt += 1
            try:
                resp = own_session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=hdrs,
                    timeout=policy.per_request_timeout,
                )
            except requests.RequestException as exc:
                if attempt > policy.retries:
                    raise RuntimeError(
                        f"HTTP 请求失败（已重试 {attempt - 1} 次）: {exc}"
                    ) from exc
                sleep_seconds = min(
                    delay * (1.0 + random.random() * policy.jitter), policy.max_sleep
                )
                if (
                    policy.hard_deadline
                    and (time.monotonic() - start + sleep_seconds)
                    > policy.hard_deadline
                ):
                    raise RuntimeError("HTTP 请求失败：超过重试预算") from exc
                _sleep_exact(sleep_seconds)
                delay *= policy.backoff_factor
                continue

            if resp.status_code in policy.status_forcelist:
                retry_after = (
                    _respect_retry_after(resp) if resp.status_code == 429 else None
                )
                if attempt > policy.retries:
                    resp.raise_for_status()
                sleep_seconds = (
                    retry_after
                    if retry_after is not None
                    else min(
                        delay * (1.0 + random.random() * policy.jitter),
                        policy.max_sleep,
                    )
                )
                if (
                    policy.hard_deadline
                    and (time.monotonic() - start + sleep_seconds)
                    > policy.hard_deadline
                ):
                    resp.raise_for_status()
                _sleep_exact(sleep_seconds)
                delay *= policy.backoff_factor
                continue

            try:
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                snippet = resp.text[:200] if getattr(resp, "text", None) else str(exc)
                raise RuntimeError(
                    f"HTTP 状态错误: {resp.status_code} {snippet}"
                ) from exc

            try:
                payload = resp.json()
            except ValueError as exc:
                if attempt <= policy.retries:
                    _sleep_exact(min(0.5 * (1 + random.random()), 1.0))
                    continue
                raise RuntimeError("响应解析失败（JSON）") from exc

            if after_each_sleep > 0:
                _sleep_exact(after_each_sleep)
            return payload
    finally:
        if close_session:
            own_session.close()


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


def _env_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _yahoo_allowed() -> bool:
    if os.getenv("DISABLE_YAHOO", "0") == "1":
        return False
    return _env_truthy(os.getenv("YFINANCE_FALLBACK"))


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


def _fetch_ai_rss_events(
    feeds: List[str],
) -> Tuple[List[Dict[str, Any]], List[FetchStatus]]:
    events: List[Dict[str, Any]] = []
    statuses: List[FetchStatus] = []
    for idx, url in enumerate(feeds, start=1):
        try:
            resp = requests.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
            )
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
            link_url = _rss_text(item, "link", "{*}link")
            if not link_url:
                link_node = item.find("link") or item.find("{*}link")
                if link_node is not None:
                    href = link_node.get("href")
                    if href:
                        link_url = href.strip()
                    elif link_node.text:
                        link_url = link_node.text.strip()
            feed_events.append(
                {
                    "title": title,
                    "date": normalized_date,
                    "impact": "medium",
                    "source": url,
                    "url": link_url or "",
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


def _fetch_arxiv_events(
    params: Dict[str, Any], throttle: float
) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    url = "https://export.arxiv.org/api/query"
    try:
        resp = requests.get(
            url,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(name="arxiv", ok=False, message=f"arXiv 请求失败: {exc}")

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        return [], FetchStatus(
            name="arxiv", ok=False, message=f"arXiv 响应解析失败: {exc}"
        )

    events: List[Dict[str, Any]] = []
    for entry in root.findall(".//{*}entry"):
        title = (_rss_text(entry, "{*}title") or "").replace("\n", " ").strip()
        if not title:
            title = "arXiv 更新"
        date_text = _rss_text(entry, "{*}updated", "{*}published")
        normalized_date = None
        if date_text:
            try:
                normalized_date = (
                    datetime.fromisoformat(date_text.replace("Z", "+00:00"))
                    .date()
                    .isoformat()
                )
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
        _sleep(throttle)

    return events, FetchStatus(
        name="arxiv", ok=True, message=f"arXiv 返回 {len(events)} 篇文章"
    )


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
        payload = _request_json(
            SOSOVALUE_INFLOW_URL,
            method="POST",
            json_body=body,
            headers=headers,
            policy=RetryPolicy(retries=2, backoff_start=0.8, hard_deadline=15.0),
        )
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(
            name="btc_etf_flow_sosovalue",
            ok=False,
            message=f"SoSoValue 请求失败: {exc}",
        )

    code = payload.get("code")
    if code not in (0, "0", 200, "200", None):
        message = payload.get("msg") or payload.get("message") or f"code={code}"
        return None, FetchStatus(
            name="btc_etf_flow_sosovalue",
            ok=False,
            message=f"SoSoValue 返回错误: {message}",
        )

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
        return None, FetchStatus(
            name="btc_etf_flow_sosovalue",
            ok=False,
            message="SoSoValue 响应缺少有效数据",
        )

    net_musd = latest_amount / 1_000_000.0
    return net_musd, FetchStatus(
        name="btc_etf_flow_sosovalue",
        ok=True,
        message=f"SoSoValue ETF 净流入已获取（{latest_day}）",
    )


def _fetch_coinglass_latest_flow(api_key: str) -> Tuple[Optional[float], FetchStatus]:
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json",
        "coinglassSecret": api_key,
    }
    params = {"page": 1, "size": 10}
    errors: List[str] = []
    policy = RetryPolicy(retries=2, backoff_start=0.7, hard_deadline=12.0)
    for url in COINGLASS_ETF_ENDPOINTS:
        try:
            payload = _request_json(url, params=params, headers=headers, policy=policy)
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

        net_amount = (
            latest_amount / 1_000_000.0
            if abs(latest_amount) > 100000
            else latest_amount
        )
        return net_amount, FetchStatus(
            name="btc_etf_flow_coinglass",
            ok=True,
            message=f"CoinGlass ETF 净流入已获取（{latest_day}）",
        )

    detail = "; ".join(errors) if errors else "未知原因"
    return None, FetchStatus(
        name="btc_etf_flow_coinglass", ok=False, message=f"CoinGlass 请求失败: {detail}"
    )


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
    for fetcher, label in (
        (
            _fetch_farside_flow_from_html,
            "html",
        ),
        (_fetch_farside_flow_from_api, "api"),
    ):
        try:
            amount = fetcher(session)
            return amount, FetchStatus(
                name="btc_etf_flow", ok=True, message=f"ETF 净流入读取成功（{label}）"
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{label}: {exc}")
    detail = "; ".join(errors) if errors else "未知原因"
    return None, FetchStatus(
        name="btc_etf_flow", ok=False, message=f"Farside 请求失败: {detail}"
    )


def _fetch_btc_etf_flow(
    api_keys: Dict[str, Any],
) -> Tuple[Optional[float], FetchStatus]:
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
        return None, FetchStatus(
            name="coinbase_spot", ok=False, message=f"Coinbase 请求失败: {exc}"
        )

    try:
        amount = float(payload["data"]["amount"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(
            name="coinbase_spot", ok=False, message=f"Coinbase 响应解析失败: {exc}"
        )

    return amount, FetchStatus(
        name="coinbase_spot", ok=True, message="Coinbase 现货价格已获取"
    )


def _fetch_okx_funding() -> Tuple[Optional[float], FetchStatus]:
    url = "https://www.okx.com/api/v5/public/funding-rate"
    try:
        payload = _request_json(url, params={"instId": "BTC-USD-SWAP"})
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(
            name="okx_funding", ok=False, message=f"OKX 请求失败: {exc}"
        )

    if payload.get("code") != "0":
        return None, FetchStatus(
            name="okx_funding", ok=False, message=f"OKX 返回错误: {payload.get('msg')}"
        )

    data = payload.get("data") or []
    if not data:
        return None, FetchStatus(
            name="okx_funding", ok=False, message="OKX 未返回资金费率"
        )

    try:
        rate = float(data[0]["fundingRate"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(
            name="okx_funding", ok=False, message=f"资金费率解析失败: {exc}"
        )

    return rate, FetchStatus(name="okx_funding", ok=True, message="OKX 资金费率已获取")


def _fetch_okx_basis(spot_price: float) -> Tuple[Optional[float], FetchStatus]:
    url = "https://www.okx.com/api/v5/market/ticker"
    try:
        payload = _request_json(url, params={"instId": "BTC-USD-SWAP"})
    except Exception as exc:  # noqa: BLE001
        return None, FetchStatus(
            name="okx_basis", ok=False, message=f"OKX ticker 请求失败: {exc}"
        )

    if payload.get("code") != "0":
        return None, FetchStatus(
            name="okx_basis", ok=False, message=f"OKX 返回错误: {payload.get('msg')}"
        )

    data = payload.get("data") or []
    if not data:
        return None, FetchStatus(
            name="okx_basis", ok=False, message="OKX 未返回永续价格"
        )

    try:
        last_price = float(data[0]["last"])
    except (KeyError, TypeError, ValueError) as exc:
        return None, FetchStatus(
            name="okx_basis", ok=False, message=f"永续价格解析失败: {exc}"
        )

    if spot_price <= 0:
        return None, FetchStatus(
            name="okx_basis", ok=False, message="现货价格无效，无法计算基差"
        )

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
        message = (
            payload.get("Information")
            or payload.get("Note")
            or "Alpha Vantage 未返回时间序列"
        )
        raise RuntimeError(message)
    return payload[key]


def _extract_close_change(
    series: Dict[str, Dict[str, str]],
) -> Tuple[str, float, float]:
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
    resp = requests.get(
        "https://stooq.com/q/d/l/",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    reader = csv.DictReader(resp.text.splitlines())
    rows: List[Dict[str, Any]] = []
    for row in reader:
        normalized = {
            k.strip().lower(): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
            if k
        }
        if normalized.get("date"):
            rows.append(normalized)
    if len(rows) < 2:
        raise RuntimeError("Stooq 未返回足够的时间序列")
    return rows


def _extract_latest_change(
    rows: List[Dict[str, Any]], *, close_key: str = "close"
) -> Tuple[str, float, float]:
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
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com/",
    }
    payload = _request_json(url, params=params, headers=headers)
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


def _attempt_quote(
    fetchers: Iterable[Tuple[str, Callable[[], _QuoteSnapshot]]],
) -> _QuoteSnapshot:
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
            return _QuoteSnapshot(
                day=day,
                close=round(close, 4),
                change_pct=round(change_pct, 4),
                source=f"stooq:{candidate}",
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{candidate}: {exc}")
    detail = "; ".join(errors) if errors else "Stooq 未返回数据"
    raise RuntimeError(detail)


def _fetch_quote_from_yahoo(symbol: str) -> _QuoteSnapshot:
    chart = _fetch_yahoo_chart(symbol)
    day, close, change_pct = _extract_yahoo_change(chart)
    return _QuoteSnapshot(
        day=day,
        close=round(close, 4),
        change_pct=round(change_pct, 4),
        source=f"yahoo:{symbol}",
    )


def _fetch_quote_from_fmp(symbol: str, api_key: str) -> _QuoteSnapshot:
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{quote(symbol)}"
    params = {"timeseries": 2, "apikey": api_key}
    payload = _request_json(url, params=params)
    history = payload.get("historical") or []
    if len(history) < 2:
        raise RuntimeError("FMP 未返回足够的历史数据")
    ordered = sorted(history, key=lambda item: item.get("date"), reverse=True)
    latest, prev = ordered[0], ordered[1]
    day = latest.get("date")
    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(prev.get("close"))
    if not day or close is None or prev_close in (None, 0):
        raise RuntimeError("FMP 历史数据缺字段")
    change_pct = (close - prev_close) / prev_close * 100
    return _QuoteSnapshot(
        day=day, close=round(close, 4), change_pct=round(change_pct, 4), source="fmp"
    )


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
    ordered = sorted(values, key=lambda item: item.get("datetime"), reverse=True)
    latest, prev = ordered[0], ordered[1]
    day = latest.get("datetime")
    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(prev.get("close"))
    if not day or close is None or prev_close in (None, 0):
        raise RuntimeError("Twelve Data 时间序列缺字段")
    change_pct = (close - prev_close) / prev_close * 100
    normalized_day = day.split(" ")[0] if isinstance(day, str) else str(day)
    return _QuoteSnapshot(
        day=normalized_day,
        close=round(close, 4),
        change_pct=round(change_pct, 4),
        source="twelve_data",
    )


def _fetch_quote_from_alpaca(symbol: str, key_id: str, secret: str) -> _QuoteSnapshot:
    params = {
        "symbols": symbol,
        "timeframe": "1Day",
        "limit": 2,
        "adjustment": "raw",
    }
    headers = {
        "APCA-API-KEY-ID": key_id,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }
    payload = _request_json(
        "https://data.alpaca.markets/v2/stocks/bars", params=params, headers=headers
    )
    bars_map = payload.get("bars") if isinstance(payload, dict) else None
    if not isinstance(bars_map, dict):
        raise RuntimeError("Alpaca 响应缺少 bars 字段")
    candidates = [symbol, symbol.upper(), symbol.lower()]
    bars: List[Dict[str, Any]] = []
    for key in candidates:
        bars = bars_map.get(key) or []
        if bars:
            break
    if len(bars) < 2:
        raise RuntimeError("Alpaca 未返回足够的时间序列")
    ordered = sorted(bars, key=lambda item: str(item.get("t")))
    prev, latest = ordered[-2], ordered[-1]
    close = _safe_float(latest.get("c"))
    prev_close = _safe_float(prev.get("c"))
    if close is None or prev_close in (None, 0):
        raise RuntimeError("Alpaca 时间序列缺少收盘价")
    raw_ts = str(latest.get("t"))
    day = raw_ts.split("T")[0] if "T" in raw_ts else raw_ts[:10]
    change_pct = (close - prev_close) / prev_close * 100
    return _QuoteSnapshot(
        day=day, close=round(close, 4), change_pct=round(change_pct, 4), source="alpaca"
    )


def _fetch_quote_from_alpha(symbol: str, api_key: str) -> _QuoteSnapshot:
    series = _fetch_alpha_series(symbol, api_key)
    day, close, change_pct = _extract_close_change(series)
    return _QuoteSnapshot(
        day=day,
        close=round(close, 4),
        change_pct=round(change_pct, 4),
        source="alpha_vantage",
    )


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


def _fetch_hk_market_snapshot(
    api_keys: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    errors: List[str] = []

    fetchers: List[Callable[[], Tuple[List[Dict[str, Any]], str]]] = [
        _fetch_hsi_from_stooq
    ]
    if _yahoo_allowed():
        fetchers.append(_fetch_hsi_from_yahoo)

    for fetcher in fetchers:
        try:
            rows, message = fetcher()
            return rows, FetchStatus(name="hongkong_HSI", ok=True, message=message)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{fetcher.__name__}: {exc}")

    if _yahoo_allowed():
        for proxy in HK_PROXY_SYMBOLS:
            try:
                rows, message = _fetch_hk_proxy_from_yahoo(proxy)
                return rows, FetchStatus(name="hongkong_HSI", ok=True, message=message)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{proxy}: {exc}")

    detail = "; ".join(errors) if errors else "未知原因"
    return [], FetchStatus(
        name="hongkong_HSI", ok=False, message=f"港股行情获取失败: {detail}"
    )


EDGAR_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
EDGAR_ALLOWED_FORMS = {
    "10-Q",
    "10-Q/A",
    "10-K",
    "10-K/A",
    "20-F",
    "40-F",
    "6-K",
    "6-K/A",
}
EDGAR_USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT",
    "DailyMessenger/0.1 (contact: [email protected])",
)
EDGAR_THROTTLE = 0.25


def _init_edgar_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": EDGAR_USER_AGENT,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
    )
    return session


def _edgar_request_json(session: requests.Session, url: str) -> Dict[str, Any]:
    try:
        payload = _request_json(
            url,
            session=session,
            headers={
                "User-Agent": EDGAR_USER_AGENT,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
            policy=RETRY_EDGAR,
            after_each_sleep=EDGAR_THROTTLE,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"EDGAR 请求失败: {exc}") from exc
    if not isinstance(payload, (dict, list)):
        raise RuntimeError("EDGAR 响应解析失败")
    return payload


_EDGAR_TICKER_CACHE: Optional[Dict[str, str]] = None


def _load_edgar_ticker_mapping(session: requests.Session) -> Dict[str, str]:
    global _EDGAR_TICKER_CACHE
    if _EDGAR_TICKER_CACHE is not None:
        return _EDGAR_TICKER_CACHE
    payload = _edgar_request_json(session, EDGAR_TICKER_URL)
    mapping: Dict[str, str] = {}
    if isinstance(payload, dict):
        values = payload.values()
    elif isinstance(payload, list):
        values = payload
    else:
        values = []
    for item in values:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").upper()
        cik_raw = item.get("cik_str")
        cik = None
        try:
            cik = f"{int(cik_raw):010d}"
        except (TypeError, ValueError):
            continue
        if ticker:
            mapping[ticker] = cik
    if not mapping:
        raise RuntimeError("EDGAR 代码映射为空")
    _EDGAR_TICKER_CACHE = mapping
    return mapping


def _fetch_edgar_companyfacts(session: requests.Session, cik: str) -> Dict[str, Any]:
    url = EDGAR_COMPANYFACTS_URL.format(cik=cik)
    payload = _edgar_request_json(session, url)
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        raise RuntimeError("EDGAR 返回缺少 facts 字段")
    return facts


def _edgar_select_fact(
    facts: Dict[str, Any], candidates: Iterable[Tuple[str, Iterable[str]]]
) -> Optional[Dict[str, Any]]:
    for taxonomy, names in candidates:
        bucket = facts.get(taxonomy)
        if not isinstance(bucket, dict):
            continue
        for name in names:
            fact = bucket.get(name)
            if isinstance(fact, dict):
                return fact
    return None


def _edgar_collect_entries(
    fact: Optional[Dict[str, Any]], unit_candidates: Iterable[str]
) -> List[Dict[str, Any]]:
    if not fact:
        return []
    units = fact.get("units")
    if not isinstance(units, dict):
        return []
    entries: List[Dict[str, Any]] = []
    for unit in unit_candidates:
        data = units.get(unit)
        if isinstance(data, list):
            entries.extend([item for item in data if isinstance(item, dict)])
    if not entries and units:
        first_key = next(iter(units))
        data = units.get(first_key)
        if isinstance(data, list):
            entries.extend([item for item in data if isinstance(item, dict)])
    return entries


def _edgar_parse_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value)


def _edgar_ttm_from_fact(fact: Optional[Dict[str, Any]]) -> Optional[float]:
    entries = _edgar_collect_entries(fact, ("USD",))
    quarterly: List[Tuple[str, float]] = []
    annual: List[Tuple[str, float]] = []
    for entry in entries:
        end = _edgar_parse_date(entry.get("end"))
        val = _safe_float(entry.get("val"))
        form = str(entry.get("form") or "").upper()
        fp = str(entry.get("fp") or "").upper()
        if end is None or val is None or form not in EDGAR_ALLOWED_FORMS:
            continue
        if fp.startswith("Q"):
            quarterly.append((end, val))
        else:
            start = entry.get("start")
            if isinstance(start, str):
                try:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                except ValueError:
                    annual.append((end, val))
                else:
                    duration = (end_dt - start_dt).days
                    if 70 <= duration <= 100:
                        quarterly.append((end, val))
                    else:
                        annual.append((end, val))
            else:
                annual.append((end, val))
    quarterly.sort(key=lambda item: item[0], reverse=True)
    if len(quarterly) >= 4:
        return sum(val for _, val in quarterly[:4])
    if annual:
        annual.sort(key=lambda item: item[0], reverse=True)
        return annual[0][1]
    if quarterly:
        return sum(val for _, val in quarterly)
    return None


def _edgar_latest_quarter_value(fact: Optional[Dict[str, Any]]) -> Optional[float]:
    entries = _edgar_collect_entries(fact, ("shares", "pure"))
    quarterly: List[Tuple[str, float]] = []
    for entry in entries:
        end = _edgar_parse_date(entry.get("end"))
        val = _safe_float(entry.get("val"))
        form = str(entry.get("form") or "").upper()
        fp = str(entry.get("fp") or "").upper()
        if end is None or val is None or form not in EDGAR_ALLOWED_FORMS:
            continue
        if fp.startswith("Q"):
            quarterly.append((end, val))
    quarterly.sort(key=lambda item: item[0], reverse=True)
    if quarterly:
        return quarterly[0][1]
    fallback = [
        (entry.get("end"), _safe_float(entry.get("val")))
        for entry in entries
        if _safe_float(entry.get("val")) is not None and entry.get("end")
    ]
    fallback.sort(key=lambda item: str(item[0]), reverse=True)
    return fallback[0][1] if fallback else None


def _edgar_latest_instant(fact: Optional[Dict[str, Any]]) -> Optional[float]:
    entries = _edgar_collect_entries(fact, ("USD",))
    instants: List[Tuple[str, float]] = []
    for entry in entries:
        end = _edgar_parse_date(entry.get("end"))
        val = _safe_float(entry.get("val"))
        form = str(entry.get("form") or "").upper()
        if end is None or val is None or form not in EDGAR_ALLOWED_FORMS:
            continue
        instants.append((end, val))
    instants.sort(key=lambda item: item[0], reverse=True)
    return instants[0][1] if instants else None


def _extract_edgar_metrics(facts: Dict[str, Any]) -> Dict[str, Optional[float]]:
    revenue_fact = _edgar_select_fact(
        facts,
        [
            (
                "us-gaap",
                [
                    "Revenues",
                    "SalesRevenueNet",
                    "RevenueFromContractWithCustomerExcludingAssessedTax",
                ],
            ),
            ("ifrs-full", ["Revenue", "RevenueFromContractsWithCustomers"]),
        ],
    )
    net_income_fact = _edgar_select_fact(
        facts,
        [
            ("us-gaap", ["NetIncomeLoss", "ProfitLoss"]),
            ("ifrs-full", ["ProfitLoss", "ProfitLossAttributableToOwnersOfParent"]),
        ],
    )
    shares_fact = _edgar_select_fact(
        facts,
        [
            (
                "us-gaap",
                [
                    "WeightedAverageNumberOfDilutedSharesOutstanding",
                    "DilutedEPSWeightedAverageSharesOutstanding",
                    "WeightedAverageNumberOfSharesOutstandingDiluted",
                    "WeightedAverageNumberOfSharesOutstanding",
                ],
            ),
            (
                "ifrs-full",
                [
                    "WeightedAverageDilutedSharesOutstanding",
                    "WeightedAverageShares",
                    "WeightedAverageNumberOfOrdinarySharesOutstanding",
                    "WeightedAverageNumberOfOrdinarySharesOutstandingDiluted",
                ],
            ),
        ],
    )
    equity_fact = _edgar_select_fact(
        facts,
        [
            (
                "us-gaap",
                [
                    "StockholdersEquity",
                    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                    "TotalEquity",
                    "Equity",
                ],
            ),
            (
                "ifrs-full",
                [
                    "Equity",
                    "EquityIncludingNoncontrollingInterests",
                    "EquityAttributableToOwnersOfParent",
                ],
            ),
        ],
    )

    revenue_ttm = _edgar_ttm_from_fact(revenue_fact)
    net_income_ttm = _edgar_ttm_from_fact(net_income_fact)
    shares_latest = _edgar_latest_quarter_value(shares_fact)
    equity_latest = _edgar_latest_instant(equity_fact)

    if not any(
        value is not None
        for value in (revenue_ttm, net_income_ttm, shares_latest, equity_latest)
    ):
        return {}

    return {
        "revenue_ttm": revenue_ttm,
        "net_income_ttm": net_income_ttm,
        "shares_diluted_latest": shares_latest,
        "equity_latest": equity_latest,
    }


def _extract_fmp_metrics(payload: Any) -> Dict[str, Optional[float]]:
    entries: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for key in ("metrics", "ttmMetrics", "data", "metric", "items"):
            block = payload.get(key)
            if isinstance(block, list):
                entries.extend(item for item in block if isinstance(item, dict))
            elif isinstance(block, dict):
                entries.append(block)
        if not entries and any(
            field in payload for field in ("revenueTTM", "netIncomeTTM")
        ):
            entries.append(payload)
    elif isinstance(payload, list):
        entries = [item for item in payload if isinstance(item, dict)]
    if not entries:
        return {}

    def _entry_date(entry: Dict[str, Any]) -> str:
        for key in ("date", "period", "fiscalDateEnding"):
            value = entry.get(key)
            if value:
                return str(value)
        return ""

    entries.sort(key=_entry_date, reverse=True)
    record = entries[0]
    lower_map = {str(k).lower(): v for k, v in record.items()}

    def pick(keys: Iterable[str]) -> Optional[float]:
        for key in keys:
            if key in record:
                value = record.get(key)
            else:
                value = record.get(key.lower())
            if value is None:
                value = lower_map.get(key.lower())
            if value is not None:
                number = _safe_float(value)
                if number is not None:
                    return number
        return None

    revenue = pick(
        [
            "revenueTTM",
            "RevenueTTM",
            "revenue_ttm",
        ]
    )
    net_income = pick(
        [
            "netIncomeTTM",
            "NetIncomeTTM",
            "net_income_ttm",
        ]
    )
    shares = pick(
        [
            "weightedAverageSharesDilutedTTM",
            "weightedAverageShsOutDilTTM",
            "weightedAverageShsOutDil",
            "WeightedAverageSharesDilutedTTM",
            "WeightedAverageShsOutDilTTM",
        ]
    )
    equity = pick(
        [
            "shareholdersEquityTTM",
            "ShareholdersEquityTTM",
            "totalShareholdersEquityTTM",
            "TotalShareholdersEquityTTM",
            "shareholdersequityttm",
        ]
    )
    if not any(val is not None for val in (revenue, net_income, shares, equity)):
        return {}
    return {
        "revenue_ttm": revenue,
        "net_income_ttm": net_income,
        "shares_diluted_latest": shares,
        "equity_latest": equity,
    }


def _fetch_fmp_fundamentals(
    symbols: Iterable[str],
    api_key: str,
) -> Tuple[Dict[str, Dict[str, Optional[float]]], List[str]]:
    results: Dict[str, Dict[str, Optional[float]]] = {}
    errors: List[str] = []
    for ticker in sorted({s.upper() for s in symbols if s}):
        url = (
            f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{quote(ticker)}"
        )
        params = {"apikey": api_key}
        try:
            payload = _request_json(url, params=params)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{ticker}: {exc}")
            continue
        metrics = _extract_fmp_metrics(payload)
        if metrics:
            results[ticker] = metrics
        else:
            errors.append(f"{ticker}: FMP 数据不足")
        _sleep(0.2)
    return results, errors


def _fetch_edgar_fundamentals(
    symbols: Iterable[str],
) -> Tuple[Dict[str, Dict[str, Optional[float]]], List[str], List[str]]:
    session = _init_edgar_session()
    mapping = _load_edgar_ticker_mapping(session)
    results: Dict[str, Dict[str, Optional[float]]] = {}
    missing: List[str] = []
    errors: List[str] = []
    for ticker in sorted({s.upper() for s in symbols if s}):
        cik = mapping.get(ticker)
        if not cik:
            missing.append(ticker)
            continue
        try:
            facts = _fetch_edgar_companyfacts(session, cik)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{ticker}: {exc}")
            continue
        metrics = _extract_edgar_metrics(facts)
        if metrics:
            results[ticker] = metrics
        else:
            errors.append(f"{ticker}: 财报数据不足")
    return results, missing, errors


def _edgar_healthcheck() -> FetchStatus:
    session = _init_edgar_session()
    try:
        mapping = _load_edgar_ticker_mapping(session)
        if not mapping:
            raise RuntimeError("EDGAR 代码映射为空")
        cik = mapping.get("AAPL")
        if not cik:
            try:
                cik = next(iter(mapping.values()))
            except StopIteration as exc:
                raise RuntimeError("EDGAR 代码映射缺失 CIK") from exc
        _fetch_edgar_companyfacts(session, cik)
        return FetchStatus(
            name="edgar", ok=True, message=f"EDGAR 正常（UA={EDGAR_USER_AGENT}）"
        )
    except Exception as exc:  # noqa: BLE001
        return FetchStatus(name="edgar", ok=False, message=f"{exc}")
    finally:
        session.close()


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
                "price": item.get("regularMarketPrice"),
            }
        if results:
            return results
    raise RuntimeError(f"Yahoo Finance 未返回报价: {last_error or 'empty'}")


def _fetch_price_only_quotes(
    symbols: List[str], api_keys: Optional[Dict[str, Any]] = None
) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    alpaca_key: Optional[str] = None
    alpaca_secret: Optional[str] = None
    if api_keys:
        alpaca_key = _coerce_api_key(api_keys.get("alpaca_key_id"))
        alpaca_secret = _coerce_api_key(api_keys.get("alpaca_secret"))
    allow_yahoo = _yahoo_allowed()
    for symbol in symbols:
        snapshot: Optional[_QuoteSnapshot] = None
        try:
            snapshot = _fetch_quote_from_stooq(symbol)
        except Exception:  # noqa: BLE001
            snapshot = None
        if not snapshot and alpaca_key and alpaca_secret:
            try:
                snapshot = _fetch_quote_from_alpaca(symbol, alpaca_key, alpaca_secret)
            except Exception:  # noqa: BLE001
                snapshot = None
        if not snapshot and allow_yahoo:
            try:
                snapshot = _fetch_quote_from_yahoo(symbol)
            except Exception:  # noqa: BLE001
                snapshot = None
        if not snapshot:
            continue
        results[symbol] = {
            "changesPercentage": snapshot.change_pct,
            "pe": None,
            "priceToSalesRatioTTM": None,
            "marketCap": None,
            "price": snapshot.close,
            "source": snapshot.source,
        }
        _sleep(0.2)
    if not results:
        raise RuntimeError("price-only fallback 无法获取任何报价")
    return results


def _mean(values: List[Optional[float]]) -> Optional[float]:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _fetch_theme_metrics_from_fmp(
    api_keys: Dict[str, Any],
) -> Tuple[Dict[str, Any], FetchStatus]:
    all_symbols: List[str] = []
    for symbols in FMP_THEME_SYMBOLS.values():
        all_symbols.extend(symbols)

    quotes: Dict[str, Dict[str, Any]] = {}
    source = ""
    errors: List[str] = []
    allow_yahoo = _yahoo_allowed()
    prefer_stooq_env = os.getenv("PREFER_STOOQ", "0") == "1"
    prefer_stooq = prefer_stooq_env or not allow_yahoo

    if prefer_stooq:
        try:
            quotes = _fetch_price_only_quotes(all_symbols, api_keys)
            source = "price_only"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"price_only: {exc}")
            if allow_yahoo:
                try:
                    quotes = _fetch_yahoo_quotes(all_symbols)
                    source = "yahoo"
                except Exception as fallback_exc:  # noqa: BLE001
                    errors.append(f"Yahoo: {fallback_exc}")
                    detail = "; ".join(errors) if errors else "未知原因"
                    return {}, FetchStatus(
                        name="fmp_theme",
                        ok=False,
                        message=f"主题估值获取失败: {detail}",
                    )
            else:
                detail = "; ".join(errors) if errors else "无可用行情来源"
                detail = f"{detail}；已禁用 Yahoo 兜底"
                return {}, FetchStatus(
                    name="fmp_theme", ok=False, message=f"主题估值获取失败: {detail}"
                )
    else:
        try:
            quotes = _fetch_yahoo_quotes(all_symbols)
            source = "yahoo"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Yahoo: {exc}")
            try:
                quotes = _fetch_price_only_quotes(all_symbols, api_keys)
                source = "price_only"
            except Exception as fallback_exc:  # noqa: BLE001
                errors.append(f"price_only: {fallback_exc}")
                detail = "; ".join(errors) if errors else "未知原因"
                return {}, FetchStatus(
                    name="fmp_theme", ok=False, message=f"主题估值获取失败: {detail}"
                )

    fundamentals: Dict[str, Dict[str, Optional[float]]] = {}
    missing_cik: List[str] = []
    edgar_errors: List[str] = []
    fmp_fallback_used = False
    fmp_fallback_errors: List[str] = []
    try:
        fundamentals, missing_cik, edgar_errors = _fetch_edgar_fundamentals(all_symbols)
    except Exception as exc:  # noqa: BLE001
        edgar_errors.append(str(exc))
    edgar_available = bool(fundamentals)

    fmp_key = _coerce_api_key(api_keys.get("financial_modeling_prep"))
    if fmp_key:
        needs_fmp: List[str] = []
        required_fields = (
            "net_income_ttm",
            "revenue_ttm",
            "shares_diluted_latest",
            "equity_latest",
        )
        for symbol in {sym.upper() for sym in all_symbols}:
            metrics = fundamentals.get(symbol)
            if not metrics:
                needs_fmp.append(symbol)
                continue
            if any(metrics.get(field) in (None, 0.0) for field in required_fields):
                needs_fmp.append(symbol)
        if needs_fmp:
            fmp_data, fallback_errors = _fetch_fmp_fundamentals(needs_fmp, fmp_key)
            if fmp_data:
                fmp_fallback_used = True
                for symbol, metrics in fmp_data.items():
                    merged = fundamentals.setdefault(symbol, {})
                    for key, value in metrics.items():
                        if value is None:
                            continue
                        existing = merged.get(key)
                        if existing in (None, 0.0):
                            merged[key] = value
            if fallback_errors:
                fmp_fallback_errors.extend(fallback_errors)

    quotes_norm = {symbol.upper(): payload for symbol, payload in quotes.items()}

    themes: Dict[str, Any] = {}
    for theme, symbols in FMP_THEME_SYMBOLS.items():
        change_values: List[Optional[float]] = []
        pe_values: List[Optional[float]] = []
        ps_values: List[Optional[float]] = []
        pb_values: List[Optional[float]] = []
        market_cap_total = 0.0
        symbol_rows: List[Dict[str, Any]] = []

        for symbol in symbols:
            quote = quotes_norm.get(symbol.upper())
            if not quote:
                continue
            quote_source = quote.get("source") or source or "unknown"
            change_val = _safe_float(quote.get("changesPercentage"))
            change_values.append(change_val)
            price = _safe_float(quote.get("price"))
            market_cap = _safe_float(quote.get("marketCap"))

            metrics = fundamentals.get(symbol.upper()) if fundamentals else None
            if (market_cap in (None, 0.0)) and (price is not None) and metrics:
                shares = _safe_float(metrics.get("shares_diluted_latest"))
                if shares not in (None, 0.0):
                    market_cap = float(price) * float(shares)
                    quote["marketCap"] = market_cap
            if market_cap not in (None, 0.0):
                market_cap_total += float(market_cap)

            computed_pe: Optional[float] = None
            computed_ps: Optional[float] = None
            computed_pb: Optional[float] = None
            if metrics:
                net_income = metrics.get("net_income_ttm")
                shares = metrics.get("shares_diluted_latest")
                revenue = metrics.get("revenue_ttm")
                equity = metrics.get("equity_latest")
                if (
                    price is not None
                    and net_income is not None
                    and shares not in (None, 0.0)
                ):
                    eps = net_income / shares if shares else None
                    if eps not in (None, 0.0):
                        computed_pe = price / eps
                if market_cap not in (None, 0.0) and revenue not in (None, 0.0):
                    computed_ps = float(market_cap) / float(revenue)
                if market_cap not in (None, 0.0) and equity not in (None, 0.0):
                    computed_pb = float(market_cap) / float(equity)

            if computed_pe is not None:
                pe_values.append(computed_pe)
            else:
                pe_values.append(_safe_float(quote.get("pe")))

            if computed_ps is not None:
                ps_values.append(computed_ps)
            else:
                ratio = quote.get("priceToSalesRatioTTM")
                if ratio is None:
                    ratio = quote.get("priceToSalesRatio")
                ps_values.append(_safe_float(ratio))

            if computed_pb is not None:
                pb_values.append(computed_pb)

            pe_value = (
                computed_pe if computed_pe is not None else _safe_float(quote.get("pe"))
            )
            ratio = quote.get("priceToSalesRatioTTM")
            if ratio is None:
                ratio = quote.get("priceToSalesRatio")
            ps_value = computed_ps if computed_ps is not None else _safe_float(ratio)
            pb_value = computed_pb

            symbol_rows.append(
                {
                    "symbol": symbol,
                    "price": round(price, 2)
                    if isinstance(price, (int, float))
                    else None,
                    "change_pct": round(change_val, 2)
                    if isinstance(change_val, (int, float))
                    else None,
                    "pe": round(pe_value, 2)
                    if isinstance(pe_value, (int, float))
                    else None,
                    "ps": round(ps_value, 2)
                    if isinstance(ps_value, (int, float))
                    else None,
                    "pb": round(pb_value, 2)
                    if isinstance(pb_value, (int, float))
                    else None,
                    "market_cap": round(market_cap, 2)
                    if isinstance(market_cap, (int, float))
                    else None,
                    "source": quote_source,
                }
            )

        if not change_values and not pe_values and not ps_values and not pb_values:
            continue

        change_avg = _mean(change_values)
        pe_avg = _mean(pe_values)
        ps_avg = _mean(ps_values)
        pb_avg = _mean(pb_values)

        themes[theme] = {
            "change_pct": round(change_avg, 2) if change_avg is not None else None,
            "avg_pe": round(pe_avg, 2) if pe_avg is not None else None,
            "avg_ps": round(ps_avg, 2) if ps_avg is not None else None,
            "avg_pb": round(pb_avg, 2) if pb_avg is not None else None,
            "market_cap": round(market_cap_total, 2) if market_cap_total else None,
            "symbols": symbol_rows,
        }

    if not themes:
        return {}, FetchStatus(name="fmp_theme", ok=False, message="主题估值数据为空")

    fundamentals_present = bool(fundamentals)
    if fundamentals_present:
        if edgar_available and fmp_fallback_used:
            fundamentals_label = "EDGAR + FMP 财报"
        elif edgar_available:
            fundamentals_label = "EDGAR 财报"
        elif fmp_fallback_used:
            fundamentals_label = "FMP 财报"
        else:
            fundamentals_label = "财报数据"
    else:
        fundamentals_label = ""

    if source == "price_only":
        status_ok = True
        fallback_providers = sorted(
            {
                str(payload.get("source") or "price_only").split(":", 1)[0]
                for payload in quotes.values()
            }
        )
        fallback_label = (
            " + ".join(fallback_providers) if fallback_providers else "价格兜底"
        )
        issue_items: List[str] = []
        if missing_cik:
            issue_items.append(f"缺少CIK: {', '.join(sorted(missing_cik))}")
        issue_items.extend(edgar_errors)
        issue_items.extend(fmp_fallback_errors)
        if fundamentals_present and fundamentals_label:
            message = f"主题估值使用 {fallback_label} 报价兜底 + {fundamentals_label}"
        elif fundamentals_present:
            message = f"主题估值使用 {fallback_label} 报价兜底 + 财报数据"
        else:
            message = f"主题估值使用 {fallback_label} 报价兜底，仅含行情字段"
        detail_parts: List[str] = []
        if errors:
            detail_parts.append("; ".join(errors))
        if issue_items:
            if len(issue_items) > 2:
                detail_parts.append(
                    "; ".join(issue_items[:2]) + f" 等 {len(issue_items)} 项"
                )
            else:
                detail_parts.append("; ".join(issue_items))
        if detail_parts:
            detail = "；".join(part for part in detail_parts if part)
            message = f"{message}（{detail}）"
    else:
        status_ok = True
        issue_items: List[str] = []
        if missing_cik:
            issue_items.append(f"缺少CIK: {', '.join(sorted(missing_cik))}")
        issue_items.extend(edgar_errors)
        issue_items.extend(fmp_fallback_errors)
        detail = ""
        if issue_items:
            if len(issue_items) > 2:
                detail = "; ".join(issue_items[:2]) + f" 等 {len(issue_items)} 项"
            else:
                detail = "; ".join(issue_items)
        if fundamentals_present and fundamentals_label:
            if detail:
                message = f"主题估值使用 Yahoo 行情 + {fundamentals_label}（{detail}）"
            else:
                message = f"主题估值使用 Yahoo 行情 + {fundamentals_label}"
        elif fundamentals_present:
            if detail:
                message = f"主题估值使用 Yahoo 行情 + 财报数据（{detail}）"
            else:
                message = "主题估值使用 Yahoo 行情 + 财报数据"
        else:
            status_ok = False
            if detail:
                message = f"主题估值缺少 EDGAR 财报，仅使用行情（{detail}）"
            else:
                message = "主题估值缺少 EDGAR 财报，仅使用行情"

    return themes, FetchStatus(name="fmp_theme", ok=status_ok, message=message)


def _resolve_index_quote(symbol: str, api_keys: Dict[str, Any]) -> _QuoteSnapshot:
    order_env = os.getenv("QUOTE_ORDER", "")
    default_order = ["stooq", "financial_modeling_prep", "twelve_data", "alpha_vantage"]
    wished = [
        item.strip().lower() for item in order_env.split(",") if item.strip()
    ] or default_order
    fmp_key = _coerce_api_key(api_keys.get("financial_modeling_prep"))
    twelve_key = _coerce_api_key(api_keys.get("twelve_data"))
    alpha_key = _coerce_api_key(api_keys.get("alpha_vantage"))
    allow_yahoo = _yahoo_allowed()

    fetchers: List[Tuple[str, Callable[[], _QuoteSnapshot]]] = []
    added: set[str] = set()
    for name in wished:
        if name == "stooq" and "stooq" not in added:
            fetchers.append(("stooq", lambda: _fetch_quote_from_stooq(symbol)))
            added.add("stooq")
        elif name == "financial_modeling_prep" and fmp_key and "fmp" not in added:
            fetchers.append(
                ("fmp", lambda key=fmp_key: _fetch_quote_from_fmp(symbol, key))
            )
            added.add("fmp")
        elif name == "twelve_data" and twelve_key and "twelve_data" not in added:
            fetchers.append(
                (
                    "twelve_data",
                    lambda key=twelve_key: _fetch_quote_from_twelve_data(symbol, key),
                )
            )
            added.add("twelve_data")
        elif name == "alpha_vantage" and alpha_key and "alpha_vantage" not in added:
            fetchers.append(
                (
                    "alpha_vantage",
                    lambda key=alpha_key: _fetch_quote_from_alpha(symbol, key),
                )
            )
            added.add("alpha_vantage")

    if allow_yahoo:
        fetchers.append(("yahoo", lambda: _fetch_quote_from_yahoo(symbol)))
    return _attempt_quote(fetchers)


def _resolve_equity_quote(symbol: str, api_keys: Dict[str, Any]) -> _QuoteSnapshot:
    order_env = os.getenv("QUOTE_ORDER", "")
    default_order = [
        "financial_modeling_prep",
        "twelve_data",
        "stooq",
        "alpaca",
        "alpha_vantage",
    ]
    wished = [
        item.strip().lower() for item in order_env.split(",") if item.strip()
    ] or default_order

    fmp_key = _coerce_api_key(api_keys.get("financial_modeling_prep"))
    twelve_key = _coerce_api_key(api_keys.get("twelve_data"))
    alpha_key = _coerce_api_key(api_keys.get("alpha_vantage"))
    alpaca_key = _coerce_api_key(api_keys.get("alpaca_key_id"))
    alpaca_secret = _coerce_api_key(api_keys.get("alpaca_secret"))
    allow_yahoo = _yahoo_allowed()

    fetchers: List[Tuple[str, Callable[[], _QuoteSnapshot]]] = []
    added: set[str] = set()
    for name in wished:
        if name == "financial_modeling_prep" and fmp_key and "fmp" not in added:
            fetchers.append(
                ("fmp", lambda key=fmp_key: _fetch_quote_from_fmp(symbol, key))
            )
            added.add("fmp")
        elif name == "twelve_data" and twelve_key and "twelve_data" not in added:
            fetchers.append(
                (
                    "twelve_data",
                    lambda key=twelve_key: _fetch_quote_from_twelve_data(symbol, key),
                )
            )
            added.add("twelve_data")
        elif name == "stooq" and "stooq" not in added:
            fetchers.append(("stooq", lambda: _fetch_quote_from_stooq(symbol)))
            added.add("stooq")
        elif (
            name == "alpaca" and alpaca_key and alpaca_secret and "alpaca" not in added
        ):
            fetchers.append(
                (
                    "alpaca",
                    lambda key=alpaca_key,
                    secret=alpaca_secret: _fetch_quote_from_alpaca(symbol, key, secret),
                )
            )
            added.add("alpaca")
        elif name == "alpha_vantage" and alpha_key and "alpha_vantage" not in added:
            fetchers.append(
                (
                    "alpha_vantage",
                    lambda key=alpha_key: _fetch_quote_from_alpha(symbol, key),
                )
            )
            added.add("alpha_vantage")

    if allow_yahoo:
        fetchers.append(("yahoo", lambda: _fetch_quote_from_yahoo(symbol)))
    return _attempt_quote(fetchers)


def _fetch_market_snapshot_real(
    api_keys: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], FetchStatus]:
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
        indices.append(
            {
                "symbol": label,
                "close": round(snapshot.close, 2),
                "change_pct": round(snapshot.change_pct, 2),
            }
        )
        index_sources[label] = snapshot.source

    sectors: List[Dict[str, Any]] = []
    sector_sources: Dict[str, str] = {}
    for name, proxy in SECTOR_PROXIES.items():
        try:
            snapshot = _resolve_equity_quote(proxy, api_keys)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")
            continue
        sectors.append(
            {"name": name, "performance": round(1 + snapshot.change_pct / 100, 3)}
        )
        sector_sources[name] = snapshot.source

    if not indices:
        detail = "; ".join(errors) if errors else "无可用行情来源"
        return None, FetchStatus(
            name="market", ok=False, message=f"美股行情获取失败: {detail}"
        )

    market = {
        "date": latest_date or _current_trading_day(),
        "indices": indices,
        "sectors": sectors,
    }

    parts: List[str] = []
    if index_sources:
        formatted = ", ".join(
            f"{symbol}:{src}" for symbol, src in index_sources.items()
        )
        parts.append(f"指数来源 {formatted}")
    if sector_sources:
        formatted = ", ".join(f"{name}:{src}" for name, src in sector_sources.items())
        parts.append(f"板块来源 {formatted}")
    if errors:
        parts.append(f"降级 {len(errors)} 项")
    message = "；".join(parts) if parts else "市场行情已获取"
    return market, FetchStatus(name="market", ok=True, message=message)


def _fetch_events_real(
    trading_day: str, api_keys: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    credential = api_keys.get("trading_economics", TE_GUEST_CREDENTIAL)
    params = {"c": credential, "format": "json"}
    try:
        payload = _request_json(
            "https://api.tradingeconomics.com/calendar", params=params
        )
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(
            name="events", ok=False, message=f"Trading Economics 请求失败: {exc}"
        )

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
        return [], FetchStatus(
            name="events", ok=False, message="Trading Economics 未返回可用事件"
        )

    return events, FetchStatus(
        name="events", ok=True, message="Trading Economics 事件日历已获取"
    )


def _fetch_finnhub_earnings(
    trading_day: str, api_keys: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], FetchStatus]:
    api_key = api_keys.get("finnhub")
    if not api_key:
        return [], FetchStatus(
            name="finnhub_earnings", ok=False, message="缺少 Finnhub API Key"
        )

    start = datetime.strptime(trading_day, "%Y-%m-%d").date()
    end = start + timedelta(days=5)
    params = {
        "from": trading_day,
        "to": end.strftime("%Y-%m-%d"),
        "token": api_key,
    }
    try:
        payload = _request_json(
            "https://finnhub.io/api/v1/calendar/earnings", params=params
        )
    except Exception as exc:  # noqa: BLE001
        return [], FetchStatus(
            name="finnhub_earnings", ok=False, message=f"Finnhub 请求失败: {exc}"
        )

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
        return [], FetchStatus(
            name="finnhub_earnings", ok=False, message="Finnhub 未返回财报事件"
        )

    events.sort(key=lambda item: item["date"])
    return events, FetchStatus(
        name="finnhub_earnings", ok=True, message="Finnhub 财报日历已获取"
    )


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
            {
                "symbol": "SPX",
                "close": round(index_level, 2),
                "change_pct": round((seed % 5 - 2) * 0.3, 2),
            },
            {
                "symbol": "NDX",
                "close": round(index_level * 1.2, 2),
                "change_pct": round((seed % 3 - 1) * 0.4, 2),
            },
        ],
        "sectors": [
            {"name": "AI", "performance": round(ai_sector_perf, 2)},
            {"name": "Defensive", "performance": round(defensive_sector_perf, 2)},
        ],
        "hk_indices": [
            {
                "symbol": "HSI",
                "close": 18000 + seed % 200,
                "change_pct": round(hk_change, 2),
            },
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
    today = datetime.now(PACIFIC_TZ)
    events = [
        {
            "title": "FOMC 会议纪要发布",
            "date": trading_day,
            "impact": "high",
        },
        {
            "title": "大型科技财报",
            "date": (
                today.replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=2)
            ).strftime("%Y-%m-%d"),
            "impact": "medium",
        },
    ]
    status = FetchStatus(name="events", ok=True, message="事件日历已生成")
    return events, status


def run(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制刷新当日数据")
    args = parser.parse_args(argv)

    logger = setup_logger("etl")
    started_at = datetime.now(timezone.utc)

    _ensure_out_dir()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    trading_day = _current_trading_day()
    logger = setup_logger("etl", trading_day=trading_day)
    log(logger, logging.INFO, "etl_start", force=args.force)
    run_meta.record_step(
        OUT_DIR, "etl", "started", trading_day=trading_day, force=args.force
    )

    raw_market_path = OUT_DIR / "raw_market.json"
    raw_events_path = OUT_DIR / "raw_events.json"
    status_path = OUT_DIR / "etl_status.json"
    marker = STATE_DIR / f"fetch_{trading_day}"

    if not args.force:
        skip_run = marker.exists()
        if skip_run and not (
            raw_market_path.exists()
            and raw_events_path.exists()
            and status_path.exists()
        ):
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
            log(logger, logging.INFO, "etl_skip_cached")
            run_meta.record_step(OUT_DIR, "etl", "cached", trading_day=trading_day)
            return 0

    log(logger, logging.INFO, "etl_execute", trading_day=trading_day)

    api_keys, ai_feeds, arxiv_params, arxiv_throttle = _load_configuration(logger)
    global API_KEYS_CACHE, AI_NEWS_FEEDS, ARXIV_QUERY_PARAMS, ARXIV_THROTTLE
    API_KEYS_CACHE = api_keys
    AI_NEWS_FEEDS = ai_feeds
    ARXIV_QUERY_PARAMS = arxiv_params
    ARXIV_THROTTLE = arxiv_throttle

    if not api_keys:
        log(logger, logging.WARNING, "etl_missing_api_keys")

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
        sectors = (
            market_data.get("sectors", []) if isinstance(market_data, dict) else []
        )
        ai_perf = next(
            (s.get("performance") for s in sectors if s.get("name") == "AI"), None
        )
        if ai_perf is not None:
            theme_metrics.setdefault("ai", {})["performance"] = ai_perf

    themes, theme_status = _fetch_theme_metrics_from_fmp(api_keys)
    statuses.append(theme_status)
    if theme_status.ok:
        for name, metrics in themes.items():
            theme_metrics.setdefault(name, {}).update(metrics)
    else:
        overall_ok = False

    edgar_status = _edgar_healthcheck()
    statuses.append(edgar_status)
    if not edgar_status.ok:
        overall_ok = False

    if market_data is not None and theme_metrics:
        market_data.setdefault("themes", {}).update(theme_metrics)

    sentiment_data: Dict[str, Any] = {}

    put_call_payload, put_call_status = cboe_putcall.fetch()
    statuses.append(put_call_status)
    if getattr(put_call_status, "ok", False) and put_call_payload:
        sentiment_data.update(put_call_payload)
    else:
        overall_ok = False
        previous_put_call = (
            previous_sentiment.get("put_call")
            if isinstance(previous_sentiment, dict)
            else None
        )
        if isinstance(previous_put_call, dict):
            sentiment_data["put_call"] = previous_put_call
            statuses.append(
                FetchStatus(
                    name="cboe_put_call_fallback",
                    ok=True,
                    message="使用上一期 Put/Call 数据",
                )
            )

    aaii_payload, aaii_status = aaii_sentiment.fetch()
    statuses.append(aaii_status)
    if getattr(aaii_status, "ok", False) and aaii_payload:
        sentiment_data.update(aaii_payload)
    else:
        overall_ok = False
        previous_aaii = (
            previous_sentiment.get("aaii")
            if isinstance(previous_sentiment, dict)
            else None
        )
        if isinstance(previous_aaii, dict):
            sentiment_data["aaii"] = previous_aaii
            statuses.append(
                FetchStatus(
                    name="aaii_sentiment_fallback",
                    ok=True,
                    message="使用上一期 AAII 数据",
                )
            )

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
        statuses.append(
            FetchStatus(
                name="okx_basis", ok=False, message="缺少现货价格，无法计算基差"
            )
        )

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
    if (
        spot_price is not None
        and funding_rate is not None
        and basis is not None
        and etf_flow is not None
    ):
        btc_data = {
            "date": trading_day,
            "spot_price_usd": round(spot_price, 2),
            "perpetual_price_usd": round(spot_price * (1 + basis), 2)
            if basis is not None
            else None,
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
            finnhub_events, finnhub_status = _fetch_finnhub_earnings(
                trading_day, api_keys
            )
            statuses.append(finnhub_status)
            if finnhub_status.ok:
                events.extend(finnhub_events)
        elif api_keys:
            statuses.append(
                FetchStatus(
                    name="finnhub_earnings", ok=False, message="缺少 Finnhub API Key"
                )
            )

    ai_updates: List[Dict[str, Any]] = []
    if ai_feeds:
        ai_events, feed_statuses = _fetch_ai_rss_events(ai_feeds)
        statuses.extend(feed_statuses)
        if ai_events:
            events.extend(ai_events)
            ai_updates.extend(ai_events)

    gemini_updates, gemini_statuses = _fetch_gemini_market_news(
        datetime.now(timezone.utc), api_keys, logger
    )
    statuses.extend(gemini_statuses)
    if gemini_updates:
        ai_updates.extend(gemini_updates)

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
        json.dump(
            {"events": events, "ai_updates": ai_updates},
            f,
            ensure_ascii=False,
            indent=2,
        )

    status_payload = {
        "date": trading_day,
        "sources": [asdict(s) for s in statuses],
        "ok": overall_ok,
    }
    with status_path.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, ensure_ascii=False, indent=2)

    for entry in statuses:
        level = logging.INFO if entry.ok else logging.WARNING
        log(
            logger,
            level,
            "etl_source_status",
            source=entry.name,
            ok=entry.ok,
            detail=entry.message,
        )

    if not status_payload["ok"]:
        log(
            logger,
            logging.WARNING,
            "etl_degraded",
            reason="one or more fetchers failed",
        )

    marker.touch(exist_ok=True)

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    log(
        logger,
        logging.INFO,
        "etl_complete",
        degraded=not status_payload["ok"],
        duration_seconds=round(duration, 2),
        sources=len(statuses),
    )
    run_meta.record_step(
        OUT_DIR,
        "etl",
        "completed",
        trading_day=trading_day,
        degraded=not status_payload["ok"],
        duration_seconds=round(duration, 2),
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
