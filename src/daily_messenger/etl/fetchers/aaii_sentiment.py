"""Fetch AAII weekly investor sentiment survey results."""
from __future__ import annotations

import datetime as dt
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import requests

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
REQUEST_TIMEOUT = 20
RSS_URL = "https://insights.aaii.com/feed"


@dataclass
class FetchStatus:
    name: str
    ok: bool
    message: str = ""


def _init_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def _resolve_latest_story(session: requests.Session) -> Optional[str]:
    try:
        response = session.get(RSS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception:  # noqa: BLE001
        return None
    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        return None
    channel = root.find("channel")
    if channel is None:
        return None
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        if "Sentiment Survey" in title:
            return link
    return None


def _parse_article(html: str) -> Optional[Dict[str, float]]:
    pattern = re.compile(
        r"(?is)Bullish[^0-9]*([0-9]+(?:\.[0-9]+)?)%.*?Neutral[^0-9]*([0-9]+(?:\.[0-9]+)?)%.*?Bearish[^0-9]*([0-9]+(?:\.[0-9]+)?)%"
    )
    match = pattern.search(html)
    if not match:
        return None
    try:
        bull, neutral, bear = (float(match.group(i)) for i in range(1, 4))
    except ValueError:
        return None
    return {
        "bullish_pct": round(bull, 2),
        "neutral_pct": round(neutral, 2),
        "bearish_pct": round(bear, 2),
        "bull_bear_spread": round(bull - bear, 2),
    }


def _parse_week(html: str) -> Optional[str]:
    date_pattern = re.compile(
        r"(?i)(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    )
    match = date_pattern.search(html)
    if not match:
        return None
    try:
        parsed = dt.datetime.strptime(match.group(0), "%B %d, %Y")
    except ValueError:
        return None
    return parsed.date().isoformat()


def fetch() -> Tuple[Dict[str, Dict[str, object]], FetchStatus]:
    session = _init_session()
    link = _resolve_latest_story(session)
    if not link:
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message="未能定位最新情绪文章")

    try:
        response = session.get(link, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message=f"AAII 请求失败: {exc}")

    metrics = _parse_article(response.text)
    if metrics is None:
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message="AAII 页面缺少情绪百分比")

    week = _parse_week(response.text) or dt.date.today().isoformat()

    payload: Dict[str, Dict[str, object]] = {
        "aaii": {
            "week": week,
            **metrics,
            "source": "aaii_insights_substack",
            "link": link,
        }
    }

    return payload, FetchStatus(name="aaii_sentiment", ok=True, message="AAII 情绪数据已更新")
