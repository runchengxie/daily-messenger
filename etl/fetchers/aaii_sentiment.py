"""Fetch AAII weekly investor sentiment survey results."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple

import requests

USER_AGENT = "daily-messenger-bot/0.1"
REQUEST_TIMEOUT = 15
URL = "https://www.aaii.com/sentimentsurvey/sent_results"


@dataclass
class FetchStatus:
    name: str
    ok: bool
    message: str = ""


class _TableCollector(HTMLParser):
    """Extract tables from AAII HTML."""

    def __init__(self) -> None:
        super().__init__()
        self._in_table = False
        self._capture = False
        self._buffer: List[str] = []
        self._current_row: List[str] = []
        self.tables: List[List[List[str]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:  # type: ignore[override]
        if tag == "table":
            self._in_table = True
            self.tables.append([])
        elif self._in_table and tag in {"tr"}:
            self._current_row = []
        elif self._in_table and tag in {"td", "th"}:
            self._capture = True
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if not self._in_table:
            return
        if tag in {"td", "th"} and self._capture:
            text = "".join(self._buffer).strip()
            self._current_row.append(text)
            self._capture = False
        elif tag == "tr" and self._current_row:
            self.tables[-1].append(self._current_row)
            self._current_row = []
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._capture:
            self._buffer.append(data)


def _normalize_header(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _parse_float(value: str) -> Optional[float]:
    text = value.strip().replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: str) -> Optional[str]:
    candidates = ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]
    text = value.strip()
    for fmt in candidates:
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _select_table(tables: List[List[List[str]]]) -> Optional[List[List[str]]]:
    for table in tables:
        if not table:
            continue
        header = table[0]
        normalized = {_normalize_header(col) for col in header}
        required = {"bullish", "neutral", "bearish"}
        if required.issubset(normalized):
            return table
    return None


def fetch() -> Tuple[Dict[str, Dict[str, object]], FetchStatus]:
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(URL, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message=f"AAII 请求失败: {exc}")

    parser = _TableCollector()
    parser.feed(response.text)
    table = _select_table(parser.tables)
    if not table or len(table) <= 1:
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message="未找到 AAII 情绪表格")

    header = table[0]
    rows = table[1:]
    header_map = {_normalize_header(col): idx for idx, col in enumerate(header)}

    latest_row = next((row for row in rows if len(row) >= len(header)), None)
    if latest_row is None:
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message="AAII 表格没有有效数据行")

    bull = _parse_float(latest_row[header_map["bullish"]])
    neutral = _parse_float(latest_row[header_map["neutral"]])
    bear = _parse_float(latest_row[header_map["bearish"]])

    if bull is None or neutral is None or bear is None:
        return {}, FetchStatus(name="aaii_sentiment", ok=False, message="AAII 数据缺少百分比")

    date_key = next((key for key in ("reporteddate", "week", "date") if key in header_map), None)
    week_value = latest_row[header_map[date_key]] if date_key else ""
    week = _parse_date(week_value) or week_value[:10] or dt.date.today().isoformat()

    spread = round(bull - bear, 2)
    payload: Dict[str, Dict[str, object]] = {
        "aaii": {
            "week": week,
            "bullish_pct": round(bull, 2),
            "neutral_pct": round(neutral, 2),
            "bearish_pct": round(bear, 2),
            "bull_bear_spread": spread,
            "source": "aaii_sentiment_survey",
        }
    }

    return payload, FetchStatus(name="aaii_sentiment", ok=True, message="AAII 情绪数据已更新")
