"""Fetch Cboe daily put/call ratios."""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, Tuple

import requests
from zoneinfo import ZoneInfo

USER_AGENT = "daily-messenger-bot/0.1"
REQUEST_TIMEOUT = 15
URL = "https://www.cboe.com/us/options/market_statistics/daily/"
EXCHANGE_TZ = ZoneInfo("America/Chicago")

LABELS = {
    "equity": "EQUITY PUT/CALL RATIO",
    "index": "INDEX PUT/CALL RATIO",
    "spx_spxw": "SPX + SPXW PUT/CALL RATIO",
    "vix": "CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO",
}


@dataclass
class FetchStatus:
    name: str
    ok: bool
    message: str = ""


class _TextExtractor(HTMLParser):
    """Minimal HTML text extractor."""

    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        text = data.strip()
        if text:
            self._chunks.append(text)

    def get_text(self) -> str:
        return " ".join(self._chunks)


def _extract_numbers(text: str) -> Dict[str, float]:
    results: Dict[str, float] = {}
    for key, label in LABELS.items():
        pattern = re.compile(rf"{re.escape(label)}[^0-9]*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            try:
                results[key] = float(match.group(1))
            except ValueError:
                continue
    return results


def fetch() -> Tuple[Dict[str, Dict[str, object]], FetchStatus]:
    """Fetch put/call ratios from the Cboe daily statistics page."""

    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(URL, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return {}, FetchStatus(name="cboe_put_call", ok=False, message=f"Cboe 请求失败: {exc}")

    parser = _TextExtractor()
    parser.feed(response.text)
    text = parser.get_text()
    ratios = _extract_numbers(text)

    if not ratios:
        return {}, FetchStatus(name="cboe_put_call", ok=False, message="未能解析 Put/Call 数据")

    now_ct = dt.datetime.now(tz=EXCHANGE_TZ)
    payload: Dict[str, Dict[str, object]] = {
        "put_call": {
            "as_of_exchange_tz": "America/Chicago",
            "as_of_utc": now_ct.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
            "source": "cboe_daily_market_statistics",
        }
    }
    payload["put_call"].update(ratios)

    return payload, FetchStatus(
        name="cboe_put_call",
        ok=True,
        message="Cboe Put/Call 数据已更新",
    )
