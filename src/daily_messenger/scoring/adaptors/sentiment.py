"""Sentiment adaptor that normalizes raw survey and options data."""
from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Mapping, Sequence

_NEUTRAL_SCORE = 50.0


@dataclass
class SentimentResult:
    score: float
    components: Dict[str, float]

    def to_dict(self) -> Dict[str, float]:
        payload = {"score": round(self.score, 2)}
        for key, value in self.components.items():
            payload[key] = round(value, 2)
        return payload


def _safe_series(values: Iterable[float | None]) -> List[float]:
    cleaned: List[float] = []
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isnan(number) or math.isinf(number):
            continue
        cleaned.append(number)
    return cleaned


def _z_score(latest: float, series: Sequence[float]) -> float:
    if not series:
        return 0.0
    if len(series) == 1:
        return 0.0
    mu = mean(series)
    sigma = pstdev(series)
    if sigma == 0:
        return 0.0
    return (latest - mu) / sigma


def _compress(value: float) -> float:
    # Bound the value between -1 and 1 to avoid overweighting.
    return math.tanh(value / 2)


def _score_put_call(series: Sequence[float]) -> float:
    if not series:
        return 0.0
    positive = [val for val in series if val and val > 0]
    if not positive:
        return 0.0
    log_series = [math.log(val) for val in positive]
    z = _z_score(log_series[-1], log_series)
    # Panic (high put/call) is contrarian bullish.
    return -_compress(z)


def _score_aaii(series: Sequence[float]) -> float:
    if not series:
        return 0.0
    z = _z_score(series[-1], series)
    # Extreme optimism is bearish, pessimism is bullish.
    return -_compress(z)


def aggregate(sentiment_node: Mapping[str, object], history: Mapping[str, Sequence[float]]) -> SentimentResult | None:
    """Aggregate sentiment signals into a 0-100 score."""

    components: Dict[str, float] = {}

    put_call_data = sentiment_node.get("put_call") if isinstance(sentiment_node, Mapping) else None
    put_call_series = _safe_series(history.get("put_call_equity", []))
    if put_call_series and isinstance(put_call_data, Mapping):
        components["put_call"] = _score_put_call(put_call_series)

    aaii_data = sentiment_node.get("aaii") if isinstance(sentiment_node, Mapping) else None
    aaii_series = _safe_series(history.get("aaii_bull_bear_spread", []))
    if aaii_series and isinstance(aaii_data, Mapping):
        components["aaii"] = _score_aaii(aaii_series)

    if not components:
        return None

    combined = sum(components.values()) / len(components)
    score = _NEUTRAL_SCORE + 50.0 * combined
    score = max(0.0, min(100.0, score))
    return SentimentResult(score=score, components={k: 50.0 + 50.0 * v for k, v in components.items()})
