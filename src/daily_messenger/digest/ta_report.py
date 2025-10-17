#!/usr/bin/env python3
"""
Generate technical analysis report for commodities (default XAU/USD) using OANDA data.

This module keeps dependencies lightweight by relying on the standard library only.
It pulls OANDA midpoint candles, computes SMA/RSI/ATR plus daily pivot levels,
and renders a Markdown report that mirrors the existing digest structure.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

import requests
import yaml


@dataclass
class Candle:
    """Minimal representation of an OHLC candle."""

    time: datetime
    complete: bool
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class WindowsConfig:
    sma_fast: int
    sma_slow: int
    rsi: int
    atr: int


@dataclass
class ThresholdConfig:
    rsi_overbought: float
    rsi_oversold: float
    near_pct: float


@dataclass
class ReportOutputConfig:
    filename: str
    include_intraday: bool
    intraday_granularities: List[str]


@dataclass
class TAReportConfig:
    instrument: str
    windows: WindowsConfig
    thresholds: ThresholdConfig
    report: ReportOutputConfig
    alignment_timezone: str | None = None
    daily_alignment: int | None = None


class ConfigError(RuntimeError):
    """Raised when configuration is invalid."""


def _parse_time(value: str) -> datetime:
    """Convert OANDA time strings into timezone-aware UTC datetimes."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _coerce_float(raw: str | float | int) -> float:
    return float(raw)


def fetch_candles(
    instrument: str,
    granularity: str,
    token: str,
    count: int = 400,
    alignment_timezone: str | None = None,
    daily_alignment: int | None = None,
) -> List[Candle]:
    """
    Fetch midpoint candles from the OANDA REST API.

    Parameters mirror the upstream REST interface. Daily/weekly/monthly requests
    accept alignment hints to roll the session at 17:00 New York time.
    """

    url = f"https://api-fxpractice.oanda.com/v3/instruments/{instrument}/candles"
    params: dict[str, str | int] = {"granularity": granularity, "count": count, "price": "M"}
    if granularity.startswith("D") or granularity in {"W", "M"}:
        if alignment_timezone:
            params["alignmentTimezone"] = alignment_timezone
        if daily_alignment is not None:
            params["dailyAlignment"] = daily_alignment

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    candles: List[Candle] = []
    for item in payload.get("candles", []):
        mid = item.get("mid")
        if not mid:
            continue
        candles.append(
            Candle(
                time=_parse_time(item["time"]),
                complete=bool(item.get("complete", False)),
                open=_coerce_float(mid["o"]),
                high=_coerce_float(mid["h"]),
                low=_coerce_float(mid["l"]),
                close=_coerce_float(mid["c"]),
                volume=int(item.get("volume", 0)),
            )
        )
    candles.sort(key=lambda c: c.time)
    if not candles:
        raise RuntimeError(f"No candles returned for {instrument} {granularity}")
    return candles


def _moving_average(values: Sequence[float], window: int) -> List[float]:
    """Simple moving average with NaN padding while window is not filled."""
    if window <= 0:
        raise ValueError("window must be > 0")
    out = [math.nan] * len(values)
    window_sum = 0.0
    for idx, value in enumerate(values):
        window_sum += value
        if idx >= window:
            window_sum -= values[idx - window]
        if idx >= window - 1:
            out[idx] = window_sum / window
    return out


def _relative_strength_index(values: Sequence[float], window: int) -> List[float]:
    """Compute RSI with Wilder smoothing."""
    if window <= 0:
        raise ValueError("window must be > 0")
    out = [math.nan] * len(values)
    if len(values) <= window:
        return out

    gains: List[float] = []
    losses: List[float] = []
    for idx in range(1, window + 1):
        change = values[idx] - values[idx - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window
    if avg_loss == 0 and avg_gain == 0:
        out[window] = 50.0
    elif avg_loss == 0:
        out[window] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[window] = 100.0 - (100.0 / (1.0 + rs))

    for idx in range(window + 1, len(values)):
        change = values[idx] - values[idx - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = ((avg_gain * (window - 1)) + gain) / window
        avg_loss = ((avg_loss * (window - 1)) + loss) / window
        if avg_loss == 0 and avg_gain == 0:
            out[idx] = 50.0
        elif avg_loss == 0:
            out[idx] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[idx] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _average_true_range(candles: Sequence[Candle], window: int) -> List[float]:
    """Average true range using a simple moving average of TR values."""
    if window <= 0:
        raise ValueError("window must be > 0")
    out = [math.nan] * len(candles)
    if not candles:
        return out

    tr_values: List[float] = []
    window_sum = 0.0
    for idx, candle in enumerate(candles):
        if idx == 0:
            true_range = candle.high - candle.low
        else:
            prev_close = candles[idx - 1].close
            true_range = max(
                candle.high - candle.low,
                abs(candle.high - prev_close),
                abs(candle.low - prev_close),
            )
        tr_values.append(true_range)
        window_sum += true_range
        if idx >= window:
            window_sum -= tr_values[idx - window]
        if idx >= window - 1:
            out[idx] = window_sum / window
    return out


def _pivot_levels(daily_candles: Sequence[Candle]) -> dict[str, float]:
    """Return classic floor-trader pivot levels based on the last completed day."""
    last_complete = next((c for c in reversed(daily_candles) if c.complete), None)
    if not last_complete:
        raise RuntimeError("No completed candle available for pivot calculation")

    high = last_complete.high
    low = last_complete.low
    close = last_complete.close
    pivot = (high + low + close) / 3.0
    price_range = high - low
    return {
        "P": pivot,
        "S1": 2 * pivot - high,
        "R1": 2 * pivot - low,
        "S2": pivot - price_range,
        "R2": pivot + price_range,
        "prev_high": high,
        "prev_low": low,
        "prev_close": close,
    }


def _near_level(price: float, level: float, near_pct: float) -> bool:
    if level == 0:
        return False
    return abs(price - level) / abs(level) <= near_pct


def _format_price(value: float) -> str:
    return f"{value:,.2f}"


def _is_finite(value: float) -> bool:
    return not math.isnan(value) and not math.isinf(value)


def _latest_price_from_intraday(intraday: dict[str, Sequence[Candle]], fallback: float) -> float:
    for granularity in ("M5", "M15", "H1", "H4"):
        candles = intraday.get(granularity)
        if candles:
            return candles[-1].close
    for candles in intraday.values():
        if candles:
            return candles[-1].close
    return fallback


def _render_intraday_lines(intraday: dict[str, Sequence[Candle]]) -> List[str]:
    lines: List[str] = []
    for granularity in sorted(intraday):
        candles = intraday[granularity]
        if not candles:
            continue
        last = candles[-1]
        timestamp = last.time.strftime("%Y-%m-%d %H:%M UTC")
        lines.append(
            f"- {granularity} 最新价：{_format_price(last.close)} （{timestamp}）"
        )
    return lines


def _build_suggestions(
    rsi_value: float,
    sma_fast: float,
    sma_slow: float,
    price: float,
    thresholds: ThresholdConfig,
    pivot_hits: list[str],
) -> List[str]:
    suggestions: List[str] = []
    if _is_finite(rsi_value):
        if rsi_value <= thresholds.rsi_oversold:
            suggestions.append("RSI 进入超卖区，结合支撑位判断反弹强度。")
        elif rsi_value >= thresholds.rsi_overbought:
            suggestions.append("RSI 进入超买区，警惕回撤并关注短均线防守。")

    if _is_finite(sma_fast) and _is_finite(sma_slow):
        if sma_fast > sma_slow:
            suggestions.append("均线呈金叉结构，中期动能改善。")
        elif sma_fast < sma_slow:
            suggestions.append("均线呈死叉结构，反弹以压力位作为确认。")
        else:
            suggestions.append("均线粘合，短期方向感不足，保持观察。")
    else:
        suggestions.append("均线样本不足，暂无法判定金叉或死叉。")

    if pivot_hits:
        suggestions.append("价格靠近关键位：" + "；".join(pivot_hits))

    if not suggestions:
        suggestions.append("指标未给出明确提示，保持观察。")
    return suggestions


def generate_report_markdown(
    daily_candles: Sequence[Candle],
    intraday: dict[str, Sequence[Candle]],
    cfg: TAReportConfig,
) -> str:
    """Render a Markdown report using daily indicators and optional intraday snapshot."""
    closes = [c.close for c in daily_candles]
    sma_fast_series = _moving_average(closes, cfg.windows.sma_fast)
    sma_slow_series = _moving_average(closes, cfg.windows.sma_slow)
    rsi_series = _relative_strength_index(closes, cfg.windows.rsi)
    atr_series = _average_true_range(daily_candles, cfg.windows.atr)
    pivot_levels = _pivot_levels(daily_candles)

    last_daily = daily_candles[-1]
    last_price = last_daily.close
    sma_fast_value = sma_fast_series[-1]
    sma_slow_value = sma_slow_series[-1]
    rsi_value = rsi_series[-1]
    atr_value = atr_series[-1]

    cross_label = "数据不足"
    trend_label = "观察中"
    if _is_finite(sma_fast_value) and _is_finite(sma_slow_value):
        if sma_fast_value > sma_slow_value:
            cross_label = "金叉"
            trend_label = "偏多" if last_price > sma_fast_value else "回踩中"
        elif sma_fast_value < sma_slow_value:
            cross_label = "死叉"
            trend_label = "偏空" if last_price < sma_fast_value else "反弹观察"
        else:
            cross_label = "均线粘合"
            trend_label = "盘整"

    ref_price = _latest_price_from_intraday(intraday, last_price)
    monitored_levels = ["S2", "S1", "P", "R1", "R2", "prev_low", "prev_high"]
    pivot_hits = [
        f"接近 {name}（{_format_price(pivot_levels[name])}），参考价 { _format_price(ref_price)}"
        for name in monitored_levels
        if _near_level(ref_price, pivot_levels[name], cfg.thresholds.near_pct)
    ]

    intraday_lines = _render_intraday_lines(intraday)
    suggestions = _build_suggestions(
        rsi_value, sma_fast_value, sma_slow_value, last_price, cfg.thresholds, pivot_hits
    )

    report_lines: List[str] = []
    report_lines.append("# XAU/USD 技术面报告")
    current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report_lines.append(f"- 时间：{current_utc}")
    report_lines.append(f"- 最新价（日线收盘）：{_format_price(last_price)}")
    report_lines.append("")
    report_lines.append("## 趋势概览")
    report_lines.append(
        f"- 日线均线：SMA{cfg.windows.sma_fast}={_format_price(sma_fast_value) if _is_finite(sma_fast_value) else 'N/A'}，"
        f"SMA{cfg.windows.sma_slow}={_format_price(sma_slow_value) if _is_finite(sma_slow_value) else 'N/A'}，结构：{cross_label}，判定：{trend_label}"
    )
    rsi_display = f"{rsi_value:.1f}" if _is_finite(rsi_value) else "N/A"
    atr_display = f"{atr_value:.2f}" if _is_finite(atr_value) else "N/A"
    report_lines.append(f"- RSI{cfg.windows.rsi}：{rsi_display} | ATR{cfg.windows.atr}：{atr_display}")
    report_lines.append("")
    report_lines.append("## 支撑与压力")
    report_lines.append(f"- 枢轴点 P: {_format_price(pivot_levels['P'])}")
    report_lines.append(
        f"- S1: {_format_price(pivot_levels['S1'])} | S2: {_format_price(pivot_levels['S2'])}"
    )
    report_lines.append(
        f"- R1: {_format_price(pivot_levels['R1'])} | R2: {_format_price(pivot_levels['R2'])}"
    )
    report_lines.append(
        "- 昨日高/低/收: "
        f"{_format_price(pivot_levels['prev_high'])} / "
        f"{_format_price(pivot_levels['prev_low'])} / "
        f"{_format_price(pivot_levels['prev_close'])}"
    )

    if intraday_lines:
        report_lines.append("")
        report_lines.append("## 盘中观察")
        report_lines.extend(intraday_lines)

    report_lines.append("")
    report_lines.append("## 交易提示（启发式，不构成建议）")
    for suggestion in suggestions:
        report_lines.append(f"- {suggestion}")

    report_lines.append("")
    report_lines.append("> 注：外汇/贵金属为场外报价，OANDA 的 volume 更接近 tick 计数，非全市场成交量。")
    report_lines.append("")
    report_lines.append("_数据源：OANDA Practice（midpoint）；计算口径：纽约 17:00 切日。_")
    report_lines.append("")
    return "\n".join(report_lines)


def _load_config(path: Path) -> TAReportConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigError("配置文件必须是字典结构")

    try:
        windows_raw = raw["windows"]
        thresholds_raw = raw["thresholds"]
        report_raw = raw["report"]
    except KeyError as missing:
        raise ConfigError(f"缺少配置字段：{missing}") from missing

    windows = WindowsConfig(
        sma_fast=int(windows_raw["sma_fast"]),
        sma_slow=int(windows_raw["sma_slow"]),
        rsi=int(windows_raw["rsi"]),
        atr=int(windows_raw["atr"]),
    )
    thresholds = ThresholdConfig(
        rsi_overbought=float(thresholds_raw["rsi_overbought"]),
        rsi_oversold=float(thresholds_raw["rsi_oversold"]),
        near_pct=float(thresholds_raw["near_pct"]),
    )
    intraday_grans = report_raw.get("intraday_granularities") or []
    if not isinstance(intraday_grans, list):
        raise ConfigError("report.intraday_granularities 必须是列表")
    report = ReportOutputConfig(
        filename=str(report_raw["filename"]),
        include_intraday=bool(report_raw.get("include_intraday", True)),
        intraday_granularities=[str(gran) for gran in intraday_grans],
    )
    return TAReportConfig(
        instrument=str(raw["instrument"]),
        alignment_timezone=raw.get("alignmentTimezone") or raw.get("alignment_timezone"),
        daily_alignment=raw.get("dailyAlignment") or raw.get("daily_alignment"),
        windows=windows,
        thresholds=thresholds,
        report=report,
    )


def _write_report(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate OANDA-based TA report")
    parser.add_argument("--config", default="config/ta_xau.yml", help="配置文件路径（默认 config/ta_xau.yml）")
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"配置文件不存在：{config_path}", file=sys.stderr)
        return 2

    try:
        cfg = _load_config(config_path)
    except ConfigError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2

    token = os.getenv("OANDA_TOKEN") or os.getenv("OANDA_API_TOKEN")
    if not token:
        print("请设置环境变量 OANDA_TOKEN=Practice Token", file=sys.stderr)
        return 2

    try:
        daily_candles = fetch_candles(
            cfg.instrument,
            "D",
            token,
            count=400,
            alignment_timezone=cfg.alignment_timezone,
            daily_alignment=int(cfg.daily_alignment) if cfg.daily_alignment is not None else None,
        )
        intraday: dict[str, List[Candle]] = {}
        if cfg.report.include_intraday:
            for gran in cfg.report.intraday_granularities:
                try:
                    intraday[gran] = fetch_candles(cfg.instrument, gran, token, count=200)
                except Exception as exc:  # pragma: no cover - network edge case
                    print(f"获取 {cfg.instrument} {gran} 数据失败：{exc}", file=sys.stderr)
        report_text = generate_report_markdown(daily_candles, intraday, cfg)
    except requests.HTTPError as exc:
        print(f"请求 OANDA API 失败：{exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"访问 OANDA API 异常：{exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"生成报告失败：{exc}", file=sys.stderr)
        return 1

    output_path = Path(cfg.report.filename)
    _write_report(output_path, report_text)
    print(f"Wrote report to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
