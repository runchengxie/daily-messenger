import math
from datetime import datetime, timedelta, timezone

from daily_messenger.digest.ta_report import (
    Candle,
    ReportOutputConfig,
    TAReportConfig,
    ThresholdConfig,
    WindowsConfig,
    _average_true_range,
    _moving_average,
    _near_level,
    _pivot_levels,
    _relative_strength_index,
    generate_report_markdown,
)


def _make_candle(
    base_time: datetime,
    close: float,
    *,
    complete: bool = True,
) -> Candle:
    return Candle(
        time=base_time,
        complete=complete,
        open=close - 1.0,
        high=close + 2.0,
        low=close - 2.0,
        close=close,
        volume=100,
    )


def test_moving_average_basic() -> None:
    values = [1.0, 2.0, 3.0, 4.0]
    result = _moving_average(values, 2)
    assert math.isnan(result[0])
    assert result[1] == 1.5
    assert result[2] == 2.5
    assert result[3] == 3.5


def test_relative_strength_index_progression() -> None:
    values = [1, 2, 3, 2, 1, 2, 3]
    rsi = _relative_strength_index([float(v) for v in values], 3)
    assert len(rsi) == len(values)
    assert rsi[3] != rsi[4]  # RSI responds to price changes


def test_average_true_range_matches_range_when_increasing() -> None:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [_make_candle(base + timedelta(days=idx), 1800 + idx) for idx in range(5)]
    atr = _average_true_range(candles, 3)
    assert len(atr) == len(candles)
    assert atr[-1] > 0.0


def test_pivot_levelsUses_last_completed() -> None:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [
        _make_candle(base + timedelta(days=idx), 1900 + idx)
        for idx in range(3)
    ]
    levels = _pivot_levels(candles)
    assert set(levels.keys()) == {"P", "S1", "R1", "S2", "R2", "prev_high", "prev_low", "prev_close"}
    assert levels["prev_close"] == candles[-1].close


def test_near_level_detects_small_distance() -> None:
    assert _near_level(100.1, 100.0, 0.002)
    assert not _near_level(102.0, 100.0, 0.002)


def test_generate_report_markdown_contains_sections() -> None:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    daily = [_make_candle(base + timedelta(days=idx), 1900 + idx) for idx in range(12)]
    intraday = {
        "H1": [_make_candle(base + timedelta(hours=idx), 1915 + idx, complete=True) for idx in range(3)]
    }
    cfg = TAReportConfig(
        instrument="XAU_USD",
        alignment_timezone="America/New_York",
        daily_alignment=17,
        windows=WindowsConfig(sma_fast=3, sma_slow=5, rsi=3, atr=3),
        thresholds=ThresholdConfig(rsi_overbought=70.0, rsi_oversold=30.0, near_pct=0.01),
        report=ReportOutputConfig(filename="out/test_ta.md", include_intraday=True, intraday_granularities=["H1"]),
    )

    markdown = generate_report_markdown(daily, intraday, cfg)

    assert "# XAU/USD 技术面报告" in markdown
    assert "## 趋势概览" in markdown
    assert "## 支撑与压力" in markdown
    assert "## 盘中观察" in markdown
    assert "## 交易提示" in markdown
