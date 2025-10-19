from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from daily_messenger.crypto import klines, report


def test_parse_lookback():
    assert klines.parse_lookback("10m") == timedelta(minutes=10)
    assert klines.parse_lookback("2h") == timedelta(hours=2)
    assert klines.parse_lookback("3d") == timedelta(days=3)
    with pytest.raises(ValueError):
        klines.parse_lookback("5w")


def test_incremental_fetch_rejects_unknown_interval(tmp_path):
    with pytest.raises(ValueError):
        klines.incremental_fetch(interval="5m", outdir=tmp_path)


def test_build_report_generates_markdown(tmp_path, monkeypatch):
    dates = pd.date_range("2024-01-01", periods=210, freq="D", tz="UTC")
    values = pd.Series(range(210), dtype="float64")
    daily = pd.DataFrame(
        {
            "open": values + 100,
            "high": values + 105,
            "low": values + 95,
            "close": values + 102,
            "volume": values + 10,
        },
        index=dates,
    )

    def fake_load_parquet(_datadir: Path, interval: str) -> pd.DataFrame:
        if interval == "1d":
            return daily.copy()
        return pd.DataFrame()

    monkeypatch.setattr(report, "_load_parquet", fake_load_parquet)

    out = tmp_path / "btc_report.md"
    report.build_report(datadir=tmp_path, outpath=out, config_path=None)
    data = out.read_text(encoding="utf-8")
    assert data.startswith("# BTC/USDT 每日技术简报")
    assert "## 枢轴位" in data
