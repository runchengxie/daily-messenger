"""BTC daily Markdown report generation."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "out" / "btc"
DEFAULT_REPORT = PROJECT_ROOT / "out" / "btc_report.md"
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "ta_btc.yml"


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()


def pivots(h, l, c):
    pp = (h + l + c) / 3
    r1 = 2 * pp - l
    s1 = 2 * pp - h
    r2 = pp + (h - l)
    s2 = pp - (h - l)
    r3 = h + 2 * (pp - l)
    s3 = l - 2 * (h - pp)
    return pp, r1, s1, r2, s2, r3, s3


def _load_parquet(datadir: Path, interval: str) -> pd.DataFrame:
    p = datadir / f"klines_{interval}.parquet"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(p)
    except ImportError as exc:
        raise SystemExit(
            "读取 Parquet 需要安装可选依赖 pyarrow 或 fastparquet，请先安装后重试。"
        ) from exc
    df = df.sort_values("open_time").set_index("open_time")
    return df


def _load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _fmt(value, *, nd: int = 2, na_text: str = "—") -> str:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return na_text
    if not np.isfinite(val):
        return na_text
    return f"{val:.{nd}f}"


def build_report(
    *,
    datadir: Path = DEFAULT_DATA_DIR,
    outpath: Path = DEFAULT_REPORT,
    config_path: Optional[Path] = DEFAULT_CONFIG,
) -> Path:
    d1 = _load_parquet(datadir, "1d")
    h1 = _load_parquet(datadir, "1h")
    m1 = _load_parquet(datadir, "1m")

    if d1.empty:
        raise SystemExit("缺少日线数据，先跑 btc fetch --interval 1d")

    config = _load_config(config_path) if config_path else {}
    report_cfg = config.get("report", {}) if isinstance(config.get("report"), dict) else {}
    title = report_cfg.get("title") or "BTC/USDT 每日技术简报"
    include_intraday = bool(report_cfg.get("include_intraday", True))
    intraday_grans_raw = report_cfg.get("intraday_granularities", ["H1", "M1"])
    if isinstance(intraday_grans_raw, (str, bytes)):
        intraday_granularities = {str(intraday_grans_raw).upper()}
    else:
        intraday_granularities = {str(gran).upper() for gran in intraday_grans_raw}

    # Compute technical indicators used in the report.
    d1["SMA50"] = d1["close"].rolling(50, min_periods=1).mean()
    d1["SMA200"] = d1["close"].rolling(200, min_periods=1).mean()
    d1["RSI14"] = rsi(d1["close"], 14)
    d1["ATR14"] = atr(d1, 14)
    last = d1.iloc[-1]

    lines = []
    lines.append(f"# {title}\n")
    lines.append(f"**收盘价**: {_fmt(last['close'])}  |  **SMA50**: {_fmt(last['SMA50'])}  |  **SMA200**: {_fmt(last['SMA200'])}\n")
    if np.isfinite(last["SMA50"]) and np.isfinite(last["SMA200"]):
        sma_state = "上穿" if last["SMA50"] > last["SMA200"] else "下穿或未上穿"
    else:
        sma_state = "数据不足，暂未形成均线信号"
    lines.append(f"**均线关系**: SMA50 相对 SMA200 为 {sma_state}\n")
    lines.append(f"**RSI14**: {_fmt(last['RSI14'], nd=1)}  |  **ATR14**: {_fmt(last['ATR14'])}\n")

    lines.append("\n## 枢轴位 (基于昨日)\n")
    if len(d1) >= 2:
        prev = d1.iloc[-2]
        pp, r1, s1, r2, s2, r3, s3 = pivots(prev["high"], prev["low"], prev["close"])
        lines.append(
            f"PP: {_fmt(pp)} | R1: {_fmt(r1)} | S1: {_fmt(s1)} | R2: {_fmt(r2)} | S2: {_fmt(s2)} | R3: {_fmt(r3)} | S3: {_fmt(s3)}\n"
        )
    else:
        lines.append("历史不足，暂无法计算枢轴位。\n")

    if include_intraday and "H1" in intraday_granularities and not h1.empty:
        h_last = h1.iloc[-1]
        lines.append("\n## 小时级快照\n")
        lines.append(
            f"1h 收盘: {_fmt(h_last['close'])}; 近 24 根均价: {_fmt(h1['close'].tail(24).mean())}\n"
        )

    if include_intraday and {"M1", "1M"}.intersection(intraday_granularities) and not m1.empty:
        m_last = m1.iloc[-1]
        m1["RSI14"] = rsi(m1["close"], 14)
        lines.append("\n## 分钟级快照\n")
        lines.append(f"1m 最新: {_fmt(m_last['close'])}; 1m RSI14: {_fmt(m1['RSI14'].iloc[-1], nd=1)}\n")

    md = "\n".join(lines)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text(md, encoding="utf-8")
    print(f"Wrote {outpath}")
    return outpath


def run_report(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="生成 BTC 日报（Markdown）")
    parser.add_argument("--datadir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--out", default=str(DEFAULT_REPORT))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args(argv)

    build_report(
        datadir=Path(args.datadir),
        outpath=Path(args.out),
        config_path=Path(args.config) if args.config else None,
    )
    return 0
