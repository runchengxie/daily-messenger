#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成 BTC 日报（Markdown）。
来源：data/btcusdt/klines_1d.parquet 和 1h/1m 作为补充。

示例：
  uv run python scripts/crypto_btc/btc_daily_report.py --out out/btc_report.md
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default="data/btcusdt")
    ap.add_argument("--out", default="out/btc_report.md")
    return ap.parse_args()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    # 需要 high/low/close 列，按日线
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
    return tr.rolling(window=period).mean()


def pivots(h, l, c):
    pp = (h + l + c) / 3
    r1 = 2 * pp - l
    s1 = 2 * pp - h
    r2 = pp + (h - l)
    s2 = pp - (h - l)
    r3 = h + 2 * (pp - l)
    s3 = l - 2 * (h - pp)
    return pp, r1, s1, r2, s2, r3, s3


def load_parquet(datadir: Path, interval: str) -> pd.DataFrame:
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


def main():
    args = parse_args()
    datadir = Path(args.datadir)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    d1 = load_parquet(datadir, "1d")
    h1 = load_parquet(datadir, "1h")
    m1 = load_parquet(datadir, "1m")

    if d1.empty:
        raise SystemExit("缺少日线数据，先跑 incremental_fetch.py --interval 1d")

    # 指标
    d1["SMA50"] = d1["close"].rolling(50).mean()
    d1["SMA200"] = d1["close"].rolling(200).mean()
    d1["RSI14"] = rsi(d1["close"], 14)
    d1["ATR14"] = atr(d1, 14)
    last = d1.iloc[-1]

    pp, r1, s1, r2, s2, r3, s3 = pivots(d1["high"].iloc[-2], d1["low"].iloc[-2], d1["close"].iloc[-2])

    # 生成报告
    lines = []
    lines.append("# BTC/USDT 每日技术简报\n")
    lines.append(f"**收盘价**: {last['close']:.2f}  |  **SMA50**: {last['SMA50']:.2f}  |  **SMA200**: {last['SMA200']:.2f}\n")
    sma_state = "上穿" if last["SMA50"] > last["SMA200"] else "下穿或未上穿"
    lines.append(f"**均线关系**: SMA50 相对 SMA200 为 {sma_state}\n")
    lines.append(f"**RSI14**: {last['RSI14']:.1f}  |  **ATR14**: {last['ATR14']:.2f}\n")

    lines.append("\n## 枢轴位 (基于昨日)\n")
    lines.append(f"PP: {pp:.2f} | R1: {r1:.2f} | S1: {s1:.2f} | R2: {r2:.2f} | S2: {s2:.2f} | R3: {r3:.2f} | S3: {s3:.2f}\n")

    if not h1.empty:
        h_last = h1.iloc[-1]
        lines.append("\n## 小时级快照\n")
        lines.append(f"1h 收盘: {h_last['close']:.2f}; 近 24 根均价: {h1['close'].tail(24).mean():.2f}\n")

    if not m1.empty:
        m_last = m1.iloc[-1]
        # 1m RSI
        m1["RSI14"] = rsi(m1["close"], 14)
        lines.append("\n## 分钟级快照\n")
        lines.append(f"1m 最新: {m_last['close']:.2f}; 1m RSI14: {m1['RSI14'].iloc[-1]:.1f}\n")

    md = "\n".join(lines)
    out.write_text(md, encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
