#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化 BTCUSDT 历史数据（1m/1h/1d），从 data.binance.vision 下载每日压缩包并合并到本地 Parquet。

示例：
  uv run python scripts/crypto_btc/init_binance_history.py \
    --symbol BTCUSDT --interval 1m --start 2024-01-01 --end 2024-12-31

注意：
- 若某些日期不存在文件（新合约/维护日），脚本会跳过。
- 若网络连不上该域名，直接退出并提示你改用 REST 回补脚本。
"""
import argparse
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

BASE = "https://data.binance.vision"
DAILY_PATH = "/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"

INTERVALS = {"1m", "1h", "1d"}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--interval", default="1m", choices=sorted(INTERVALS))
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--outdir", default="data/btcusdt")
    return ap.parse_args()


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def fetch_one(symbol: str, interval: str, date_str: str) -> pd.DataFrame:
    url = BASE + DAILY_PATH.format(symbol=symbol, interval=interval, date=date_str)
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return pd.DataFrame()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        # 每个 zip 里只有一个 CSV，文件名形如 BTCUSDT-1m-2024-01-01.csv
        name = [n for n in zf.namelist() if n.endswith(".csv")][0]
        with zf.open(name) as f:
            df = pd.read_csv(
                f,
                header=None,
                names=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base",
                    "taker_buy_quote",
                    "ignore",
                ],
            )
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
            numeric_cols = [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_asset_volume",
                "taker_buy_base",
                "taker_buy_quote",
            ]
            for c in numeric_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["symbol"] = symbol
            df["interval"] = interval
            return df


def main():
    args = parse_args()
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"klines_{args.interval}.parquet"

    frames = []
    for d in daterange(start, end):
        date_str = d.strftime("%Y-%m-%d")
        try:
            df = fetch_one(args.symbol, args.interval, date_str)
        except Exception as e:
            print(f"WARN {date_str}: {e}")
            df = pd.DataFrame()
        if not df.empty:
            frames.append(df)
            print(f"OK  {date_str}: {len(df)} rows")
        else:
            print(f"MISS {date_str}")

    if not frames:
        print("No data fetched. Check connectivity or use incremental REST script.")
        return

    all_df = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["open_time"])
        .sort_values("open_time")
    )
    try:
        all_df.to_parquet(outpath, index=False)
    except ImportError as exc:
        raise SystemExit(
            "写入 Parquet 需要安装可选依赖 pyarrow 或 fastparquet，请先安装后重试。"
        ) from exc
    print(f"Saved {len(all_df)} rows -> {outpath}")


if __name__ == "__main__":
    main()
