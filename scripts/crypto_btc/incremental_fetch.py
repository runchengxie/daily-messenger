#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量刷新 BTC K 线，优先 Binance，失败 fallback Kraken -> Bitstamp。

示例：
  uv run python scripts/crypto_btc/incremental_fetch.py --interval 1m --lookback 7d
"""
import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
import requests

BINANCE = "https://data-api.binance.vision/api/v3/klines"
KRAKEN = "https://api.kraken.com/0/public/OHLC"  # pair=XBTUSD, interval=1/5/15/60/240/1440
BITSTAMP = "https://www.bitstamp.net/api/v2/ohlc/btcusd/"  # step=60/300/900/3600/86400, limit

INTERVAL_MAP = {
    "1m": {"binance": "1m", "kraken": 1, "bitstamp": 60},
    "1h": {"binance": "1h", "kraken": 60, "bitstamp": 3600},
    "1d": {"binance": "1d", "kraken": 1440, "bitstamp": 86400},
}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MAP))
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--outdir", default="data/btcusdt")
    ap.add_argument("--lookback", default="2d", help="回看窗口，如 7d/3h/1d")
    ap.add_argument("--max_pages", type=int, default=100)  # 每页最多 1000 根（Binance）
    return ap.parse_args()


def parse_lookback(spec: str) -> timedelta:
    unit = spec[-1]
    val = int(spec[:-1])
    if unit == "m":
        return timedelta(minutes=val)
    if unit == "h":
        return timedelta(hours=val)
    if unit == "d":
        return timedelta(days=val)
    raise ValueError("lookback 只支持 m/h/d")


def load_existing(outdir: Path, interval: str) -> pd.DataFrame:
    p = outdir / f"klines_{interval}.parquet"
    if p.exists():
        try:
            return pd.read_parquet(p)
        except ImportError as exc:
            raise SystemExit(
                "读取 Parquet 需要安装可选依赖 pyarrow 或 fastparquet，请先安装后重试。"
            ) from exc
    return pd.DataFrame(
        columns=[
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
            "symbol",
            "interval",
        ]
    )


def fetch_binance(symbol: str, interval: str, start_ms: int, end_ms: int, limit=1000) -> pd.DataFrame:
    params = {"symbol": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": limit}
    r = requests.get(BINANCE, params=params, timeout=15)
    r.raise_for_status()
    rows = r.json()
    cols = [
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
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["symbol"] = symbol
    return df.drop(columns=["ignore"]).assign(interval=interval)


def fetch_kraken(interval_min: int, since_unix: int) -> pd.DataFrame:
    params = {"pair": "XBTUSD", "interval": interval_min, "since": since_unix}
    r = requests.get(KRAKEN, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()["result"]
    pair_key = [k for k in data.keys() if k != "last"][0]
    rows = data[pair_key]
    # Kraken: [time, open, high, low, close, vwap, volume, count]
    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "vwap", "volume", "count"])
    if df.empty:
        return df
    df = df.assign(
        open_time=pd.to_datetime(df["time"], unit="s", utc=True),
        close_time=pd.to_datetime(df["time"], unit="s", utc=True),
        quote_asset_volume=pd.NA,
        number_of_trades=df["count"],
        taker_buy_base=pd.NA,
        taker_buy_quote=pd.NA,
        symbol="XBTUSD",
    )
    df = df[
        [
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
            "symbol",
        ]
    ]
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def fetch_bitstamp(step: int, start_unix: int, end_unix: int) -> pd.DataFrame:
    params = {"step": step, "limit": 1000, "start": start_unix, "end": end_unix}
    r = requests.get(BITSTAMP, params=params, timeout=15)
    r.raise_for_status()
    # {'data': {'ohlc': [{'timestamp': '1711920000', 'open': '...', ...}]}}
    ohlc = r.json().get("data", {}).get("ohlc", [])
    if not ohlc:
        return pd.DataFrame()
    df = pd.DataFrame(ohlc)
    df = df.assign(
        open_time=pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True),
        close_time=pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True),
        open=pd.to_numeric(df["open"]),
        high=pd.to_numeric(df["high"]),
        low=pd.to_numeric(df["low"]),
        close=pd.to_numeric(df["close"]),
        volume=pd.to_numeric(df["volume"]),
        quote_asset_volume=pd.NA,
        number_of_trades=pd.NA,
        taker_buy_base=pd.NA,
        taker_buy_quote=pd.NA,
        symbol="BTCUSD",
    )
    return df[
        [
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
            "symbol",
        ]
    ]


def main():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"klines_{args.interval}.parquet"

    df = load_existing(outdir, args.interval)
    if df.empty:
        start = datetime.now(timezone.utc) - parse_lookback(args.lookback)
    else:
        start = (
            pd.to_datetime(df["open_time"].max()).to_pydatetime().replace(tzinfo=timezone.utc)
            - parse_lookback(args.lookback)
        )
    end = datetime.now(timezone.utc)

    b_int = INTERVAL_MAP[args.interval]["binance"]
    k_int = INTERVAL_MAP[args.interval]["kraken"]
    s_int = INTERVAL_MAP[args.interval]["bitstamp"]

    cur = start
    frames: List[pd.DataFrame] = []
    pages = 0
    while cur < end and pages < args.max_pages:
        pages += 1
        # Binance 时间窗口（毫秒）
        start_ms = int(cur.timestamp() * 1000)
        # 估个窗口长度：1m -> 1000 分钟，1h -> 1000 小时，1d -> 1000 天
        if args.interval == "1m":
            delta = timedelta(minutes=1000)
        elif args.interval == "1h":
            delta = timedelta(hours=1000)
        else:
            delta = timedelta(days=1000)
        end_ms = int(min(end, cur + delta).timestamp() * 1000)

        try:
            chunk = fetch_binance(args.symbol, b_int, start_ms, end_ms)
            source = "binance"
        except Exception as e:
            print(f"Binance fail: {e}; try Kraken...")
            # Kraken since=秒，返回会包含 since 起始之后的段
            since = int(cur.timestamp())
            try:
                chunk = fetch_kraken(k_int, since)
                source = "kraken"
            except Exception as e2:
                print(f"Kraken fail: {e2}; try Bitstamp...")
                try:
                    chunk = fetch_bitstamp(s_int, int(cur.timestamp()), int((cur + delta).timestamp()))
                    source = "bitstamp"
                except Exception as e3:
                    print(f"Bitstamp fail: {e3}; giving up this window")
                    chunk = pd.DataFrame()

        if chunk is not None and not chunk.empty:
            frames.append(chunk)
            last_ts = pd.to_datetime(chunk["open_time"].max()).to_pydatetime().replace(tzinfo=timezone.utc)
            cur = last_ts + timedelta(milliseconds=1)
            print(f"{source}: +{len(chunk)} rows -> advance to {last_ts}")
        else:
            cur = cur + delta
            print("empty window, advance")
        time.sleep(0.2)  # 稍微礼貌点

    if frames:
        add = pd.concat(frames, ignore_index=True)
        all_df = pd.concat([df, add], ignore_index=True)
        all_df = all_df.drop_duplicates(subset=["open_time"]).sort_values("open_time")
        try:
            all_df.to_parquet(outpath, index=False)
        except ImportError as exc:
            raise SystemExit(
                "写入 Parquet 需要安装可选依赖 pyarrow 或 fastparquet，请先安装后重试。"
            ) from exc
        print(f"Saved {len(all_df)} rows -> {outpath}")
    else:
        print("No new data fetched.")


if __name__ == "__main__":
    main()
