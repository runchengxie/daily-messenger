"""BTC kline helpers reused by the CLI and CI workflows."""

from __future__ import annotations

import argparse
import io
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "out" / "btc"

BASE = "https://data.binance.vision"
DAILY_PATH = "/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"

BINANCE = "https://data-api.binance.vision/api/v3/klines"
KRAKEN = "https://api.kraken.com/0/public/OHLC"  # pair=XBTUSD, interval=1/5/15/60/240/1440
BITSTAMP = "https://www.bitstamp.net/api/v2/ohlc/btcusd/"  # step=60/300/900/3600/86400, limit

INTERVALS = {"1m", "1h", "1d"}
INTERVAL_MAP = {
    "1m": {"binance": "1m", "kraken": 1, "bitstamp": 60},
    "1h": {"binance": "1h", "kraken": 60, "bitstamp": 3600},
    "1d": {"binance": "1d", "kraken": 1440, "bitstamp": 86400},
}


def parse_lookback(spec: str) -> timedelta:
    """Convert lookback strings like ``7d`` or ``12h`` to ``timedelta``."""

    unit = spec[-1]
    val = int(spec[:-1])
    if unit == "m":
        return timedelta(minutes=val)
    if unit == "h":
        return timedelta(hours=val)
    if unit == "d":
        return timedelta(days=val)
    raise ValueError("lookback 只支持 m/h/d")


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _load_existing(outdir: Path, interval: str) -> pd.DataFrame:
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


def _write_parquet(df: pd.DataFrame, outpath: Path) -> None:
    try:
        df.to_parquet(outpath, index=False)
    except ImportError as exc:
        raise SystemExit(
            "写入 Parquet 需要安装可选依赖 pyarrow 或 fastparquet，请先安装后重试。"
        ) from exc


def fetch_one(symbol: str, interval: str, date_str: str) -> pd.DataFrame:
    url = BASE + DAILY_PATH.format(symbol=symbol, interval=interval, date=date_str)
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return pd.DataFrame()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        # Each archive contains a single CSV such as BTCUSDT-1m-2024-01-01.csv.
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


def init_history(
    *,
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    outdir: Path = DEFAULT_DATA_DIR,
) -> Path:
    if interval not in INTERVALS:
        raise ValueError(f"interval must be one of {sorted(INTERVALS)}")

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"klines_{interval}.parquet"

    frames = []
    for d in daterange(start, end):
        date_str = d.strftime("%Y-%m-%d")
        try:
            df = fetch_one(symbol, interval, date_str)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN {date_str}: {exc}")
            df = pd.DataFrame()
        if not df.empty:
            frames.append(df)
            print(f"OK  {date_str}: {len(df)} rows")
        else:
            print(f"MISS {date_str}")

    if not frames:
        print("No data fetched. Check connectivity or use incremental REST script.")
        return outpath

    all_df = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["open_time"])
        .sort_values("open_time")
    )
    _write_parquet(all_df, outpath)
    print(f"Saved {len(all_df)} rows -> {outpath}")
    return outpath


def incremental_fetch(
    *,
    interval: str,
    symbol: str = "BTCUSDT",
    outdir: Path = DEFAULT_DATA_DIR,
    lookback: str = "2d",
    max_pages: int = 100,
) -> Path:
    if interval not in INTERVAL_MAP:
        raise ValueError(f"interval must be one of {sorted(INTERVAL_MAP)}")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"klines_{interval}.parquet"

    df = _load_existing(outdir, interval)
    if df.empty:
        start = datetime.now(timezone.utc) - parse_lookback(lookback)
    else:
        start = (
            pd.to_datetime(df["open_time"].max()).to_pydatetime().replace(tzinfo=timezone.utc)
            - parse_lookback(lookback)
        )
    end = datetime.now(timezone.utc)

    b_int = INTERVAL_MAP[interval]["binance"]
    k_int = INTERVAL_MAP[interval]["kraken"]
    s_int = INTERVAL_MAP[interval]["bitstamp"]

    cur = start
    frames: List[pd.DataFrame] = []
    pages = 0
    while cur < end and pages < max_pages:
        pages += 1
        # Binance endpoints expect millisecond timestamps for the window.
        start_ms = int(cur.timestamp() * 1000)
        # Window size heuristic: 1m ≈ 1000 minutes, 1h ≈ 1000 hours, 1d ≈ 1000 days.
        if interval == "1m":
            delta = timedelta(minutes=1000)
        elif interval == "1h":
            delta = timedelta(hours=1000)
        else:
            delta = timedelta(days=1000)
        end_ms = int(min(end, cur + delta).timestamp() * 1000)

        try:
            chunk = fetch_binance(symbol, b_int, start_ms, end_ms)
            source = "binance"
        except Exception as exc:  # noqa: BLE001
            print(f"Binance fail: {exc}; try Kraken...")
            # Kraken uses second-based timestamps and returns data since the provided value.
            since = int(cur.timestamp())
            try:
                chunk = fetch_kraken(k_int, since)
                source = "kraken"
            except Exception as exc2:  # noqa: BLE001
                print(f"Kraken fail: {exc2}; try Bitstamp...")
                try:
                    chunk = fetch_bitstamp(s_int, int(cur.timestamp()), int((cur + delta).timestamp()))
                    source = "bitstamp"
                except Exception as exc3:  # noqa: BLE001
                    print(f"Bitstamp fail: {exc3}; giving up this window")
                    chunk = pd.DataFrame()

        if chunk is not None and not chunk.empty:
            frames.append(chunk)
            last_ts = pd.to_datetime(chunk["open_time"].max()).to_pydatetime().replace(tzinfo=timezone.utc)
            cur = last_ts + timedelta(milliseconds=1)
            print(f"{source}: +{len(chunk)} rows -> advance to {last_ts}")
        else:
            cur = cur + delta
            print("empty window, advance")
        time.sleep(0.2)  # Be polite to upstream APIs.

    if frames:
        add = pd.concat(frames, ignore_index=True)
        all_df = pd.concat([df, add], ignore_index=True)
        all_df = all_df.drop_duplicates(subset=["open_time"]).sort_values("open_time")
        _write_parquet(all_df, outpath)
        print(f"Saved {len(all_df)} rows -> {outpath}")
    else:
        print("No new data fetched.")
    return outpath


def run_init_history(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="初始化 BTCUSDT 历史数据")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m", choices=sorted(INTERVALS))
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--outdir", default=str(DEFAULT_DATA_DIR))
    args = parser.parse_args(argv)
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    init_history(
        symbol=args.symbol,
        interval=args.interval,
        start=start,
        end=end,
        outdir=Path(args.outdir),
    )
    return 0


def run_fetch(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="增量刷新 BTC K 线")
    parser.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MAP))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--outdir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--lookback", default="2d", help="回看窗口，如 7d/3h/1d")
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args(argv)
    incremental_fetch(
        interval=args.interval,
        symbol=args.symbol,
        outdir=Path(args.outdir),
        lookback=args.lookback,
        max_pages=args.max_pages,
    )
    return 0
