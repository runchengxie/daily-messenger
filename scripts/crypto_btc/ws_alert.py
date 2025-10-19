#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时告警（常驻进程）：订阅 Binance ws kline_1m，触发阈值后调用 post_feishu。

示例：
  FEISHU_WEBHOOK_ALERTS=xxx FEISHU_SECRET_ALERTS=yyy \
  uv run python scripts/crypto_btc/ws_alert.py --symbol btcusdt --rsi_high 70 --rsi_low 30
"""
import argparse
import asyncio
import json
import os
import subprocess
from collections import deque

import pandas as pd
import websockets

WS = "wss://stream.binance.com:9443/ws"


def rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = (avg_gain.iloc[-1]) / max(avg_loss.iloc[-1], 1e-9)
    return float(100 - (100 / (1 + rs)))


async def run(symbol: str, rsi_high: float, rsi_low: float, min_gap_sec: int):
    stream = f"{symbol.lower()}@kline_1m"
    url = f"{WS}/{stream}"
    buf = deque(maxlen=500)
    last_alert_ts = 0
    print(f"Connecting {url}")
    async for ws in websockets.connect(url, ping_interval=20, ping_timeout=20):
        try:
            async for msg in ws:
                data = json.loads(msg)
                k = data.get("k", {})
                if not k:
                    continue
                close = float(k["c"])
                buf.append(close)
                if len(buf) < 20:
                    continue
                value = rsi(pd.Series(list(buf)))
                import time as _time

                now = int(_time.time())
                if value >= rsi_high and now - last_alert_ts > min_gap_sec:
                    send_feishu(f"BTC 1m RSI 触顶 {value:.1f}，价格 {close:.2f}")
                    last_alert_ts = now
                if value <= rsi_low and now - last_alert_ts > min_gap_sec:
                    send_feishu(f"BTC 1m RSI 触底 {value:.1f}，价格 {close:.2f}")
                    last_alert_ts = now
        except Exception as e:
            print("ws error:", e)
            await asyncio.sleep(3)
            continue


def send_feishu(text: str):
    print("ALERT:", text)
    # 复用你仓库已有的推送器
    try:
        subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "daily_messenger.tools.post_feishu",
                "--channel",
                "alerts",
                "--mode",
                "post",
                "--summary",
                "-",
            ],
            input=text.encode("utf-8"),
            check=False,
        )
    except FileNotFoundError:
        # 退而求其次：直接用 webhook（若存在）
        webhook = os.getenv("FEISHU_WEBHOOK_ALERTS")
        if not webhook:
            return
        import requests

        requests.post(webhook, json={"msg_type": "text", "content": {"text": text}}, timeout=10)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="btcusdt")
    ap.add_argument("--rsi_high", type=float, default=70)
    ap.add_argument("--rsi_low", type=float, default=30)
    ap.add_argument("--min_gap_sec", type=int, default=900, help="两次告警最小间隔")
    args = ap.parse_args()
    asyncio.run(run(args.symbol, args.rsi_high, args.rsi_low, args.min_gap_sec))
