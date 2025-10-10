import os, sys, time, json
import requests

API_KEY = os.getenv("FMP_API_KEY") or (sys.argv[1] if len(sys.argv) > 1 else None)
if not API_KEY:
    print("请在环境变量 FMP_API_KEY 中设置 API Key，或作为第一个参数传入。")
    sys.exit(1)

session = requests.Session()
session.headers.update({"User-Agent": "curl/8.0"})
base = "https://financialmodelingprep.com"


def get_stable_quote(symbol: str):
    url = f"{base}/stable/quote"
    params = {"symbol": symbol, "apikey": API_KEY}
    r = session.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    raise RuntimeError("stable/quote 无数据")


def get_quote_short(symbol: str):
    url = f"{base}/api/v3/quote-short/{symbol}"
    params = {"apikey": API_KEY}
    r = session.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    raise RuntimeError("quote-short 无数据")


symbol = "AAPL"
try:
    q = get_stable_quote(symbol)
    src = "stable/quote"
except Exception as e:
    print(f"[WARN] stable/quote 失败: {e}")
    q = get_quote_short(symbol)
    src = "api/v3/quote-short"

# 统一打印关键信息
price = (
    q.get("price")
    or q.get("priceAvg")
    or q.get("ask")
    or q.get("bid")
    or q.get("price")
)  # 兜底
ts = int(time.time())
print(
    json.dumps(
        {"symbol": symbol, "price": price, "raw": q, "source": src, "ts": ts},
        ensure_ascii=False,
        indent=2,
    )
)
