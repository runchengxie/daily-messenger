# project_tools/fmp_connectivity_test.py
import os
import sys
import json
import time
from datetime import UTC, datetime

import requests


def load_fmp_key():
    # 1) 环境变量优先
    env_key = os.getenv("FMP_API_KEY")
    if env_key:
        return env_key, "env:FMP_API_KEY"

    # 2) 从 API_KEYS_PATH 指定的 json 里读
    path = os.getenv("API_KEYS_PATH", "./api_keys.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            k = data.get("financial_modeling_prep")
            if k and k not in ("FMP_KEY", "YOUR_FMP_KEY", ""):
                return k, f"json:{path}"
        except Exception as e:
            print(f"[WARN] 读取 {path} 失败: {e}")

    # 3) 命令行参数作为兜底
    if len(sys.argv) > 1:
        return sys.argv[1], "argv"

    return None, None


def http_get(session, url, params):
    r = session.get(url, params=params, timeout=12)
    r.raise_for_status()
    return r.json()


def main():
    key, source = load_fmp_key()
    if not key:
        print(
            "请设置 FMP_API_KEY，或在 API_KEYS_PATH 指向的 api_keys.json 里提供 financial_modeling_prep，或把 key 当第一个参数传入。"
        )
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"User-Agent": "curl/8.3"})

    base = "https://financialmodelingprep.com"
    symbol = "AAPL"

    quote_source = "stable/quote"
    try:
        data = http_get(
            session, f"{base}/stable/quote", {"symbol": symbol, "apikey": key}
        )
        if not isinstance(data, list) or not data:
            raise RuntimeError("stable/quote 返回空")
        quote = data[0]
    except Exception as e:
        print(f"[WARN] stable/quote 失败: {e}")
        data = http_get(session, f"{base}/api/v3/quote-short/{symbol}", {"apikey": key})
        quote_source = "api/v3/quote-short"
        if not isinstance(data, list) or not data:
            raise RuntimeError("quote-short 返回空")
        quote = data[0]

    ratios = {}
    ratios_source = "api/v3/ratios-ttm"
    try:
        ratios_data = http_get(
            session,
            f"{base}/api/v3/ratios-ttm/{symbol}",
            {"limit": 1, "apikey": key},
        )
        if isinstance(ratios_data, list) and ratios_data:
            ratios = ratios_data[0]
        else:
            print("[WARN] ratios-ttm 返回空，跳过估值字段")
    except Exception as e:
        print(f"[WARN] ratios-ttm 请求失败: {e}")

    price = (
        quote.get("price")
        or quote.get("priceAvg")
        or quote.get("ask")
        or quote.get("bid")
    )

    timestamp = quote.get("timestamp")
    iso_ts = None
    if isinstance(timestamp, (int, float)):
        try:
            iso_ts = datetime.fromtimestamp(timestamp, UTC).isoformat()
        except Exception as exc:  # noqa: BLE001 - diagnostic only
            print(f"[WARN] timestamp 解析失败: {exc}")

    out = {
        "symbol": symbol,
        "price": price,
        "marketCap": quote.get("marketCap"),
        "pe_ttm": ratios.get("peRatioTTM"),
        "ps_ttm": ratios.get("priceToSalesRatioTTM"),
        "raw": {
            "quote": quote,
            "ratios": ratios,
        },
        "sources": {
            "quote": quote_source,
            "ratios": ratios_source,
        },
        "key_source": source,
        "ts": int(time.time()),
        "quote_timestamp_iso": iso_ts,
        "proxy_on": bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")),
    }

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
