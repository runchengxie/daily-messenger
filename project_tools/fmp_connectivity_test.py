# project_tools/fmp_connectivity_test.py
import os, sys, json, time, requests


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

    try:
        data = http_get(
            session, f"{base}/stable/quote", {"symbol": symbol, "apikey": key}
        )
        src = "stable/quote"
        if not isinstance(data, list) or not data:
            raise RuntimeError("stable/quote 返回空")
        q = data[0]
    except Exception as e:
        print(f"[WARN] stable/quote 失败: {e}")
        data = http_get(session, f"{base}/api/v3/quote-short/{symbol}", {"apikey": key})
        src = "api/v3/quote-short"
        if not isinstance(data, list) or not data:
            raise RuntimeError("quote-short 返回空")
        q = data[0]

    price = q.get("price") or q.get("priceAvg") or q.get("ask") or q.get("bid")
    print(
        json.dumps(
            {
                "symbol": symbol,
                "price": price,
                "raw": q,
                "source": src,
                "key_source": source,
                "ts": int(time.time()),
                "proxies_respected": bool(
                    os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
