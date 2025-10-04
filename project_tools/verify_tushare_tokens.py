# -*- coding: utf-8 -*-
"""Utility script to verify TuShare tokens via the user quota endpoint."""
import os
from pathlib import Path
from typing import Iterable, Literal, TypedDict

import tushare as ts


ENV_KEYS = ("TUSHARE_TOKEN", "TUSHARE_TOKEN_2")


class TokenInfo(TypedDict):
    """Structured token metadata for a successful check."""

    env_key: str
    user_id: str
    rows: str
    has_rows: bool


class TokenCheckFailure(TypedDict):
    """Description of a failed token check."""

    env_key: str
    ok: Literal[False]
    message: str


class TokenCheckSuccess(TokenInfo):
    """Successful token check enriched with a status flag."""

    ok: Literal[True]


TokenCheckResult = TokenCheckSuccess | TokenCheckFailure


def _env_paths_to_try() -> Iterable[Path]:
    """Yield plausible locations of a .env file for convenience."""
    script_dir = Path(__file__).resolve().parent
    # Start with CWD (useful when running via poetry/pytest), then walk up from script dir
    yield Path.cwd() / ".env"
    for parent in [script_dir, *script_dir.parents]:
        yield parent / ".env"


def load_local_env() -> None:
    """Populate environment variables from the first existing .env file."""
    for env_path in _env_paths_to_try():
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        break


def check_token(env_key: str) -> TokenCheckResult:
    """Return the outcome of verifying the TuShare token stored under ``env_key``."""
    token = os.getenv(env_key)
    if not token:
        return {"env_key": env_key, "ok": False, "message": f"环境变量 {env_key} 未设置。"}

    try:
        pro = ts.pro_api(token=token)
        df = pro.user(token=token)
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "env_key": env_key,
            "ok": False,
            "message": f"调用 TuShare 接口失败: {exc}",
        }

    if df is None:
        return {"env_key": env_key, "ok": False, "message": f"TuShare 返回空对象，无法验证 {env_key}。"}

    # ``pro.user`` returns multiple rows when several quotas are expiring; serialize for readability.
    return {
        "env_key": env_key,
        "user_id": str(df.iloc[0]["user_id"]) if not df.empty else "<未知>",
        "rows": df.to_json(orient="records", force_ascii=False),
        "has_rows": not df.empty,
        "ok": True,
    }


def main() -> None:
    load_local_env()

    results = [check_token(env_key) for env_key in ENV_KEYS]
    any_success = False

    for result in results:
        print("-" * 40)
        print(f"环境变量: {result['env_key']}")
        if result["ok"] is True:
            any_success = True
            print(f"用户 ID: {result['user_id']}")
            if result["has_rows"] is True:
                print(f"积分明细: {result['rows']}")
            else:
                print("积分明细: [] (未返回即将到期的积分记录)")
        else:
            print(f"检测失败: {result['message']}")

    if not any_success:
        raise SystemExit("未检测到有效的 TuShare Token。")


if __name__ == "__main__":
    main()
