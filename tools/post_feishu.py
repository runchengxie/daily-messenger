#!/usr/bin/env python3
"""Send Feishu notification using incoming webhook."""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from typing import Dict, Any

import requests


def _read_file(path: str | None) -> str:
    if not path:
        return ""
    file_path = Path(path)
    if not file_path.exists():
        return ""
    data = file_path.read_text(encoding="utf-8")
    return data.strip()


def _sign_if_needed(secret: str | None) -> Dict[str, str]:
    if not secret:
        return {}
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{secret}"
    key = secret.encode("utf-8")
    hmac_code = hmac.new(key, string_to_sign.encode("utf-8"), digestmod=hashlib.sha256)
    sign = base64.b64encode(hmac_code.digest()).decode("utf-8")
    return {"timestamp": timestamp, "sign": sign}


def _build_payload(args: argparse.Namespace, summary: str, card: str | None) -> Dict[str, Any]:
    if args.mode == "interactive":
        if not card:
            raise ValueError("需要提供 --card 文件以发送互动卡片")
        return {"msg_type": "interactive", "card": json.loads(card)}

    # post 模式
    zh_lines = summary.splitlines() or ["今日暂无摘要"]
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": args.title or "内参播报",
                    "content": [[{"tag": "text", "text": line + "\n"}] for line in zh_lines],
                }
            }
        },
    }


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Push message to Feishu webhook")
    from pathlib import Path as _P

    base_dir = _P(__file__).resolve().parents[1]
    out_dir = base_dir / "out"
    parser.add_argument(
        "--webhook",
        default=os.getenv("FEISHU_WEBHOOK"),
        help="飞书自定义机器人 Webhook（可从 FEISHU_WEBHOOK 读取）",
    )
    parser.add_argument(
        "--summary",
        default=str(out_dir / "digest_summary.txt"),
        help="摘要文本路径（默认 out/digest_summary.txt）",
    )
    parser.add_argument(
        "--card",
        default=str(out_dir / "digest_card.json"),
        help="互动卡片 JSON 文件路径（默认 out/digest_card.json）",
    )
    parser.add_argument("--secret", default=os.getenv("FEISHU_SECRET"), help="签名密钥")
    parser.add_argument("--mode", choices=["interactive", "post"], default=None)
    parser.add_argument("--title", help="备用标题（post 模式使用）")
    args = parser.parse_args(argv)

    summary = _read_file(args.summary)
    card_text = _read_file(args.card)
    if args.mode is None:
        card_path = _P(args.card) if args.card else None
        args.mode = "interactive" if card_path and card_path.exists() else "post"
    card = card_text or None

    payload = _build_payload(args, summary, card)
    payload.update(_sign_if_needed(args.secret))

    if not args.webhook:
        print("缺少 FEISHU_WEBHOOK，跳过推送。")
        return 0

    resp = requests.post(args.webhook, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"发送失败，状态码 {resp.status_code}，响应：{resp.text}")
        return 1

    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if body.get("StatusCode", 0) != 0:
        print(f"飞书返回错误：{body}")
        return 1

    print("飞书通知发送完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
