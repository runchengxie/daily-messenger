#!/usr/bin/env python3
"""Send Feishu notification using incoming webhook."""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

from daily_messenger.common.logging import log, setup_logger


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


def _normalize_channel(value: str | None) -> str:
    if not value:
        return "daily"
    lowered = value.lower()
    if lowered in {"daily", "report"}:
        return "daily"
    if lowered in {"alerts", "alert"}:
        return "alerts"
    raise ValueError(f"未知频道 {value!r}，请使用 daily 或 alerts")


def _resolve_credentials(
    channel: str,
    explicit_webhook: Optional[str],
    explicit_secret: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    if explicit_webhook:
        return explicit_webhook, explicit_secret

    suffix = channel.upper()
    channel_webhook = os.getenv(f"FEISHU_WEBHOOK_{suffix}")
    channel_secret = os.getenv(f"FEISHU_SECRET_{suffix}")

    fallback_webhook = os.getenv("FEISHU_WEBHOOK")
    fallback_secret = os.getenv("FEISHU_SECRET")

    webhook = channel_webhook or fallback_webhook
    secret = explicit_secret or channel_secret or fallback_secret
    return webhook, secret


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Push message to Feishu webhook")
    from pathlib import Path as _P

    base_dir = _P(__file__).resolve().parents[3]
    out_dir = base_dir / "out"
    parser.add_argument("--channel", default="daily", help="消息频道（daily 或 alerts）")
    parser.add_argument("--webhook", help="飞书自定义机器人 Webhook（覆盖 channel 推断）")
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
    parser.add_argument("--secret", help="签名密钥（覆盖 channel 推断）")
    parser.add_argument("--mode", choices=["interactive", "post"], default=None)
    parser.add_argument("--title", help="备用标题（post 模式使用）")
    args = parser.parse_args(argv)

    logger = setup_logger("feishu")

    try:
        channel = _normalize_channel(args.channel)
    except ValueError as exc:
        parser.error(str(exc))

    webhook, secret = _resolve_credentials(channel, args.webhook, args.secret)

    summary = _read_file(args.summary)
    card_text = _read_file(args.card)
    if args.mode is None:
        card_path = _P(args.card) if args.card else None
        args.mode = "interactive" if card_path and card_path.exists() else "post"
    card = card_text or None

    payload = _build_payload(args, summary, card)
    payload.update(_sign_if_needed(secret))

    if not webhook:
        log(logger, logging.INFO, "feishu_skip_missing_webhook", channel=channel)
        return 0

    resp = requests.post(webhook, json=payload, timeout=10)
    if resp.status_code != 200:
        log(
            logger,
            logging.ERROR,
            "feishu_http_error",
            status_code=resp.status_code,
            response=resp.text[:500],
        )
        return 1

    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if body.get("StatusCode", 0) != 0:
        log(logger, logging.ERROR, "feishu_business_error", response=body)
        return 1

    log(
        logger,
        logging.INFO,
        "feishu_push_completed",
        channel=channel,
        mode=args.mode,
        has_card=bool(card),
        summary_length=len(summary.splitlines()),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
