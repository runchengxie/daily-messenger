#!/usr/bin/env python3
"""Render HTML report, plain-text digest, and Feishu card payload."""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping

from daily_messenger.common import run_meta
from daily_messenger.common.logging import log, setup_logger

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, PackageLoader, select_autoescape

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = PROJECT_ROOT / "out"
TEMPLATE_DIR = PACKAGE_ROOT / "templates"
METRIC_LABELS = {
    "fundamental": "基本面",
    "valuation": "估值",
    "sentiment": "情绪",
    "liquidity": "资金",
    "event": "事件",
}
SENTIMENT_LABELS = {
    "put_call": "Cboe 认沽/认购",
    "aaii": "AAII 多空差",
}


@dataclass
class ThemePayload:
    name: str
    label: str
    total: float
    breakdown: Dict[str, float]
    breakdown_detail: Dict[str, Dict[str, object]] = field(default_factory=dict)
    meta: Dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "ThemePayload":
        try:
            name = str(data["name"])
        except KeyError as exc:  # noqa: B904
            raise ValueError("主题缺少 name 字段") from exc
        label = str(data.get("label", name))
        total = float(data.get("total", 0.0))
        breakdown_raw = data.get("breakdown", {})
        breakdown: Dict[str, float] = {}
        if isinstance(breakdown_raw, Mapping):
            for key, value in breakdown_raw.items():
                try:
                    breakdown[str(key)] = float(value)
                except (TypeError, ValueError):
                    continue
        detail_raw = data.get("breakdown_detail", {})
        if isinstance(detail_raw, Mapping):
            detail = {str(k): dict(v) for k, v in detail_raw.items() if isinstance(v, Mapping)}
        else:
            detail = {}
        meta_raw = data.get("meta")
        meta = dict(meta_raw) if isinstance(meta_raw, Mapping) else {}
        return cls(name=name, label=label, total=total, breakdown=breakdown, breakdown_detail=detail, meta=meta)

    def to_mapping(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "label": self.label,
            "total": self.total,
            "breakdown": self.breakdown,
            "breakdown_detail": self.breakdown_detail,
            "meta": self.meta,
        }


@dataclass
class ActionPayload:
    action: str
    name: str
    reason: str

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "ActionPayload":
        try:
            action = str(data["action"])
            name = str(data["name"])
        except KeyError as exc:  # noqa: B904
            raise ValueError("操作项缺少 action/name 字段") from exc
        reason = str(data.get("reason", ""))
        return cls(action=action, name=name, reason=reason)

    def to_mapping(self) -> Dict[str, str]:
        return {"action": self.action, "name": self.name, "reason": self.reason}


def _load_json(path: Path, *, required: bool = True) -> Dict[str, object]:
    """Load a JSON document from *path*.

    Args:
        path: Location of the JSON payload.
        required: When ``False`` the function returns an empty mapping if the
            file is absent instead of raising ``FileNotFoundError``. This keeps
            optional inputs from aborting the digest run during local
            development and tests where only a subset of artefacts are
            generated.

    Raises:
        FileNotFoundError: If the file is missing and ``required`` is ``True``.
    """

    if not path.exists():
        if required:
            raise FileNotFoundError(f"缺少输入文件: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _coerce_themes(raw: object) -> List[Dict[str, object]]:
    themes: List[Dict[str, object]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            try:
                themes.append(ThemePayload.from_mapping(item).to_mapping())
            except ValueError:
                continue
    return themes


def _coerce_actions(raw: object) -> List[Dict[str, str]]:
    actions: List[Dict[str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            try:
                actions.append(ActionPayload.from_mapping(item).to_mapping())
            except ValueError:
                continue
    return actions


def _build_env() -> Environment:
    loaders = []
    if TEMPLATE_DIR.exists():
        loaders.append(FileSystemLoader(str(TEMPLATE_DIR)))
    loaders.append(PackageLoader("daily_messenger.digest", "templates"))
    loader = loaders[0] if len(loaders) == 1 else ChoiceLoader(loaders)
    return Environment(loader=loader, autoescape=select_autoescape(["html", "xml"]))


def _render_report(env: Environment, payload: Dict[str, object]) -> str:
    template = env.get_template("report.html.j2")
    return template.render(**payload)


def _filter_future_events(events: List[Dict[str, object]], today: date) -> List[Dict[str, object]]:
    future: List[Dict[str, object]] = []
    for entry in events:
        if not isinstance(entry, dict):
            continue
        raw_date = entry.get("date")
        if not raw_date:
            continue
        try:
            event_date = datetime.strptime(str(raw_date), "%Y-%m-%d").date()
        except ValueError:
            continue
        if event_date >= today:
            item = dict(entry)
            item["_date_obj"] = event_date
            future.append(item)
    future.sort(key=lambda item: item["_date_obj"])
    for item in future:
        item.pop("_date_obj", None)
    return future[:20]


def _build_summary_lines(themes: List[Dict[str, object]], actions: List[Dict[str, str]], degraded: bool) -> List[str]:
    lines = []
    if degraded:
        lines.append("⚠️ 数据延迟，以下为中性参考。")
    for theme in themes:
        label = theme.get("label", theme.get("name", "主题"))
        breakdown = theme.get("breakdown", {})
        detail = theme.get("breakdown_detail", {})
        meta = theme.get("meta", {})
        total = theme.get("total")
        if isinstance(total, (int, float)):
            line = f"{label} 总分 {total:.0f}"
        else:
            line = f"{label} 总分 —"
        delta = meta.get("delta")
        if isinstance(delta, (int, float)):
            line += f" (Δ {delta:+.1f})"
        fundamental = breakdown.get("fundamental")
        if isinstance(fundamental, (int, float)):
            line += f"｜基本面 {fundamental:.0f}"
        valuation_detail = detail.get("valuation", {})
        valuation_value = breakdown.get("valuation")
        if valuation_detail.get("fallback"):
            line += "｜估值 ∅"
        elif isinstance(valuation_value, (int, float)):
            line += f"｜估值 {valuation_value:.0f}"
        distance_to_add = meta.get("distance_to_add")
        if isinstance(distance_to_add, (int, float)):
            line += f"｜距增持 {distance_to_add:+.0f}"
        lines.append(line)
    if actions:
        for action in actions:
            lines.append(f"操作：{action['action']} {action['name']}（{action['reason']}）")
    return lines[:12]


def _build_card_payload(
    title: str,
    lines: List[str],
    report_url: str,
    *,
    news_preview: List[str] | None = None,
    stock_preview: List[str] | None = None,
) -> Dict[str, object]:
    content = "\n".join(lines)
    elements: List[Dict[str, object]] = [
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": content},
        }
    ]

    preview_chunks: List[str] = []
    if news_preview:
        preview_chunks.append("**新闻** " + " ｜ ".join(news_preview))
    if stock_preview:
        preview_chunks.append("**成分股** " + " ｜ ".join(stock_preview))
    if preview_chunks:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(preview_chunks)}})

    elements.append(
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "查看完整报告"},
                    "url": report_url,
                    "type": "default",
                }
            ],
        }
    )

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": elements,
    }


def run(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render daily digest")
    parser.add_argument("--degraded", action="store_true", help="强制输出降级版本")
    args = parser.parse_args(argv)

    logger = setup_logger("digest")
    started_at = datetime.now(timezone.utc)

    scores = _load_json(OUT_DIR / "scores.json")
    actions_payload = _load_json(OUT_DIR / "actions.json")
    raw_market_path = OUT_DIR / "raw_market.json"
    if raw_market_path.exists():
        raw_market_payload = _load_json(raw_market_path)
    else:
        log(
            logger,
            logging.WARNING,
            "digest_missing_input",
            input="raw_market",
            path=str(raw_market_path),
        )
        raw_market_payload = {}
    raw_events_path = OUT_DIR / "raw_events.json"
    if raw_events_path.exists():
        raw_events_payload = _load_json(raw_events_path)
    else:
        log(
            logger,
            logging.WARNING,
            "digest_missing_input",
            input="raw_events",
            path=str(raw_events_path),
        )
        raw_events_payload = {}

    degraded = bool(scores.get("degraded")) or args.degraded
    date_str = scores.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    try:
        report_date = datetime.strptime(str(date_str), "%Y-%m-%d").date()
    except ValueError:
        report_date = datetime.now(timezone.utc).date()

    logger = setup_logger("digest", date=date_str)
    log(logger, logging.INFO, "digest_start", degraded=degraded)
    run_meta.record_step(OUT_DIR, "digest", "started", date=date_str, degraded=degraded)

    themes = _coerce_themes(scores.get("themes"))
    actions = _coerce_actions(actions_payload.get("items"))
    events_future = _filter_future_events(scores.get("events", []), report_date)
    etl_status = scores.get("etl_status", {})
    etl_sources = []
    if isinstance(etl_status, dict):
        raw_sources = etl_status.get("sources", [])
        if isinstance(raw_sources, list):
            etl_sources = [src for src in raw_sources if isinstance(src, dict)]
    sentiment_candidate = scores.get("sentiment")
    sentiment_detail = sentiment_candidate if isinstance(sentiment_candidate, dict) else None
    thresholds_candidate = scores.get("thresholds", {})
    thresholds = thresholds_candidate if isinstance(thresholds_candidate, dict) else {}

    theme_details_candidate = scores.get("theme_details")
    if isinstance(theme_details_candidate, Mapping):
        theme_details = dict(theme_details_candidate)
    else:
        market_node = raw_market_payload.get("market", {}) if isinstance(raw_market_payload, Mapping) else {}
        details_node = market_node.get("themes") if isinstance(market_node, Mapping) else {}
        theme_details = dict(details_node) if isinstance(details_node, Mapping) else {}

    ai_updates_candidate = scores.get("ai_updates")
    if isinstance(ai_updates_candidate, list):
        ai_updates = [item for item in ai_updates_candidate if isinstance(item, Mapping)]
    else:
        candidate = raw_events_payload.get("ai_updates") if isinstance(raw_events_payload, Mapping) else []
        if not isinstance(candidate, list):
            candidate = []
        ai_updates = [item for item in candidate if isinstance(item, Mapping)]

    news_preview: List[str] = []
    for entry in ai_updates[:3]:
        title = str(entry.get("title", "更新"))
        url = entry.get("url")
        if isinstance(url, str) and url:
            news_preview.append(f"[{title}]({url})")
        else:
            news_preview.append(title)

    stock_preview: List[str] = []
    if theme_details:
        preferred_order = ["magnificent7", "ai", "btc"]
        selected_detail: Mapping[str, Any] | None = None
        for key in preferred_order:
            detail_candidate = theme_details.get(key) if isinstance(theme_details, Mapping) else None
            if isinstance(detail_candidate, Mapping) and detail_candidate.get("symbols"):
                selected_detail = detail_candidate
                break
        if selected_detail is None:
            for detail_candidate in theme_details.values():
                if isinstance(detail_candidate, Mapping) and detail_candidate.get("symbols"):
                    selected_detail = detail_candidate
                    break
        if selected_detail:
            symbols_list = selected_detail.get("symbols", [])
            if isinstance(symbols_list, list):
                sortable = []
                for item in symbols_list:
                    if isinstance(item, Mapping):
                        change_value = item.get("change_pct")
                        try:
                            change_float = float(change_value)
                        except (TypeError, ValueError):
                            change_float = 0.0
                        sortable.append((abs(change_float), item))
                for _, item in sorted(sortable, key=lambda pair: pair[0], reverse=True)[:3]:
                    symbol = item.get("symbol")
                    if not symbol:
                        continue
                    change_value = item.get("change_pct")
                    preview_text = str(symbol)
                    try:
                        change_float = float(change_value)
                    except (TypeError, ValueError):
                        change_float = None
                    if change_float is not None:
                        preview_text += f" {change_float:+.2f}%"
                    else:
                        price_value = item.get("price")
                        try:
                            price_float = float(price_value)
                        except (TypeError, ValueError):
                            price_float = None
                        if price_float is not None:
                            preview_text += f" {price_float:.2f}"
                    stock_preview.append(preview_text)

    raw_links = {"market": "raw_market.json", "events": "raw_events.json"}

    env = _build_env()

    payload = {
        "title": f"盘前播报{'（数据延迟）' if degraded else ''}",
        "date": date_str,
        "themes": themes,
        "actions": actions,
        "events": events_future,
        "etl_sources": etl_sources,
        "sentiment": sentiment_detail,
        "thresholds": thresholds,
        "metric_labels": METRIC_LABELS,
        "sentiment_labels": SENTIMENT_LABELS,
        "degraded": degraded,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "theme_details": theme_details,
        "ai_updates": ai_updates,
        "raw_links": raw_links,
    }

    html = _render_report(env, payload)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dated_report_path = OUT_DIR / f"{date_str}.html"
    dated_report_path.write_text(html, encoding="utf-8")
    (OUT_DIR / "index.html").write_text(html, encoding="utf-8")

    summary_lines = _build_summary_lines(payload["themes"], payload["actions"], degraded)
    summary_text = "\n".join(summary_lines)
    if summary_text:
        summary_text += "\n"
    (OUT_DIR / "digest_summary.txt").write_text(summary_text, encoding="utf-8")

    repo = os.getenv("GITHUB_REPOSITORY", "org/repo")
    owner, repo_name = repo.split("/") if "/" in repo else ("org", repo)
    report_url = f"https://{owner}.github.io/{repo_name}/{date_str}.html"

    card_title = f"内参 · 盘前{'（数据延迟）' if degraded else ''}"
    card_payload = _build_card_payload(
        card_title,
        summary_lines or ["今日暂无摘要"],
        report_url,
        news_preview=news_preview,
        stock_preview=stock_preview,
    )
    (OUT_DIR / "digest_card.json").write_text(json.dumps(card_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    log(
        logger,
        logging.INFO,
        "digest_complete",
        degraded=degraded,
        summary_lines=len(summary_lines),
        duration_seconds=round(duration, 2),
        summary_path=str(OUT_DIR / "digest_summary.txt"),
        card_path=str(OUT_DIR / "digest_card.json"),
    )
    if degraded:
        log(logger, logging.WARNING, "digest_degraded_output", reason="degraded flag or downstream status")
    run_meta.record_step(
        OUT_DIR,
        "digest",
        "completed",
        degraded=degraded,
        duration_seconds=round(duration, 2),
        summary_lines=len(summary_lines),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
