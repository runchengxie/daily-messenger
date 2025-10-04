#!/usr/bin/env python3
"""Render HTML report, plain-text digest, and Feishu card payload."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "out"
TEMPLATE_DIR = BASE_DIR / "digest" / "templates"


def _load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"缺少输入文件: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_env() -> Environment:
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    loader = FileSystemLoader(str(TEMPLATE_DIR))
    return Environment(loader=loader, autoescape=select_autoescape(["html", "xml"]))


def _ensure_templates(env: Environment) -> None:
    index_template = TEMPLATE_DIR / "report.html.j2"
    if not index_template.exists():
        index_template.write_text(
            """<!DOCTYPE html>
<html lang=\"zh\">
<head>
  <meta charset=\"utf-8\">
  <title>{{ title }}</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <style>
    body { font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; margin: 2rem; color: #1f2933; }
    h1 { color: #2563eb; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }
    th, td { border: 1px solid #d1d5db; padding: 0.5rem; text-align: left; }
    th { background: #eff6ff; }
    .degraded { color: #d97706; font-weight: 600; }
  </style>
</head>
<body>
  <h1>{{ title }}</h1>
  <p>日期：{{ date }}</p>
  {% if degraded %}
  <p class=\"degraded\">⚠️ 数据存在缺口，以下结论仅供参考，请谨慎操作。</p>
  {% endif %}
  <section>
    <h2>主题评分</h2>
    <table>
      <thead>
        <tr>
          <th>主题</th>
          <th>总分</th>
          <th>基本面</th>
          <th>估值</th>
          <th>情绪</th>
          <th>资金</th>
          <th>事件</th>
        </tr>
      </thead>
      <tbody>
      {% for theme in themes %}
        <tr>
          <td>{{ theme.label }}</td>
          <td>{{ '%.1f' | format(theme.total) }}</td>
          <td>{{ theme.breakdown.fundamental | round(1) }}</td>
          <td>{{ theme.breakdown.valuation | round(1) }}</td>
          <td>{{ theme.breakdown.sentiment | round(1) }}</td>
          <td>{{ theme.breakdown.liquidity | round(1) }}</td>
          <td>{{ theme.breakdown.event | round(1) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </section>
  <section>
    <h2>操作建议</h2>
    <ul>
    {% for action in actions %}
      <li><strong>{{ action.action }}</strong> {{ action.name }} —— {{ action.reason }}</li>
    {% endfor %}
    </ul>
  </section>
  <section>
    <h2>未来事件</h2>
    {% if events %}
      <ul>
      {% for event in events %}
        <li>{{ event.date }} · {{ event.title }} · 影响级别：{{ event.impact }}</li>
      {% endfor %}
      </ul>
    {% else %}
      <p>暂无可用事件数据。</p>
    {% endif %}
  </section>
  <footer>
    <p>报告自动生成时间：{{ generated_at }}</p>
  </footer>
</body>
</html>
""",
            encoding="utf-8",
        )


def _render_report(env: Environment, payload: Dict[str, object]) -> str:
    template = env.get_template("report.html.j2")
    return template.render(**payload)


def _build_summary_lines(themes: List[Dict[str, object]], actions: List[Dict[str, str]], degraded: bool) -> List[str]:
    lines = []
    if degraded:
        lines.append("⚠️ 数据延迟，以下为中性参考。")
    for theme in themes:
        lines.append(f"{theme['label']} 总分 {theme['total']:.0f} ｜ 基本面 {theme['breakdown']['fundamental']:.0f}")
    if actions:
        for action in actions:
            lines.append(f"操作：{action['action']} {action['name']}（{action['reason']}）")
    return lines[:12]


def _build_card_payload(title: str, lines: List[str], report_url: str) -> Dict[str, object]:
    content = "\n".join(lines)
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": content},
            },
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
            },
        ],
    }


def run(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render daily digest")
    parser.add_argument("--degraded", action="store_true", help="强制输出降级版本")
    args = parser.parse_args(argv)

    scores = _load_json(OUT_DIR / "scores.json")
    actions_payload = _load_json(OUT_DIR / "actions.json")

    degraded = bool(scores.get("degraded")) or args.degraded
    date_str = scores.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    env = _build_env()
    _ensure_templates(env)

    payload = {
        "title": f"盘前播报{'（数据延迟）' if degraded else ''}",
        "date": date_str,
        "themes": scores.get("themes", []),
        "actions": actions_payload.get("items", []),
        "events": scores.get("events", []),
        "degraded": degraded,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
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
    card_payload = _build_card_payload(card_title, summary_lines or ["今日暂无摘要"], report_url)
    (OUT_DIR / "digest_card.json").write_text(json.dumps(card_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("报告已生成，包括 HTML、摘要与卡片 JSON。")
    if degraded:
        print("当前处于降级模式，提醒用户谨慎操作。")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
