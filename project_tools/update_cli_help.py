#!/usr/bin/env python3
from __future__ import annotations

# Refresh (or validate) the CLI help snippet embedded in README.md.

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


MARKER_START = "<!-- cli-help:start -->"
MARKER_END = "<!-- cli-help:end -->"
CLI_SUBCOMMANDS: Tuple[str, ...] = ("run", "fetch", "score", "digest")


def _collect_help(argv: Sequence[str]) -> str:
    """Return help text for the provided argv fragment."""
    result = subprocess.run(
        [sys.executable, "-m", "daily_messenger.cli", *argv, "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _render_sections(sections: Iterable[Tuple[str, str]]) -> str:
    rows: List[str] = []
    for heading, body in sections:
        rows.append(f"$ {heading}\n{body}")
    return "\n\n".join(rows)


def _build_help_block() -> str:
    sections: List[Tuple[str, str]] = []
    sections.append(("dm --help", _collect_help([])))
    for command in CLI_SUBCOMMANDS:
        sections.append((f"dm {command} --help", _collect_help([command])))
    return "```text\n" + _render_sections(sections) + "\n```"


def _replace_block(content: str, block: str) -> str:
    pattern = re.compile(
        rf"{re.escape(MARKER_START)}.*?{re.escape(MARKER_END)}",
        flags=re.DOTALL,
    )
    return pattern.sub(f"{MARKER_START}\n{block}\n{MARKER_END}", content)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update README CLI help snippet")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write changes; exit with non-zero status if README differs.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    readme_path = repo_root / "README.md"
    content = readme_path.read_text(encoding="utf-8")

    if MARKER_START not in content or MARKER_END not in content:
        raise SystemExit("README.md 缺少 CLI help 标记块")

    block = _build_help_block()
    updated = _replace_block(content, block)

    if args.check:
        if updated != content:
            sys.stderr.write("README.md CLI help snippet is out of date.\n")
            raise SystemExit(1)
        return

    readme_path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
