#!/usr/bin/env python3
from __future__ import annotations

# Refresh the CLI help snippet embedded in README.md.

import re
import subprocess
import sys
from pathlib import Path


MARKER_START = "<!-- cli-help:start -->"
MARKER_END = "<!-- cli-help:end -->"


def _get_cli_help() -> str:
    result = subprocess.run(
        [sys.executable, "-m", "daily_messenger.cli", "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    readme_path = repo_root / "README.md"
    content = readme_path.read_text(encoding="utf-8")

    if MARKER_START not in content or MARKER_END not in content:
        raise SystemExit("README.md 缺少 CLI help 标记块")

    help_text = _get_cli_help()
    block = "```text\n" + help_text + "\n```"

    pattern = re.compile(
        rf"{re.escape(MARKER_START)}.*?{re.escape(MARKER_END)}",
        flags=re.DOTALL,
    )
    replacement = f"{MARKER_START}\n{block}\n{MARKER_END}"
    updated = pattern.sub(replacement, content)
    readme_path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
