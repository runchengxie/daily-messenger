from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from contextlib import contextmanager
from typing import List, Optional

from daily_messenger.common.logging import log, setup_logger
from daily_messenger.digest import make_daily
from daily_messenger.etl import run_fetch
from daily_messenger.scoring import run_scores


@contextmanager
def _env_override(key: str, value: Optional[str]) -> None:
    original = os.environ.get(key)
    if value is None:
        yield
        return
    os.environ[key] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original


def _ensure_run_id() -> None:
    os.environ.setdefault("DM_RUN_ID", uuid.uuid4().hex)


def _execute_step(name: str, func, args: Optional[List[str]], logger: logging.Logger) -> int:
    log(logger, logging.INFO, "cli_step_start", step=name, argv=args or [])
    code = func(args)
    level = logging.INFO if code == 0 else logging.ERROR
    log(logger, level, "cli_step_complete", step=name, exit_code=code)
    return code


def main(argv: Optional[List[str]] = None) -> int:
    _ensure_run_id()

    parser = argparse.ArgumentParser(prog="dm", description="Daily Messenger CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run ETL, scoring, and digest sequentially")
    run_parser.add_argument("--date", help="Override trading day (YYYY-MM-DD)")
    run_parser.add_argument("--force-fetch", action="store_true", help="Force refresh ETL step")
    run_parser.add_argument("--force-score", action="store_true", help="Force recompute scoring step")
    run_parser.add_argument("--degraded", action="store_true", help="Render digest in degraded mode")
    run_parser.add_argument("--strict", action="store_true", help="Enable STRICT mode during scoring")
    run_parser.add_argument("--disable-throttle", action="store_true", help="Disable network throttling helpers")

    fetch_parser = subparsers.add_parser("fetch", help="Run ETL only")
    fetch_parser.add_argument("--date", help="Override trading day (YYYY-MM-DD)")
    fetch_parser.add_argument("--force", action="store_true", help="Force refresh ETL step")
    fetch_parser.add_argument("--disable-throttle", action="store_true", help="Disable network throttling helpers")

    score_parser = subparsers.add_parser("score", help="Run scoring only")
    score_parser.add_argument("--date", help="Override trading day (YYYY-MM-DD)")
    score_parser.add_argument("--force", action="store_true", help="Force recompute scoring")
    score_parser.add_argument("--strict", action="store_true", help="Enable STRICT mode")

    digest_parser = subparsers.add_parser("digest", help="Render digest only")
    digest_parser.add_argument("--date", help="Override trading day (YYYY-MM-DD)")
    digest_parser.add_argument("--degraded", action="store_true", help="Render in degraded mode")

    args = parser.parse_args(argv)
    logger = setup_logger("cli", command=args.command)

    if getattr(args, "disable_throttle", False):
        os.environ["DM_DISABLE_THROTTLE"] = "1"

    if args.command == "fetch":
        with _env_override("DM_OVERRIDE_DATE", args.date):
            code = _execute_step(
                "etl",
                run_fetch.run,
                ["--force"] if args.force else [],
                logger,
            )
        return code

    if args.command == "score":
        with _env_override("DM_OVERRIDE_DATE", args.date), _env_override("STRICT", "1" if args.strict else None):
            step_args = ["--force"] if args.force else []
            code = _execute_step("scoring", run_scores.run, step_args, logger)
        return code

    if args.command == "digest":
        with _env_override("DM_OVERRIDE_DATE", args.date):
            digest_args: List[str] = []
            if args.degraded:
                digest_args.append("--degraded")
            code = _execute_step("digest", make_daily.run, digest_args, logger)
        return code

    # command == run
    exit_code = 0
    with _env_override("DM_OVERRIDE_DATE", args.date):
        fetch_args = ["--force"] if args.force_fetch else []
        exit_code = _execute_step("etl", run_fetch.run, fetch_args, logger)
        if exit_code != 0:
            return exit_code

        score_args = ["--force"] if args.force_score else []
        with _env_override("STRICT", "1" if args.strict else None):
            exit_code = _execute_step("scoring", run_scores.run, score_args, logger)
        if exit_code != 0:
            return exit_code

        digest_args: List[str] = []
        if args.degraded:
            digest_args.append("--degraded")
        exit_code = _execute_step("digest", make_daily.run, digest_args, logger)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
