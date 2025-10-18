import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List

import pytest

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from daily_messenger import cli  # noqa: E402  (import after sys.path mutation)
from daily_messenger.common import run_meta  # noqa: E402
from daily_messenger.digest import make_daily  # noqa: E402
from daily_messenger.etl import run_fetch  # noqa: E402
from daily_messenger.scoring import run_scores  # noqa: E402


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _synthetic_raw_market(trading_day: str) -> Dict[str, object]:
    return {
        "date": trading_day,
        "market": {
            "indices": [
                {"symbol": "SPX", "close": 4825.0, "change_pct": 0.6},
                {"symbol": "NDX", "close": 16250.0, "change_pct": 0.8},
            ],
            "sectors": [
                {"name": "AI", "performance": 1.12},
                {"name": "Defensive", "performance": 0.96},
            ],
            "themes": {
                "ai": {
                    "performance": 1.12,
                    "change_pct": 0.7,
                    "avg_pe": 32.0,
                    "avg_ps": 6.5,
                    "symbols": [
                        {
                            "symbol": "NVDA",
                            "change_pct": 1.2,
                            "price": 900.0,
                            "pe": 45.0,
                            "ps": 20.5,
                            "pb": 18.2,
                            "market_cap": 2_200_000_000_000,
                        },
                        {
                            "symbol": "MSFT",
                            "change_pct": 0.8,
                            "price": 420.5,
                            "pe": 32.1,
                            "ps": 12.3,
                            "pb": 11.4,
                            "market_cap": 3_100_000_000_000,
                        },
                    ],
                },
                "magnificent7": {
                    "change_pct": 0.55,
                    "avg_pe": 31.0,
                    "avg_ps": 6.0,
                    "market_cap": 12_500_000_000_000,
                    "symbols": [
                        {
                            "symbol": "NVDA",
                            "change_pct": 1.2,
                            "price": 900.0,
                            "pe": 45.0,
                            "ps": 20.5,
                            "pb": 18.2,
                            "market_cap": 2_200_000_000_000,
                        },
                        {
                            "symbol": "META",
                            "change_pct": 0.9,
                            "price": 470.3,
                            "pe": 28.4,
                            "ps": 9.8,
                            "pb": 8.7,
                            "market_cap": 1_200_000_000_000,
                        },
                    ],
                },
            },
        },
        "btc": {
            "etf_net_inflow_musd": 35.2,
            "funding_rate": 0.012,
            "futures_basis": 0.0015,
        },
        "sentiment": {
            "put_call": {"equity": 0.72},
            "aaii": {"bull_bear_spread": -10.5},
        },
    }


def _synthetic_events(trading_day: str) -> Dict[str, object]:
    return {
        "events": [
            {"title": "宏观数据公布", "date": trading_day, "impact": "high"},
            {"title": "龙头财报", "date": trading_day, "impact": "medium"},
        ],
        "ai_updates": [
            {
                "title": "AI 监管观察",
                "url": "https://example.com/ai",
                "summary": "监管进展",
            }
        ],
    }


@dataclass
class PipelineRunner:
    run: Callable[..., Path]


@pytest.fixture
def pipeline_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PipelineRunner:
    out_dir = tmp_path / "out"
    state_dir = tmp_path / "state"

    monkeypatch.setattr(run_fetch, "OUT_DIR", out_dir)
    monkeypatch.setattr(run_fetch, "STATE_DIR", state_dir)
    monkeypatch.setattr(run_scores, "OUT_DIR", out_dir)
    monkeypatch.setattr(run_scores, "STATE_DIR", state_dir)
    monkeypatch.setattr(
        run_scores, "SENTIMENT_HISTORY_PATH", state_dir / "sentiment_history.json"
    )
    monkeypatch.setattr(
        run_scores, "SCORE_HISTORY_PATH", state_dir / "score_history.json"
    )
    monkeypatch.setattr(make_daily, "OUT_DIR", out_dir)

    def _invoke(
        *,
        trading_day: str = "2024-04-01",
        etl_ok: bool = True,
        cli_args: List[str] | None = None,
    ) -> Path:
        monkeypatch.setenv("DM_OVERRIDE_DATE", trading_day)
        monkeypatch.setenv("API_KEYS", "{}")
        monkeypatch.setenv("DM_RUN_ID", "test-run")
        monkeypatch.setenv("DM_DISABLE_THROTTLE", "1")

        def fake_fetch(argv: List[str] | None = None) -> int:
            out_dir.mkdir(parents=True, exist_ok=True)
            state_dir.mkdir(parents=True, exist_ok=True)
            _write_json(out_dir / "raw_market.json", _synthetic_raw_market(trading_day))
            _write_json(out_dir / "raw_events.json", _synthetic_events(trading_day))
            _write_json(
                out_dir / "etl_status.json",
                {
                    "date": trading_day,
                    "ok": etl_ok,
                    "sources": [
                        {
                            "name": "market",
                            "ok": etl_ok,
                            "message": "synthetic dataset",
                        },
                        {
                            "name": "coinbase_spot",
                            "ok": etl_ok,
                            "message": "synthetic dataset",
                        },
                        {
                            "name": "okx_funding",
                            "ok": etl_ok,
                            "message": "synthetic dataset",
                        },
                        {
                            "name": "okx_basis",
                            "ok": etl_ok,
                            "message": "synthetic dataset",
                        },
                    ],
                },
            )
            run_meta.record_step(
                out_dir,
                "etl",
                "completed",
                trading_day=trading_day,
                degraded=not etl_ok,
            )
            (state_dir / f"fetch_{trading_day}").write_text(
                trading_day, encoding="utf-8"
            )
            return 0

        monkeypatch.setattr(run_fetch, "run", fake_fetch)

        args = ["run", "--force-score"]
        if cli_args:
            args.extend(cli_args)
        exit_code = cli.main(args)
        if exit_code != 0:
            raise AssertionError(f"cli exited with {exit_code}")
        return out_dir

    return PipelineRunner(run=_invoke)
