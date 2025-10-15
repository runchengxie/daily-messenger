import json
from pathlib import Path

import pytest


@pytest.mark.cli_pipeline
def test_cli_pipeline_smoke(pipeline_runner) -> None:
    out_dir: Path = pipeline_runner.run()

    expected_artifacts = [
        "raw_market.json",
        "raw_events.json",
        "etl_status.json",
        "scores.json",
        "actions.json",
        "index.html",
        "digest_summary.txt",
        "digest_card.json",
        "run_meta.json",
    ]
    for name in expected_artifacts:
        assert (out_dir / name).exists(), f"missing artifact: {name}"

    meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    assert meta.get("run_id"), "run_meta must include run_id"
    steps = meta.get("steps", {})
    for step in ("etl", "scoring", "digest"):
        assert steps.get(step, {}).get("status") == "completed"

    scores = json.loads((out_dir / "scores.json").read_text(encoding="utf-8"))
    assert scores.get("themes"), "scores.json must contain at least one theme"
    for theme in scores["themes"]:
        assert {"name", "label", "total", "breakdown", "weights"} <= theme.keys()

    actions = json.loads((out_dir / "actions.json").read_text(encoding="utf-8"))
    assert isinstance(actions.get("items"), list)

    card_payload = json.loads((out_dir / "digest_card.json").read_text(encoding="utf-8"))
    assert card_payload["header"]["title"]["content"].startswith("内参")
    assert card_payload["elements"], "Feishu card should include body elements"


def test_cli_pipeline_run_respects_custom_date(pipeline_runner) -> None:
    target_date = "2024-04-10"
    out_dir: Path = pipeline_runner.run(trading_day=target_date)

    scores = json.loads((out_dir / "scores.json").read_text(encoding="utf-8"))
    assert scores["date"] == target_date

    summary_text = (out_dir / "digest_summary.txt").read_text(encoding="utf-8")
    assert target_date in (out_dir / f"{target_date}.html").read_text(encoding="utf-8")
    assert summary_text.strip(), "digest summary should not be empty"
