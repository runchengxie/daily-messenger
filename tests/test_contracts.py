import json
from pathlib import Path

import pytest


@pytest.fixture
def artifacts_dir(pipeline_runner) -> Path:
    return pipeline_runner.run()


@pytest.mark.contract
def test_etl_status_contract(artifacts_dir: Path) -> None:
    payload = json.loads((artifacts_dir / "etl_status.json").read_text(encoding="utf-8"))
    assert {"date", "ok", "sources"} <= payload.keys()
    assert isinstance(payload["sources"], list) and payload["sources"], "sources should list fetchers"
    for source in payload["sources"]:
        assert {"name", "ok", "message"} <= source.keys()


@pytest.mark.contract
def test_scores_contract(artifacts_dir: Path) -> None:
    payload = json.loads((artifacts_dir / "scores.json").read_text(encoding="utf-8"))
    assert {"date", "degraded", "themes"} <= payload.keys()
    assert isinstance(payload["themes"], list) and payload["themes"], "themes must not be empty"

    first_theme = payload["themes"][0]
    assert {"name", "label", "total", "breakdown", "weights"} <= first_theme.keys()

    breakdown = first_theme["breakdown"]
    assert isinstance(breakdown, dict) and breakdown, "breakdown should contain factor scores"

    optional_keys = {"theme_details", "ai_updates", "config_version", "config_changed_at", "sentiment"}
    assert optional_keys.issuperset(
        set(payload.keys()) - {"date", "degraded", "themes", "events", "thresholds", "etl_status"}
    )
