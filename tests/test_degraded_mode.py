import json
from pathlib import Path

import pytest


@pytest.mark.cli_pipeline
def test_pipeline_records_degraded_state(pipeline_runner) -> None:
    out_dir: Path = pipeline_runner.run(etl_ok=False, cli_args=["--degraded"])

    etl_status = json.loads((out_dir / "etl_status.json").read_text(encoding="utf-8"))
    assert etl_status["ok"] is False

    meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
    digest_meta = meta.get("steps", {}).get("digest", {})
    assert digest_meta.get("degraded") is True

    scores = json.loads((out_dir / "scores.json").read_text(encoding="utf-8"))
    assert "degraded" in scores
