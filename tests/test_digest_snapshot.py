from pathlib import Path

import pytest


@pytest.fixture
def html_report(pipeline_runner) -> str:
    out_dir: Path = pipeline_runner.run()
    return (out_dir / "index.html").read_text(encoding="utf-8")


def test_digest_contains_key_sections(html_report: str) -> None:
    assert "<title>盘前播报" in html_report
    for snippet in ["主题评分", "建议动作", "原始产物"]:
        assert snippet in html_report
