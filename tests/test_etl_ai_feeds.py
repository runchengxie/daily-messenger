import importlib
import json
import sys

import pytest


@pytest.fixture
def load_run_fetch(monkeypatch):
    modules_cache = sys.modules

    def _loader(api_keys):
        if api_keys is None:
            monkeypatch.delenv("API_KEYS", raising=False)
        else:
            monkeypatch.setenv("API_KEYS", json.dumps(api_keys))
        module_name = "daily_messenger.etl.run_fetch"
        if module_name in modules_cache:
            module = importlib.reload(modules_cache[module_name])
        else:
            module = importlib.import_module(module_name)
        return module

    return _loader


class DummyResponse:
    def __init__(self, content: str, status_code: int = 200):
        self.content = content.encode("utf-8")
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise Exception(f"status {self.status_code}")


def test_fetch_ai_rss_events_parses_items(monkeypatch, load_run_fetch):
    module = load_run_fetch(
        {
            "ai_feeds": [
                "https://example.com/rss",  # success
                "https://example.com/fail",  # failure
            ]
        }
    )

    rss_payload = """
        <rss><channel>
            <item>
                <title>OpenAI Update</title>
                <pubDate>Mon, 01 Apr 2024 10:00:00 GMT</pubDate>
                <link>https://example.com/rss</link>
            </item>
            <item>
                <title>Another Story</title>
                <pubDate>Tue, 02 Apr 2024 12:00:00 GMT</pubDate>
                <link>https://example.com/rss-2</link>
            </item>
        </channel></rss>
    """

    def fake_get(url, headers=None, timeout=None):
        if "fail" in url:
            raise module.requests.RequestException("boom")
        return DummyResponse(rss_payload)

    monkeypatch.setattr(module.requests, "get", fake_get)

    events, statuses = module._fetch_ai_rss_events(module.AI_NEWS_FEEDS)
    assert len(events) == 2
    assert events[0]["source"] == "https://example.com/rss"
    assert events[0]["url"] == "https://example.com/rss"
    assert any(not status.ok for status in statuses)
    assert any(status.ok for status in statuses)


def test_fetch_arxiv_events_parses_entries(monkeypatch, load_run_fetch):
    module = load_run_fetch({})

    feed_payload = """
        <feed xmlns="http://www.w3.org/2005/Atom">
            <entry>
                <title>Sample Paper</title>
                <updated>2024-04-01T08:00:00Z</updated>
            </entry>
        </feed>
    """

    def fake_get(url, params=None, headers=None, timeout=None):
        assert params["search_query"] == module.ARXIV_QUERY_PARAMS["search_query"]
        return DummyResponse(feed_payload)

    monkeypatch.setattr(module.requests, "get", fake_get)

    events, status = module._fetch_arxiv_events(module.ARXIV_QUERY_PARAMS, 0)
    assert status.ok
    assert events[0]["title"].startswith("arXiv: Sample Paper")
    assert events[0]["date"] == "2024-04-01"


def test_run_includes_ai_sources(tmp_path, monkeypatch, load_run_fetch):
    module = load_run_fetch(
        {
            "ai_feeds": ["https://example.com/rss"],
            "arxiv": {
                "search_query": "cat:cs.AI",
                "max_results": 1,
                "sort_by": "submittedDate",
                "sort_order": "descending",
                "throttle_seconds": 0,
            },
        }
    )

    out_dir = tmp_path / "out"
    monkeypatch.setattr(module, "OUT_DIR", out_dir)

    monkeypatch.setattr(
        module,
        "_fetch_market_snapshot_real",
        lambda api_keys: ({}, module.FetchStatus(name="market", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_simulate_market_snapshot",
        lambda trading_day: ({}, module.FetchStatus(name="market_sim", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_coinbase_spot",
        lambda: (50000.0, module.FetchStatus(name="coinbase_spot", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_okx_funding",
        lambda: (0.001, module.FetchStatus(name="okx_funding", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_okx_basis",
        lambda spot_price: (0.0, module.FetchStatus(name="okx_basis", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_btc_etf_flow",
        lambda api_keys: (12.5, module.FetchStatus(name="btc_etf_flow", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_simulate_btc_theme",
        lambda trading_day: ({}, module.FetchStatus(name="btc_sim", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_events_real",
        lambda trading_day, api_keys: (
            [
                {
                    "title": "宏观事件",
                    "date": "2024-04-01",
                    "impact": "high",
                }
            ],
            module.FetchStatus(name="events", ok=True, message="ok"),
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_finnhub_earnings",
        lambda trading_day, api_keys: ([], module.FetchStatus(name="finnhub_earnings", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_ai_rss_events",
        lambda feeds: (
            [
                {
                    "title": "RSS Event",
                    "date": "2024-04-02",
                    "impact": "medium",
                    "source": feeds[0],
                    "url": "https://example.com/rss-item",
                }
            ],
            [module.FetchStatus(name="ai_rss_1", ok=True, message="ok")],
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_arxiv_events",
        lambda params, throttle: (
            [
                {
                    "title": "arXiv: Paper",
                    "date": "2024-04-03",
                    "impact": "low",
                    "source": "arxiv",
                }
            ],
            module.FetchStatus(name="arxiv", ok=True, message="ok"),
        ),
    )

    result = module.run([])
    assert result == 0

    raw_events_path = out_dir / "raw_events.json"
    assert raw_events_path.exists()
    payload = json.loads(raw_events_path.read_text(encoding="utf-8"))
    titles = {event["title"] for event in payload["events"]}
    assert "宏观事件" in titles
    assert "RSS Event" in titles
    assert "arXiv: Paper" in titles
    assert payload["ai_updates"][0]["title"] == "RSS Event"
