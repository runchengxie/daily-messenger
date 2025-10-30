import importlib
import json
import sys
from datetime import datetime, timezone

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
        lambda trading_day: (
            {},
            module.FetchStatus(name="market_sim", ok=True, message="ok"),
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_coinbase_spot",
        lambda: (
            50000.0,
            module.FetchStatus(name="coinbase_spot", ok=True, message="ok"),
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_okx_funding",
        lambda: (0.001, module.FetchStatus(name="okx_funding", ok=True, message="ok")),
    )
    monkeypatch.setattr(
        module,
        "_fetch_okx_basis",
        lambda spot_price: (
            0.0,
            module.FetchStatus(name="okx_basis", ok=True, message="ok"),
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_btc_etf_flow",
        lambda api_keys: (
            12.5,
            module.FetchStatus(name="btc_etf_flow", ok=True, message="ok"),
        ),
    )
    monkeypatch.setattr(
        module,
        "_simulate_btc_theme",
        lambda trading_day: (
            {},
            module.FetchStatus(name="btc_sim", ok=True, message="ok"),
        ),
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
        lambda trading_day, api_keys: (
            [],
            module.FetchStatus(name="finnhub_earnings", ok=True, message="ok"),
        ),
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


def test_fetch_ai_market_news_generates_updates_glm(
    monkeypatch, load_run_fetch
):
    module = load_run_fetch(
        {
            "ai_news": {
                "provider": "glm",
                "model": "glm-4.6",
                "keys": ["PRIMARY_TOKEN"],
                "enable_network": False,
            }
        }
    )
    module.THROTTLE_DISABLED = True

    def fake_call(model, api_key, prompt, enable_network, timeout, thinking):
        assert model == "glm-4.6"
        assert api_key == "PRIMARY_TOKEN"
        assert thinking == "enabled"
        assert "<news>" in prompt
        return {
            "choices": [
                {
                    "message": {
                        "content": "<news>- 要点 A\n- 要点 B</news>",
                    }
                }
            ]
        }

    monkeypatch.setattr(module, "_call_glm_chat_completions", fake_call)

    now = datetime(2024, 4, 2, 10, 0, tzinfo=timezone.utc)
    updates, statuses = module._fetch_ai_market_news(
        now,
        {
            "ai_news": {
                "provider": "glm",
                "model": "glm-4.6",
                "keys": ["PRIMARY_TOKEN"],
                "enable_network": False,
            }
        },
        logger=None,
    )

    assert len(updates) == len(module.AI_NEWS_MARKET_SPECS)
    assert all(update["source"] == "glm" for update in updates)
    assert all(update["summary"] for update in updates)
    assert all(status.ok for status in statuses)


def test_fetch_ai_market_news_rotates_keys_gemini(monkeypatch, load_run_fetch):
    module = load_run_fetch(
        {
            "ai_news": {
                "provider": "gemini",
                "model": "gemini-test",
                "keys": ["PRIMARY_TOKEN", "BACKUP_TOKEN"],
                "enable_network": False,
            }
        }
    )
    module.THROTTLE_DISABLED = True

    call_counter = {"PRIMARY_TOKEN": 0, "BACKUP_TOKEN": 0}

    def fake_call(model, api_key, prompt, enable_network, timeout):
        call_counter[api_key] += 1
        if api_key == "PRIMARY_TOKEN":
            raise module.requests.HTTPError("quota exceeded")
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "<news>- OK</news>",
                            }
                        ]
                    }
                }
            ]
        }

    monkeypatch.setattr(module, "_call_gemini_generate_content", fake_call)

    now = datetime(2024, 4, 3, 12, 0, tzinfo=timezone.utc)
    updates, statuses = module._fetch_ai_market_news(
        now,
        {
            "ai_news": {
                "provider": "gemini",
                "model": "gemini-test",
                "keys": ["PRIMARY_TOKEN", "BACKUP_TOKEN"],
                "enable_network": False,
            }
        },
        logger=None,
    )

    assert len(updates) == len(module.AI_NEWS_MARKET_SPECS)
    assert call_counter["PRIMARY_TOKEN"] >= 1
    assert call_counter["BACKUP_TOKEN"] >= len(module.AI_NEWS_MARKET_SPECS)
    assert all(status.ok for status in statuses)


def test_resolve_edgar_user_agent_raises_on_blank(monkeypatch, load_run_fetch):
    module = load_run_fetch({})
    monkeypatch.setenv("EDGAR_USER_AGENT", "   ")

    with pytest.raises(RuntimeError):
        module._resolve_edgar_user_agent()


def test_resolve_ai_news_settings_env_fallback_gemini(monkeypatch, load_run_fetch):
    monkeypatch.delenv("API_KEYS", raising=False)
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro")
    monkeypatch.setenv("GEMINI_ENABLE_NETWORK", "true")
    monkeypatch.setenv("GEMINI_KEY_1", "ENV_KEY_A")
    monkeypatch.setenv("GEMINI_API_KEY_2", "ENV_KEY_B")

    module = load_run_fetch(None)
    settings = module._resolve_ai_news_settings(module.API_KEYS_CACHE)

    assert settings is not None
    assert settings.provider == "gemini"
    assert settings.model == "gemini-2.5-pro"
    assert settings.enable_network is True
    tokens = {token for _, token in settings.keys}
    assert {"ENV_KEY_A", "ENV_KEY_B"} <= tokens


def test_resolve_ai_news_settings_defaults_to_glm(monkeypatch, load_run_fetch):
    monkeypatch.delenv("API_KEYS", raising=False)
    monkeypatch.setenv("GLM_KEY_1", "GLM_KEY_PRIMARY")
    monkeypatch.setenv("GLM_ENABLE_NETWORK", "0")

    module = load_run_fetch(None)
    settings = module._resolve_ai_news_settings(module.API_KEYS_CACHE)

    assert settings is not None
    assert settings.provider == "glm"
    assert settings.model == "glm-4.6"
    assert settings.enable_network is False
    assert settings.thinking == "enabled"
    tokens = {token for _, token in settings.keys}
    assert "GLM_KEY_PRIMARY" in tokens
