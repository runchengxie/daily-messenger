import pytest

from daily_messenger.etl.fetchers import aaii_sentiment, cboe_putcall
from daily_messenger.scoring.adaptors import sentiment as sentiment_adaptor


class _DummyResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:  # noqa: D401 - test helper
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> object:
        import json

        return json.loads(self.text)


class _DummySession:
    def __init__(self, responses: dict[str, _DummyResponse]) -> None:
        self._responses = responses
        self.headers: dict[str, str] = {}

    def get(  # noqa: ANN001 - test double
        self,
        url: str,
        timeout: int | None = None,
        params: dict | None = None,
    ) -> _DummyResponse:
        key = url
        if params:
            key = f"{url}?{sorted(params.items())}"
        response = self._responses.get(key)
        if response is None:
            raise RuntimeError(f"no response for {key}")
        return response


def test_cboe_fetch_parses_ratios(monkeypatch: pytest.MonkeyPatch) -> None:
    base = "DATE,CALL,PUT,TOTAL,P/C Ratio\n10/10/2024,100,80,180,0.77\n"
    responses = {
        "https://www.cboe.com": _DummyResponse("ok"),
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv": _DummyResponse(base),
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/indexpc.csv": _DummyResponse(base.replace("0.77", "1.25")),
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/spxpc.csv": _DummyResponse(base.replace("0.77", "1.11")),
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/vixpc.csv": _DummyResponse(base.replace("0.77", "0.42")),
    }

    dummy_session = _DummySession(responses)
    monkeypatch.setattr(cboe_putcall.requests, "Session", lambda: dummy_session)

    payload, status = cboe_putcall.fetch()

    assert status.ok
    assert payload["put_call"]["equity"] == 0.77
    assert payload["put_call"]["index"] == 1.25
    assert payload["put_call"]["as_of_exchange_tz"] == "America/Chicago"


def test_aaii_fetch_parses_latest_row(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <table>
          <tr>
            <th>Reported Date</th>
            <th>Bullish</th>
            <th>Neutral</th>
            <th>Bearish</th>
          </tr>
          <tr>
            <td>October 10, 2024</td>
            <td>42.5%</td>
            <td>25.0%</td>
            <td>32.5%</td>
          </tr>
        </table>
      </body>
    </html>
    """

    rss = """<?xml version='1.0'?><rss><channel><item><title>October 10, 2024 AAII Sentiment Survey</title><link>https://insights.aaii.com/p/october-10-2024-aaii-sentiment</link></item></channel></rss>"""
    html = """
    <html>
      <body>
        <h1>October 10, 2024 AAII Sentiment Survey</h1>
        <p>Bullish sentiment registered 42.5% while Neutral investors were 25.0%.</p>
        <p>Bearish responses fell to 32.5%.</p>
      </body>
    </html>
    """
    responses = {
        aaii_sentiment.RSS_URL: _DummyResponse(rss),
        "https://insights.aaii.com/p/october-10-2024-aaii-sentiment": _DummyResponse(html),
    }

    dummy_session = _DummySession(responses)
    monkeypatch.setattr(aaii_sentiment.requests, "Session", lambda: dummy_session)

    payload, status = aaii_sentiment.fetch()

    assert status.ok
    assert payload["aaii"]["week"] == "2024-10-10"
    assert payload["aaii"]["bullish_pct"] == 42.5
    assert payload["aaii"]["bull_bear_spread"] == pytest.approx(10.0)


def test_sentiment_adaptor_combines_sources() -> None:
    history = {
        "put_call_equity": [0.6, 0.7, 0.8, 1.9],
        "aaii_bull_bear_spread": [10.0, 12.0, -5.0, -20.0],
    }
    sentiment_node = {"put_call": {"equity": history["put_call_equity"][-1]}, "aaii": {"bull_bear_spread": history["aaii_bull_bear_spread"][-1]}}
    result = sentiment_adaptor.aggregate(sentiment_node, history)
    assert result is not None
    assert 0.0 <= result.score <= 100.0
    assert "put_call" in result.components
    assert "aaii" in result.components


def test_sentiment_adaptor_handles_missing() -> None:
    result = sentiment_adaptor.aggregate({}, {})
    assert result is None
