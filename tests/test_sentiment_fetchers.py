import pytest

from etl.fetchers import aaii_sentiment, cboe_putcall
from scoring.adaptors import sentiment as sentiment_adaptor


class _DummyResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:  # noqa: D401 - test helper
        return None


def test_cboe_fetch_parses_ratios(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <table>
          <tr><th>Label</th><th>Value</th></tr>
          <tr><td>EQUITY PUT/CALL RATIO</td><td>0.77</td></tr>
          <tr><td>INDEX PUT/CALL RATIO</td><td>1.25</td></tr>
          <tr><td>SPX + SPXW PUT/CALL RATIO</td><td>1.11</td></tr>
          <tr><td>CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO</td><td>0.42</td></tr>
        </table>
      </body>
    </html>
    """

    def fake_get(url: str, headers: dict, timeout: int) -> _DummyResponse:  # noqa: ANN001
        assert "cboe.com" in url
        return _DummyResponse(html)

    monkeypatch.setattr(cboe_putcall.requests, "get", fake_get)

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

    def fake_get(url: str, headers: dict, timeout: int) -> _DummyResponse:  # noqa: ANN001
        assert "aaii.com" in url
        return _DummyResponse(html)

    monkeypatch.setattr(aaii_sentiment.requests, "get", fake_get)

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
