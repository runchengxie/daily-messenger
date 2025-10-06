# 真实数据源接入建议

## 免费数据源速查表

在不额外付费的前提下，可以利用下列官方或提供免费层的 API，基本覆盖需求清单里的三大模块：

| 目标 | 免费数据源 | 说明 |
| --- | --- | --- |
| 美股/港股行情、板块代理、基础估值 | [Alpha Vantage](https://www.alphavantage.co/documentation/?utm_source=chatgpt.com)、[Financial Modeling Prep](https://site.financialmodelingprep.com/developer/docs?utm_source=chatgpt.com)、[Twelve Data](https://twelvedata.com/exchanges?utm_source=chatgpt.com)、[marketstack](https://marketstack.com/documentation?utm_source=chatgpt.com)、[Stooq](https://stooq.com/q/d/?s=^hsi&utm_source=chatgpt.com) | Alpha Vantage/FMP 可做美股与 ETF，Twelve Data/marketstack 覆盖港股 EOD，Stooq 提供恒指等指数做情绪或风格代理。 |
| 估值与财务报表 | [Financial Modeling Prep](https://site.financialmodelingprep.com/developer/docs?utm_source=chatgpt.com)、[SEC EDGAR](https://www.sec.gov/search-filings/edgar-application-programming-interfaces?utm_source=chatgpt.com) | FMP 免费层有常见估值比率，EDGAR 披露可自行计算更细指标。 |
| 宏观事件、非农、假期 | [Trading Economics](https://docs.tradingeconomics.com/?utm_source=chatgpt.com)、[FRED](https://fred.stlouisfed.org/docs/api/fred/?utm_source=chatgpt.com)、[Polygon Market Holidays](https://polygon.io/docs/rest/stocks/market-operations/market-holidays?utm_source=chatgpt.com) | Trading Economics 提供日历，FRED 给宏观时间序列，Polygon 或 Finnhub 可补交易日。 |
| 公司财报日历 | [Finnhub](https://finnhub.io/docs/api/earnings-calendar?utm_source=chatgpt.com) | 免费层每日限额，可直接并入事件流。 |
| AI 与比特币赛道行情 | [Coinbase](https://docs.cdp.coinbase.com/coinbase-business/track-apis/prices?utm_source=chatgpt.com)、[OKX](https://www.okx.com/docs-v5/en/?utm_source=chatgpt.com)、[Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History?utm_source=chatgpt.com)、[Deribit](https://docs.deribit.com/?utm_source=chatgpt.com)、[Farside Investors](https://farside.co.uk/btc/?utm_source=chatgpt.com) | Coinbase 现货、OKX/Binance/Deribit 的资金费率与永续价格覆盖币圈指标，Farside 提供 ETF 净流入。 |
| 情绪代理 | [Cboe Put/Call Ratio](https://www.cboe.com/us/options/market_statistics/historical_data/?utm_source=chatgpt.com)、[AAII Sentiment Survey](https://www.aaii.com/sentimentsurvey?utm_source=chatgpt.com) | 衍生品偏度与投资者问卷可作为股市情绪补充，币圈已有资金费率与基差。 |
| AI 大模型资讯 | 官方博客（如 [OpenAI](https://openai.com/news/?utm_source=chatgpt.com)、DeepMind、Anthropic、Meta AI）、[arXiv API](https://info.arxiv.org/help/api/index.html?utm_source=chatgpt.com) | 通过 RSS 或 API 白名单聚合官方动态，接入现有事件模块。 |

> 这些接口大多有调用频控或返回粒度限制，建议先在本地跑通流程，需求稳定后再考虑升级到更高配额或商用数据源。

参考下列数据供应商和搭配方案，可以让项目从“模拟/假数据”平滑迁移到可上线的真实行情：

## 指数 / 股票行情与板块代理

* **[Polygon.io](https://polygon.io/docs?utm_source=chatgpt.com)**：覆盖股票、期权、期货、外汇与指数，提供 REST 与 WebSocket 接口，连指数的分钟级聚合也能拉取。适合用各类 ETF 作为板块代理（例如 XAR/ITA 表示军工，XLK 表示科技）。
* **[Alpha Vantage](https://www.alphavantage.co/documentation/?utm_source=chatgpt.com)**：免费层即可获取股票 / ETF 的历史及日内 K 线，足够做原型，但频控较严。
* **[Tiingo](https://www.tiingo.com/?utm_source=chatgpt.com)**：EOD、日内行情、新闻与加密货币数据俱全，质量稳定，是纯免费方案的升级版。

## 比特币现货价格与交易所数据

* **[Coinbase Advanced Trade API](https://docs.cdp.coinbase.com/advanced-trade/docs/welcome?utm_source=chatgpt.com)**：官方现货行情与订单流，文档完整，还提供 SDK，适合拿来作为“现货腿”参与基差计算。

## 资金费率（Funding）与永续合约

* **[Binance USDⓈ-M 期货 API](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History?utm_source=chatgpt.com)**：`/fapi/v1/fundingRate` 提供历史资金费率，覆盖面广，官方示例多。
* **[Deribit 公共 API](https://docs.deribit.com/?utm_source=chatgpt.com)**：包含 “perpetual funding rate” 历史接口，适合做多交易所对比或对冲。

## 期货价格与基差（CME 比特币期货）

* **[CME Group 实时数据 API](https://www.cmegroup.com/market-data/real-time-futures-and-options-data-api.html?utm_source=chatgpt.com)**：官方实时行情流，是基差计算里“期货腿”的直接来源，但属于付费/许可数据。
* **[CME DataMine](https://www.cmegroup.com/market-data/datamine-api.html?utm_source=chatgpt.com)**：可下载官方历史结算、成交量等，适合算日度基差，同样需要购买授权。
* 如果仍在验证原型，可用 Coinbase 现货 + 任意能获取的 CME 延时/结算价跑通流程，之后再升级数据源。

## 比特币现货 ETF 资金流（净申赎）

* **[Farside Investors](https://farside.co.uk/btc/?utm_source=chatgpt.com)**：免费聚合多只美股比特币现货 ETF 的每日净流入/流出表格与图表，社区常用参考源。
* **[iShares IBIT 官方页](https://www.blackrock.com/us/individual/products/333011/ishares-bitcoin-trust?utm_source=chatgpt.com)**：发行方披露信息，可作为校准基准。
* 若需要更系统化的历史与多指标比对，可引入 Glassnode、Kaiko 等付费链上/市场数据商。

## 经济与事件日历（宏观 / 财报）

* **[Trading Economics Calendar API](https://tradingeconomics.com/api/calendar.aspx?utm_source=chatgpt.com)**：提供全球宏观日历，REST 接口简单易用。
* **[Finnhub](https://finnhub.io/docs/api/earnings-calendar?utm_source=chatgpt.com)**：既有财报日历，也包含全球交易所假期接口，可拼成项目所需的“事件日历”模块。

## 交易日 / 市场开闭市判断

* **[Polygon Market Holidays](https://polygon.io/docs/rest/stocks/market-operations/market-holidays?utm_source=chatgpt.com)**：`/v1/marketstatus/upcoming` 返回未来假期与开闭市时间，用于保护定时任务。
* **IEX / Finnhub**：可作为假期、交易日的备援数据源。

## 落地拼装建议

1. **先用免费或低成本源跑通 ETL**：例如现货用 Coinbase，ETF 流用 Farside，宏观日历用 Trading Economics，假期信息用 Polygon。

2. **完成三大核心指标**：

   * **板块 / 主题表现**：拉取对应 ETF 的收盘或日内数据并聚合（如科技 = XLK，军工 = ITA/XAR，AI = BOTZ/ROBO）。

   * **资金费率**：使用 Binance 或 Deribit 的历史 funding rate 构建时间序列。

   * **期货基差**：基差 = 期货近月价格 − 现货价格；年化基差 = (期货 / 现货 − 1) × 365 / 到期天数。期货价格来自 CME 实时或结算数据，现货价格来自 Coinbase。

3. **需求稳定后升级为专业付费源**：逐步将 ETF/股票数据迁移至 Polygon/Tiingo，将期货数据升级为 CME 官方流或数据集，提前处理好授权与法务流程。
