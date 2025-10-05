# Daily Messenger (每日简报)

一个自动化的每日市场简报，专为 GitHub Actions 和 GitHub Pages 部署而设计。该应用模拟了需求列表中描述的 ETL → 评分 → 报告 → 飞书通知的工作流，并已准备好接入真实的数据源。

目前该项目定期抓取（默认抓取每日咨询，每天抓取一次）的信息流如下：

1. 美股/港股基本面，各个主要行业板块的情况，估值逻辑，情绪面；

2. 美国宏观相关资讯，非农，公司财报，AI大模型动态等；

3. 实盘相关信息，主要关注人工智能和比特币等数字货币相关赛道，以及其他类似的观察仓，比如美股Magnificent 7剧透

## 仓库结构

```tree
repo/
  etl/                 # 数据抓取脚本
  scoring/             # 评分逻辑
  digest/              # HTML/文本/卡片渲染
  tools/               # 工具脚本 (飞书推送)
  config/              # 可配置的权重和阈值
  data/                # 可选的历史数据快照
  state/               # 幂等性标记
  out/                 # 构建产物
  .github/workflows/   # CI 定义
```

## 快速开始

1. **安装依赖**

    使用 [uv](https://github.com/astral-sh/uv) (推荐):

    ```bash
    uv sync
    ```

    或者使用 virtualenv + pip:

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

2. **在本地运行流水线**

    ```bash
    uv run python etl/run_fetch.py
    uv run python scoring/run_scores.py --force
    uv run python digest/make_daily.py
    ```

    如果你使用 pip 的工作流程，请将 `uv run` 替换为 `python`。

    生成的文件将位于 `out/` 目录下。如果你想测试飞书消息发送，请准备一个测试用的 Webhook 并运行：

    ```bash
    export FEISHU_WEBHOOK=https://open.feishu.cn/xxx
    python tools/post_feishu.py --webhook "$FEISHU_WEBHOOK" --summary out/digest_summary.txt --card out/digest_card.json
    ```

3. **重置幂等性**

    评分步骤会写入 `state/done_YYYY-MM-DD` 文件。删除该文件，或运行 `python scoring/run_scores.py --force` 来重新生成当天的报告。

### 配置真实数据 API

`etl/run_fetch.py` 会优先尝试调用真实的数据源：

- Alpha Vantage：用于抓取 SPX/NDX 指数代理与 AI/防御板块 ETF 的最新收盘价与涨跌幅。
- Twelve Data：覆盖恒生指数等港股行情，补足国际视角。
- Financial Modeling Prep：拉取 NVDA/MSFT 等主题篮子的估值比率与涨跌表现，驱动 AI 与 Magnificent 7 主题打分。
- Coinbase / OKX：分别提供 BTC 现货、永续合约资金费率与基差。
- Farside Investors：解析现货 ETF 表格获取最近一天的净申赎额。
- Trading Economics + Finnhub：拉取宏观事件与未来一周的财报日历，二者合并为统一事件流。

将上游凭证序列化为 JSON 放入 `API_KEYS` 环境变量即可：

```bash
export API_KEYS='{"alpha_vantage": "YOUR_ALPHA_KEY", "trading_economics": "user:password", "twelve_data": "TD_KEY", "financial_modeling_prep": "FMP_KEY", "finnhub": "FINNHUB_TOKEN"}'
```

如果缺少密钥或接口超时，脚本会自动回退到内置的模拟数据，并在 `out/etl_status.json` 中标记失败项。

## 运行测试

首先安装用于开发的额外依赖，然后使用 `uv run` (或在已激活的虚拟环境中直接使用 `pytest`) 来执行覆盖核心评分和简报生成助手的轻量级单元测试：

```bash
uv sync --extra dev
uv run pytest
```

这些测试会验证分数权重逻辑、生成的行为标签，以及用于构建飞书消息内容的摘要/卡片生成器。

## GitHub Actions 自动化

定义在 `.github/workflows/daily.yml` 中的工作流会在每个工作日的 UTC 时间 14:00 (PT 时间 07:00) 运行。它会执行以下步骤：

1. 检出代码并初始化 Python 环境
2. 使用 pip 缓存安装依赖
3. 运行 ETL → 评分 → 简报生成脚本
4. 将 `out/` 目录作为 GitHub Pages 构建产物上传并部署
5. 发送飞书交互式卡片

需要配置以下 Secrets：

- `FEISHU_WEBHOOK`: 飞书自定义机器人的 Webhook URL
- `FEISHU_SECRET` (可选): 签名密钥 (如果启用)
- `API_KEYS`: 包含上游 API 凭证的 JSON 字符串 (支持占位符)，建议包含 `alpha_vantage`、`twelve_data`、`financial_modeling_prep`、`trading_economics`、`finnhub` 等键名

## 失败与降级处理

- 如果 ETL 失败或原始文件丢失，评分步骤将回退到中性分数，并且生成的简报会被标记为降级状态。
- 可以通过运行 `python digest/make_daily.py --degraded` 强制让简报生成步骤进入降级模式。
- 即使上游任务失败，GitHub Actions 仍会继续发送降级后的通知。

## 问题排查

| 现象 | 解决方法 |
| --- | --- |
| 原始数据文件 `FileNotFoundError` | 确保在运行评分/简报步骤之前，`etl/run_fetch.py` 已成功执行。 |
| 简报显示的是旧内容 | 删除 `state/done_YYYY-MM-DD` 文件，并使用 `--force` 选项重新运行评分脚本。 |
| 飞书 Webhook 拒绝请求 | 仔细检查签名密钥，并确保机器人已开启接收交互式卡片消息的权限。 |

## 真实数据源接入建议

### 免费数据源速查表

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

### 指数 / 股票行情与板块代理

- **[Polygon.io](https://polygon.io/docs?utm_source=chatgpt.com)**：覆盖股票、期权、期货、外汇与指数，提供 REST 与 WebSocket 接口，连指数的分钟级聚合也能拉取。适合用各类 ETF 作为板块代理（例如 XAR/ITA 表示军工，XLK 表示科技）。
- **[Alpha Vantage](https://www.alphavantage.co/documentation/?utm_source=chatgpt.com)**：免费层即可获取股票 / ETF 的历史及日内 K 线，足够做原型，但频控较严。
- **[Tiingo](https://www.tiingo.com/?utm_source=chatgpt.com)**：EOD、日内行情、新闻与加密货币数据俱全，质量稳定，是纯免费方案的升级版。

### 比特币现货价格与交易所数据

- **[Coinbase Advanced Trade API](https://docs.cdp.coinbase.com/advanced-trade/docs/welcome?utm_source=chatgpt.com)**：官方现货行情与订单流，文档完整，还提供 SDK，适合拿来作为“现货腿”参与基差计算。

### 资金费率（Funding）与永续合约

- **[Binance USDⓈ-M 期货 API](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History?utm_source=chatgpt.com)**：`/fapi/v1/fundingRate` 提供历史资金费率，覆盖面广，官方示例多。
- **[Deribit 公共 API](https://docs.deribit.com/?utm_source=chatgpt.com)**：包含 “perpetual funding rate” 历史接口，适合做多交易所对比或对冲。

### 期货价格与基差（CME 比特币期货）

- **[CME Group 实时数据 API](https://www.cmegroup.com/market-data/real-time-futures-and-options-data-api.html?utm_source=chatgpt.com)**：官方实时行情流，是基差计算里“期货腿”的直接来源，但属于付费/许可数据。
- **[CME DataMine](https://www.cmegroup.com/market-data/datamine-api.html?utm_source=chatgpt.com)**：可下载官方历史结算、成交量等，适合算日度基差，同样需要购买授权。
- 如果仍在验证原型，可用 Coinbase 现货 + 任意能获取的 CME 延时/结算价跑通流程，之后再升级数据源。

### 比特币现货 ETF 资金流（净申赎）

- **[Farside Investors](https://farside.co.uk/btc/?utm_source=chatgpt.com)**：免费聚合多只美股比特币现货 ETF 的每日净流入/流出表格与图表，社区常用参考源。
- **[iShares IBIT 官方页](https://www.blackrock.com/us/individual/products/333011/ishares-bitcoin-trust?utm_source=chatgpt.com)**：发行方披露信息，可作为校准基准。
- 若需要更系统化的历史与多指标比对，可引入 Glassnode、Kaiko 等付费链上/市场数据商。

### 经济与事件日历（宏观 / 财报）

- **[Trading Economics Calendar API](https://tradingeconomics.com/api/calendar.aspx?utm_source=chatgpt.com)**：提供全球宏观日历，REST 接口简单易用。
- **[Finnhub](https://finnhub.io/docs/api/earnings-calendar?utm_source=chatgpt.com)**：既有财报日历，也包含全球交易所假期接口，可拼成项目所需的“事件日历”模块。

### 交易日 / 市场开闭市判断

- **[Polygon Market Holidays](https://polygon.io/docs/rest/stocks/market-operations/market-holidays?utm_source=chatgpt.com)**：`/v1/marketstatus/upcoming` 返回未来假期与开闭市时间，用于保护定时任务。
- **IEX / Finnhub**：可作为假期、交易日的备援数据源。

### 落地拼装建议

1. **先用免费或低成本源跑通 ETL**：例如现货用 Coinbase，ETF 流用 Farside，宏观日历用 Trading Economics，假期信息用 Polygon。
2. **完成三大核心指标**：
   - **板块 / 主题表现**：拉取对应 ETF 的收盘或日内数据并聚合（如科技 = XLK，军工 = ITA/XAR，AI = BOTZ/ROBO）。
   - **资金费率**：使用 Binance 或 Deribit 的历史 funding rate 构建时间序列。
   - **期货基差**：基差 = 期货近月价格 − 现货价格；年化基差 = (期货 / 现货 − 1) × 365 / 到期天数。期货价格来自 CME 实时或结算数据，现货价格来自 Coinbase。
3. **需求稳定后升级为专业付费源**：逐步将 ETF/股票数据迁移至 Polygon/Tiingo，将期货数据升级为 CME 官方流或数据集，提前处理好授权与法务流程。

### 版权与合规提醒

- 大多数商用数据都限制再分发、长期缓存或展示粒度。上线前务必审阅条款，尤其是缓存或下游 API 的使用是否被允许。
- 官方文档通常标注了许可范围，不要忽视相关说明。

## 测试思路

- 为 `_score_ai` / `_score_btc` 添加单元测试，通过输入示例字典并断言总分的方式进行。
- 对渲染生成的 HTML 和飞书卡片内容进行快照测试。
- 在获得 API 密钥后，使用真实的市场 API 来扩展 ETL 流程。
