# 每日简报 Daily Messenger

一个自动化的每日市场简报，专为GitHub Actions和GitHub Pages部署而设计。该应用了ETL(extract/transform/load) → 评分 → 报告 → 飞书通知的工作流。

目前该项目定期抓取（默认抓取每日资讯，每天抓取一次）的信息流如下：

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

* Alpha Vantage：用于抓取 SPX/NDX 指数代理与 AI/防御板块 ETF 的最新收盘价与涨跌幅。

* Twelve Data：覆盖恒生指数等港股行情，补足国际视角。

* Financial Modeling Prep：拉取 NVDA/MSFT 等主题篮子的估值比率与涨跌表现，驱动 AI 与 Magnificent 7 主题打分。

* Coinbase / OKX：分别提供 BTC 现货、永续合约资金费率与基差。

* Farside Investors：解析现货 ETF 表格获取最近一天的净申赎额。

* Trading Economics + Finnhub：拉取宏观事件与未来一周的财报日历，二者合并为统一事件流。

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

* `FEISHU_WEBHOOK`: 飞书自定义机器人的 Webhook URL

* `FEISHU_SECRET` （可选）: 签名密钥 （如果启用）

* `API_KEYS`: 包含上游 API 凭证的 JSON 字符串 (支持占位符)，建议包含
`alpha_vantage`、`twelve_data`、`financial_modeling_prep`、`trading_economics`、`finnhub` 等键名

## 失败与降级处理

* 如果ETL失败或原始文件丢失，评分步骤将回退到中性分数，并且生成的简报会被标记为降级状态。

* 可以通过运行 `python digest/make_daily.py --degraded` 强制让简报生成步骤进入降级模式。

* 即使上游任务失败，GitHub Actions 仍会继续发送降级后的通知。

## 测试运行

```bash
# 安装依赖

# 可选：提供上游 API key（不配也能跑，脚本会回退到模拟数据）
export API_KEYS='{"alpha_vantage":"...", "trading_economics":"user:pass", "twelve_data":"...", "financial_modeling_prep":"...", "finnhub":"..."}'

# 1) 抓数（写出 raw_*.json 和 etl_status.json）
python etl/run_fetch.py

# 2) 计算打分（写出 scores.json / actions.json）
python scoring/run_scores.py --force

# 3) 渲染页面（写出 index.html、摘要与卡片 JSON）
python digest/make_daily.py
```
