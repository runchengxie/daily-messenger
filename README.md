# Daily Messenger (每日简报)

一个自动化的每日市场简报，专为 GitHub Actions 和 GitHub Pages 部署而设计。该应用模拟了需求列表中描述的 ETL → 评分 → 报告 → 飞书通知的工作流，并已准备好接入真实的数据源。

## 仓库结构

```
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
  requirements.txt
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
- `API_KEYS`: 包含上游 API 凭证的 JSON 字符串 (支持占位符)

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

## 测试思路

- 为 `_score_ai` / `_score_btc` 添加单元测试，通过输入示例字典并断言总分的方式进行。
- 对渲染生成的 HTML 和飞书卡片内容进行快照测试。
- 在获得 API 密钥后，使用真实的市场 API 来扩展 ETL 流程。
