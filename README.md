# Daily Messenger

Automated daily market digest tailored for GitHub Actions + GitHub Pages deployments. The application simulates the ETL → scoring → reporting → Feishu notification workflow described in the requirement list and is ready to be wired into real data sources.

## Repository layout

```
repo/
  etl/                 # Data fetch scripts
  scoring/             # Scoring logic
  digest/              # HTML/text/card rendering
  tools/               # Utility scripts (Feishu push)
  config/              # Configurable weights and thresholds
  data/                # Optional historical snapshots
  state/               # Idempotency markers
  out/                 # Build artefacts
  .github/workflows/   # CI definitions
  requirements.txt
```

## Quickstart

1. **Install dependencies**

   Using [uv](https://github.com/astral-sh/uv) (preferred):

   ```bash
   uv sync
   ```

   Or with virtualenv + pip:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the pipeline locally**

   ```bash
   uv run python etl/run_fetch.py
   uv run python scoring/run_scores.py --force
   uv run python digest/make_daily.py
   ```

   If you are using the pip workflow, replace `uv run` with `python`.

   The generated files will be placed under `out/`. If you wish to test Feishu delivery, prepare a test webhook and run:

   ```bash
   export FEISHU_WEBHOOK=https://open.feishu.cn/xxx
   python tools/post_feishu.py --webhook "$FEISHU_WEBHOOK" --summary out/digest_summary.txt --card out/digest_card.json
   ```

3. **Reset idempotency**

   The scoring step writes `state/done_YYYY-MM-DD`. Remove the file or run `python scoring/run_scores.py --force` to regenerate the same day’s report.

## Running tests

Install the development extra first and then use `uv run` (or plain `pytest` inside an activated virtual environment) to execute the lightweight unit tests that cover core scoring and digest helpers:

```bash
uv sync --extra dev
uv run pytest
```

These tests validate the score weighting logic, generated action labels, and the summary/card builders that power the Feishu message payload.

## GitHub Actions automation

The workflow defined in `.github/workflows/daily.yml` runs every weekday at 14:00 UTC (07:00 PT). It performs:

1. Checkout + Python environment bootstrap
2. Dependency installation with pip cache
3. ETL → scoring → digest scripts
4. Upload `out/` as a GitHub Pages artifact and deploy
5. Send the Feishu interactive card

Secrets required:

- `FEISHU_WEBHOOK`: Feishu custom bot webhook URL
- `FEISHU_SECRET` (optional): signature secret if enabled
- `API_KEYS`: JSON string containing upstream API credentials (placeholders supported)

## Failure & degraded handling

- If ETL fails or raw files are missing, scoring falls back to neutral scores and the digest is marked as degraded.
- The digest step can be forced into degraded mode via `python digest/make_daily.py --degraded`.
- GitHub Actions continues to send a downgraded broadcast even after upstream failures.

## Troubleshooting

| Symptom | Fix |
| ------- | --- |
| `FileNotFoundError` for raw data | Ensure `etl/run_fetch.py` ran successfully before scoring/digest. |
| Digest shows stale content | Delete `state/done_YYYY-MM-DD` and rerun scoring with `--force`. |
| Feishu webhook rejects request | Double-check signature secret, ensure the robot allows interactive cards. |

## Testing ideas

- Add unit tests for `_score_ai` / `_score_btc` by feeding sample dictionaries and asserting the totals.
- Snapshot-test the rendered HTML and Feishu card payload.
- Extend ETL with actual market APIs when API keys are available.

