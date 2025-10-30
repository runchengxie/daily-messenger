# Repository Guidelines

## Project Structure & Module Organization
Runtime code now lives under `src/daily_messenger/`: `etl/` fetches market and macro data, `scoring/` derives weights, `digest/` assembles summaries, `crypto/` holds Bitcoin market tooling, `tools/` handles notifications like `post_feishu.py`, and shared bits stay in `common/`. Config lives in `config/`, run-state markers in `state/`, and build outputs in `out/`. Docs live in `docs/`; CI helpers sit under `project_tools/`. Tests mirror the runtime layout inside `tests/`.

## Build, Test, and Development Commands
Install dependencies with `uv sync --locked --no-dev` to mirror CI; add `--locked --extra dev` locally when you need linting or pytest extras. Execute the pipeline via `uv run dm run` for the common path, or fall back to the packaged modules (`uv run python -m daily_messenger.etl.run_fetch` / `uv run python -m daily_messenger.scoring.run_scores --force` / `uv run python -m daily_messenger.digest.make_daily`) when you need targeted steps. Artefacts appear in `out/`. Prefer the same entry points for partial runs, swapping `uv run` for `python` only when using a manual virtualenv. BTC helpers live behind `uv run dm btc <subcommand>` (`init-history`, `fetch`, `report`) and read configs like `config/ta_btc.yml`. Validate changes with `uv run pytest`, using `-k module_name` for targeted runs.

## Coding Style & Naming Conventions
All code targets Python 3.11, four-space indentation, and PEP 8 naming (snake_case modules and functions, PascalCase classes). Keep new logic inside existing package boundaries; shared helpers belong beside their consumers rather than duplicated. Ruff ships as an optional linter—run `uv run ruff check .` and apply autofixes with `--fix`. Prefer type hints and short docstrings on entry scripts to clarify CLI usage.

## Testing Guidelines
Pytest exercises every stage; add new coverage beside files like `tests/test_scoring.py` and `tests/test_crypto_btc.py` when touching the BTC flows. Name tests `test_<behavior>` and store fixtures under `tests/__fixtures__/` when mocks beat live calls. Focus assertions on score calculations, digest formatting, degraded-mode fallbacks, and crypto data wiring. Run `uv run pytest --cov=daily_messenger.digest --cov=daily_messenger.scoring` for meaningful feature work and note any gaps in the PR.

## Commit & Pull Request Guidelines
Commit subjects stay under 72 characters and use the imperative mood (e.g., `Fix run_fetch direct execution imports`). Keep each commit scoped to one concern, expanding in the body only when data contracts or configs change. Pull requests need a concise summary, linked issue or task ID, and sample artefacts from `out/` when headlines or cards change. Confirm the pipeline, tests, and linting ran locally, and flag secret or config updates up front.

## Configuration & Secrets
Runtime credentials load from the `API_KEYS` environment variable; keep `api_keys.json.example` in sync with schema changes. Local overrides belong in untracked files or `.env`. Never commit live keys—exercise new fetchers with mock data and verify `out/etl_status.json` records graceful degradation. Document new flags in `config/` and ensure CI picks up defaults when behavior shifts.

## Submission Guardrails
- Changes touching `config/weights.yml`, digest/templates, or any contract field **must** update the README examples and accompanying tests (`pytest -k contract`) in the same PR. Missing updates are treated as release blockers.
- Run `uv run pytest --cov=daily_messenger --cov-report=term-missing --cov-fail-under=70` and `uv run ruff check .` locally before sending a PR. CI enforces these commands and will reject runs that skip them.
- Attach representative artefacts from `out/` when digest layouts, cards, or weights shift so reviewers can validate the rendered output.

## Branch Rules & CODEOWNERS
- `main` is protected; all merges require a green CI run plus review approval.
- Paths under `src/daily_messenger/digest/**` and `config/**` require at least **two** reviewers from the code owner group before merging.
- Keep PR descriptions concise but explicit about data-contract or config impacts to unblock CODEOWNER review quickly.
