# Repository Guidelines

## Project Structure & Module Organization
The pipeline is split across packages: `etl/` fetches market and macro data, `scoring/` derives weights, `digest/` assembles summaries, and `tools/` ships notifications like `post_feishu.py`. Config lives in `config/`, run-state markers in `state/`, and build outputs in `out/`. Docs live in `docs/`; CI helpers sit under `project_tools/`. Tests mirror the runtime layout inside `tests/`.

## Build, Test, and Development Commands
Install dependencies with `uv sync --locked --no-dev` to mirror CI; add `--locked --extra dev` locally when you need linting or pytest extras. Execute the pipeline via `uv run dm run` for the common path, or fall back to the individual scripts (`uv run python etl/run_fetch.py` / `uv run python scoring/run_scores.py --force` / `uv run python digest/make_daily.py`) when you need targeted steps. Artefacts appear in `out/`. Prefer the same entry points for partial runs, swapping `uv run` for `python` only when using a manual virtualenv. Validate changes with `uv run pytest`, using `-k module_name` for targeted runs.

## Coding Style & Naming Conventions
All code targets Python 3.11, four-space indentation, and PEP 8 naming (snake_case modules and functions, PascalCase classes). Keep new logic inside existing package boundaries; shared helpers belong beside their consumers rather than duplicated. Ruff ships as an optional linter—run `uv run ruff check .` and apply autofixes with `--fix`. Prefer type hints and short docstrings on entry scripts to clarify CLI usage.

## Testing Guidelines
Pytest exercises every stage; add new coverage beside files like `tests/test_scoring.py`. Name tests `test_<behavior>` and store fixtures under `tests/__fixtures__/` when mocks beat live calls. Focus assertions on score calculations, digest formatting, and degraded-mode fallbacks. Run `uv run pytest --cov=digest --cov=scoring` for meaningful feature work and note any gaps in the PR.

## Commit & Pull Request Guidelines
Commit subjects stay under 72 characters and use the imperative mood (e.g., `Fix run_fetch direct execution imports`). Keep each commit scoped to one concern, expanding in the body only when data contracts or configs change. Pull requests need a concise summary, linked issue or task ID, and sample artefacts from `out/` when headlines or cards change. Confirm the pipeline, tests, and linting ran locally, and flag secret or config updates up front.

## Configuration & Secrets
Runtime credentials load from the `API_KEYS` environment variable; keep `api_keys.json.example` in sync with schema changes. Local overrides belong in untracked files or `.env`. Never commit live keys—exercise new fetchers with mock data and verify `out/etl_status.json` records graceful degradation. Document new flags in `config/` and ensure CI picks up defaults when behavior shifts.
