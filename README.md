# existence.app

## Career scraper quickstart

Run the Flask app:

```bash
python main.py
```

Run tests:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## cPanel deployment (main.py + app)

For cPanel Python App, use:

- `Application startup file`: `main.py`
- `Application Entry point`: `app`

Do not track `passenger_wsgi.py` in this repository. cPanel can manage that file server-side, and tracking it in Git can cause pull conflicts on the server.

## Scraper endpoint

`GET /scrape`

Required query params:

- `sleeve` = `A|B|C|D|E`

Useful optional query params:

- `location_mode` = `nl_only|nl_eu|global` (default: `nl_only`)
- `sources` = comma-separated source IDs (default: config defaults)
- `max_results` = max returned records (default: `20`, max: `100`)
- `max_pages` = pages per query/source for pagination (default: `8`)
- `target_raw` = raw target per run before early stop (default: `150`)
- `rps` = list-page request rate per domain (default: `1.5`)
- `detail_rps` = detail-page request rate per domain (default: `1.2`)
- `no_new_unique_pages` = stop after N pages without new unique jobs (default: `2`)
- `strict` = `1|0` target sleeve strictness (default: `0`)
- `refresh` = `1|0` bypass source cache (default: `0`)
- `failover` = `1|0` allow backend to add fallback sources on low yield/blocking (default: auto: `0` when explicit `sources` are passed, else `1`)
- `include_fail` = `1|0` include FAIL records in returned `jobs` (default: `0`)
- `incremental` = `1|0` return only unseen jobs from local state (default: `0`)
- `state_window_days` = retention window for incremental seen-state (default: `14`)
- `legacy` = `1|0` return legacy list-only payload (default: `0`)

## Debugging & observability

The scraper now reports:

- Per source+query+location: `raw_count`, `parsed_count`, `error_count`, `blocked_detected`, `pages_attempted`
- Funnel metrics: `raw -> after_dedupe -> scored -> pass/maybe/fail`
- Top fail reasons and dynamic fallback steps
- Dedupe ratio per source
- Full-description coverage (`full_description_count`, `full_description_coverage`)
- Auto-failover diagnostics when primary sources are blocked or low-yield
- Example response payload: `sample_scrape_output.json`

HTML snapshots for blocking/parse failures are written to:

- `debug_snapshots/` (override with env var `SCRAPE_SNAPSHOT_DIR`)

Console logging includes:

- structured JSON events with `run_id`
- run-level summary (`raw/deduped/pass/maybe/fail`)
- per source+query summary with blocked/error indicators

## Runtime config

`scrape_runtime_config.json` controls:

- `config_version`
- threshold overrides for PASS/MAYBE
- detail-fetch budgets
- crawl no-new stop behavior
- query-performance pruning settings

## Config knobs in code

Main knobs are in `main.py`:

- `DEFAULT_MAX_PAGES`
- `DEFAULT_TARGET_RAW_PER_SLEEVE`
- `DEFAULT_RATE_LIMIT_RPS`
- `DEFAULT_DETAIL_RATE_LIMIT_RPS`
- `DEFAULT_HTTP_RETRIES`
- `SNAPSHOT_DIR`
