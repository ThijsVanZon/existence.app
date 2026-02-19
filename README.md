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

## Authentication & account modes

The app supports:

- email/password accounts
- email verification
- password reset via email
- TOTP 2FA (Google Authenticator / Microsoft Authenticator)

Environment variables:

- `AUTH_ENFORCE=1` to require login for Synergy/Enlightenment/scrape endpoints
- `AUTH_ADMIN_EMAIL=thijs.vanzon@existenceinitiative.com` for admin account behavior
- `FLASK_SECRET_KEY` (required in production)
- `PUBLIC_BASE_URL` for absolute email links (example: `https://existence.app`)
- SMTP settings for real email delivery:
  - `AUTH_EMAIL_FROM`
  - `AUTH_SMTP_HOST`
  - `AUTH_SMTP_PORT`
  - `AUTH_SMTP_USERNAME`
  - `AUTH_SMTP_PASSWORD`
  - `AUTH_SMTP_USE_TLS=1`

Account behavior:

- Admin account (`AUTH_ADMIN_EMAIL`): fixed Career Sleeves `A-D` + custom sleeves.
- Other accounts: only custom sleeves (`A-Z`) and uniform custom ranking profile.

## Scraper endpoint

`GET /scrape`

Required query params:

- `career_sleeve` = one letter `A-Z` (admin uses fixed/custom semantics; non-admin uses custom semantics)

Useful optional query params:

- `max_results` = max returned records (default: `200`, max: `500`)
- `max_pages` = pages per query/source for pagination (default: `4`, max: `12`)
- `target_raw` = raw target per run before early stop (default: `150`)
- `rps` = list-page request rate per domain (default: `0.45`)
- `detail_rps` = detail-page request rate per domain (default: `0.25`)
- `no_new_unique_pages` = stop after N pages without new unique jobs (default: `2`)
- `strict` = `1|0` target Career Sleeve strictness (default: `0`)
- `refresh` = `1|0` bypass source cache (default: `0`)
- `failover` = `1|0` allow backend to add fallback sources on low yield/blocking (default: currently disabled in MVP flow)
- `include_fail` = `1|0` include FAIL records in returned `jobs` (default: `0`)
- `incremental` = `1|0` return only unseen jobs from local state (default: `0`)
- `state_window_days` = retention window for incremental seen-state (default: `14`)
- `search_queries` = comma-separated custom search queries; backend expands EN/NL variants for querying and matching
- abroad extraction/scoring uses EN/NL variants for travel context + geo; returned job openings include `abroad_identifiers` and `abroad_summary`

Auth-aware scrape behavior:

- Non-admin accounts always score with generic profile `E` (summary field: `scoring_profile_career_sleeve`).
- Admin keeps fixed profile behavior (`A-D`) plus custom mode for custom sleeves.

Current MVP backend behavior:

- `location_mode` is enforced to `nl_vn` (Netherlands + Vietnam abroad/local mix)
- sources are enforced to `indeed_web` + `linkedin_web` + `nl_web_openings`

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

