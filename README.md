# SEO Dashboard

SEO Dashboard is a local Python project for SEO analysis and weekly operational review.

The project is no longer just a smoke-test OAuth prototype. In its current state it supports:

- local OAuth 2.0 authentication for Google Search Console and GA4
- multi-window GSC and GA4 bundle exports
- unified page datasets across `last_28_days`, `previous_28_days`, `last_90_days`, and `last_365_days`
- quick-win scoring and queue generation
- Search Console URL Inspection enrichment for prioritized pages
- sitemap ingestion and sitemap-based opportunity review
- rule-based brand / non-brand, intent, and page-segment interpretation
- weekly historical snapshots and weekly delta generation
- a static official dashboard at `output/seo-dashboard.html`
- one canonical frontend contract at `output/data.json`
- a local workflow layer with statuses, notes, saved views, and filtered exports

## What The Project Does Today

Current implemented scope:

- local settings loading and validation
- structured app and error logging
- browser-based OAuth installed-app flow
- GSC smoke tests
- GA4 smoke tests
- GSC sitewide trend, queries, pages, countries, devices, and page+query exports
- GA4 landing-page exports across configured windows
- merged page-level datasets from GSC and GA4
- normalized `recommended_action` scoring output
- queue generation for top page opportunities
- URL Inspection enrichment with cache reuse and scope-based review
- sitemap index and sitemap URL ingestion
- sitemap opportunity review with transparent heuristic scoring
- canonical dashboard payload generation with validation metadata
- weekly snapshot persistence and weekly delta comparison
- local rule-based interpretation layer
- local workflow persistence in JSON files
- static dashboard UX with filters, saved views, exports, and workflow editing

Still intentionally not included:

- website content editing from the dashboard
- CMS behavior or database-backed task management
- `launchd` / cron scheduling built into the project
- paid SEO APIs or third-party keyword tools
- a production crawler with content-body extraction and internal-link graph analysis

Note on crawl support:

- `src/services/crawl_service.py` currently exists only as a stub.
- The project does **not** yet provide a full crawl pipeline or standalone crawl command.

## Requirements

- macOS-oriented local workflow
- Python 3.11+
- a Google Cloud project with:
  - Search Console API enabled
  - Google Analytics Data API enabled
- a desktop OAuth client JSON saved locally

Recommended browser for the dashboard:

- Chromium-based browser on `http://localhost`, especially if you want workflow write-back into `data/state/`

## Project Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the example settings file:

```bash
cp settings.example.json settings.json
```

## Google Cloud Setup

Before running OAuth:

1. Create or open a Google Cloud project.
2. Enable:
   - Search Console API
   - Google Analytics Data API
3. Configure the OAuth consent screen.
4. Create an OAuth Client ID of type `Desktop app`.
5. Download the credentials JSON to:

```bash
config/credentials.json
```

If the OAuth consent screen is still in testing mode, add the Google account you will use as a test user.

## Configuration

The project reads `settings.json`.

Current example fields in [settings.example.json](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/settings.example.json):

- `project_name`
- `site_url`
- `sitemap_url`
- `sitemap_urls`
- `ga4_property_id`
- `default_language`
- `default_period_days`
- `comparison_period_days`
- `secondary_period_days`
- `long_term_period_days`
- `inspection_daily_limit`
- `crawl_frequency_days`
- `inspection_scope_prefix`
- `output_html`
- `output_data_json`
- `log_app_file`
- `log_error_file`
- `google_oauth_credentials_file`
- `google_oauth_token_file`

Important validation rules:

- `site_url` must start with `http://` or `https://`
- `site_url` must end with `/`
- `sitemap_url` or `sitemap_urls` must contain at least one sitemap URL
- `ga4_property_id` must be numeric
- `default_language` must be `ru` or `en`
- period / quota fields must be positive integers
- `inspection_scope_prefix` must start with `/`

Notes:

- `sitemap_url` still works as the primary single-sitemap field.
- `sitemap_urls` can contain one or more sitemap or sitemap-index URLs.
- when both are provided, the project deduplicates them and uses the first configured sitemap as the primary one

## CLI Commands

All commands use the same entrypoint:

```bash
python3 -m src.main --config settings.json --command <command>
```

### Bootstrap And Validation

Initialize folders and log files:

```bash
python3 -m src.main --config settings.json --command init
```

Validate the config:

```bash
python3 -m src.main --config settings.json --command validate-config
```

Show resolved project paths:

```bash
python3 -m src.main --config settings.json --command show-paths
```

### OAuth And Smoke Tests

Authenticate via local browser flow:

```bash
python3 -m src.main --config settings.json --command auth
```

Test Search Console access:

```bash
python3 -m src.main --config settings.json --command test-gsc
```

Test GA4 access:

```bash
python3 -m src.main --config settings.json --command test-ga4
```

Run both smoke tests:

```bash
python3 -m src.main --config settings.json --command test-all
```

### GSC Fetch Commands

Fetch the full GSC bundle:

```bash
python3 -m src.main --config settings.json --command fetch-gsc
```

This bundle includes:

- sitewide trends
- query reports
- page reports
- country reports
- device reports
- page+query rows for `last_28_days`

Fetch only trends:

```bash
python3 -m src.main --config settings.json --command fetch-gsc-trends
```

Fetch only queries:

```bash
python3 -m src.main --config settings.json --command fetch-gsc-queries
```

Fetch only pages:

```bash
python3 -m src.main --config settings.json --command fetch-gsc-pages
```

Fetch only page+query rows:

```bash
python3 -m src.main --config settings.json --command fetch-gsc-page-query
```

### GA4 Fetch Commands

Fetch the full GA4 landing-page bundle:

```bash
python3 -m src.main --config settings.json --command fetch-ga4
```

Fetch only landing-page data:

```bash
python3 -m src.main --config settings.json --command fetch-ga4-landing
```

### Merge, Scoring, Inspection

Build unified page datasets:

```bash
python3 -m src.main --config settings.json --command merge-pages
```

Score pages and build the queue:

```bash
python3 -m src.main --config settings.json --command score-pages
```

Inspect top pages with URL Inspection:

```bash
python3 -m src.main --config settings.json --command inspect-top-pages
```

Current inspection command behavior:

- selects up to `500` queue pages per run
- uses configured `inspection_daily_limit`
- reuses cached inspection results when possible
- preserves partial results if quota or API errors interrupt a run
- focuses the formal indexing-review layer on `inspection_scope_prefix`

### Sitemap Opportunity Review

Fetch sitemap inventory and generate the sitemap opportunity review:

```bash
python3 -m src.main --config settings.json --command enrich-with-sitemap
```

This command:

- downloads and parses configured sitemap URLs locally
- supports sitemap indexes
- builds `data/raw/sitemap_inventory.json`
- generates `data/processed/sitemap_opportunity_review.csv`
- generates `data/processed/sitemap_opportunity_review.json`

### Dashboard Generation

Generate the canonical dashboard contract and update local history:

```bash
python3 -m src.main --config settings.json --command generate-dashboard
```

This command now also:

- ensures local workflow state files exist
- builds / refreshes the canonical payload at `output/data.json`
- captures a history snapshot under `data/history/snapshots/`
- updates `data/history/latest/latest_snapshot.json`
- updates `data/history/latest/latest_weekly_delta.json`
- prints validation status, missing files, missing sections, and warnings

## Recommended Workflows

### Full Refresh

```bash
python3 -m src.main --config settings.json --command fetch-gsc
python3 -m src.main --config settings.json --command fetch-ga4
python3 -m src.main --config settings.json --command merge-pages
python3 -m src.main --config settings.json --command score-pages
python3 -m src.main --config settings.json --command inspect-top-pages
python3 -m src.main --config settings.json --command enrich-with-sitemap
python3 -m src.main --config settings.json --command generate-dashboard
```

### Faster Dashboard Refresh After Data Exists

```bash
python3 -m src.main --config settings.json --command merge-pages
python3 -m src.main --config settings.json --command score-pages
python3 -m src.main --config settings.json --command inspect-top-pages
python3 -m src.main --config settings.json --command generate-dashboard
```

## Official Dashboard

The only official dashboard entrypoint is:

```bash
output/seo-dashboard.html
```

The only canonical frontend contract is:

```bash
output/data.json
```

Legacy files remain only for reference and are not used by default:

- `output/dashboard.html`
- `output/seo-dashboard-old.html`

### Running The Dashboard Locally

Serve the project directory locally:

```bash
cd /Users/antonnechayevsky/Documents/SEO\ Dashboard/seo-dashboard
python3 -m http.server 8000
```

Then open:

```bash
http://localhost:8000/output/seo-dashboard.html
```

Using `http://localhost` is recommended because:

- the dashboard fetches `output/data.json`
- workflow write-back to `data/state/` relies on File System Access API
- Chromium browsers support the best local workflow experience

## Canonical Dashboard Contract

Current metadata in the generated contract includes:

- `contract_version: 1.4`
- `default_language`
- `default_window`
- `official_dashboard_path`
- `official_data_path`
- `pages_section_mode`
- `page_windows_with_inspection`
- `interpretation_rules_path`
- `interpretation_layers`
- `workflow_statuses`
- `workflow_state_paths`
- `history_snapshot_dir`
- `history_latest_dir`
- `sitemap_review_paths`

Current top-level payload areas:

- `metadata`
- `windows`
- `kpis`
- `sections`
- `validation`

Current `sections` in the canonical contract:

- `sitewide_trends`
- `queries`
- `pages`
- `top_page_movers`
- `indexing_review`
- `sitemap_opportunity_review`
- `quick_wins`
- `countries`
- `devices`
- `workflow`
- `weekly_delta`

Current validation behavior:

- `is_ready` is `true` only when required sections are present
- missing backend prerequisites are surfaced through:
  - `missing_files`
  - `missing_sections`
  - `warnings`

## Dashboard Features

Current dashboard capabilities in [output/seo-dashboard.html](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/output/seo-dashboard.html):

- RU/EN localization with Russian as the default UI
- dark mode toggle
- top-bar date selector for:
  - `28 days`
  - `90 days`
  - `365 days`
- overview KPI cards with interpretation and neutral fallback when comparison is unavailable
- sitewide trend visualization
- weekly workflow guide block
- floating side navigation
- quick filters + advanced filters
- active-filter chip list
- saved views:
  - non-brand opportunities
  - blog only
  - commercial pages only
  - indexing issues
  - quick wins
  - planned
  - done
- first-class visible sections for:
  - quick wins
  - weekly delta
  - indexing review
  - sitemap opportunity review
  - queries
  - top page movers
  - pages
- local query search inside the Queries section
- sortable tables
- clickable page-path links
- wide-table horizontal scrolling inside section cards
- local workflow editing for supported rows
- filtered export to CSV or JSON
- localized validation and empty states

## Workflow Layer

Workflow state is local-only and file-based.

Persistent files:

- `data/state/workflow_statuses.json`
- `data/state/notes.json`

Supported workflow statuses:

- `new`
- `planned`
- `in_progress`
- `done`
- `ignored`
- `recheck_later`

Stable identity strategy:

- page records use `page::<normalized_page_path>`
- fallback page identity uses normalized page URL
- issue records use `issue::<normalized_page_path>::<issue_type_signature>`

Important behavior:

- workflow state is **not** stored only inside generated dashboard files
- running `fetch-gsc`, `fetch-ga4`, `merge-pages`, `score-pages`, `inspect-top-pages`, or `generate-dashboard` does **not** overwrite `data/state/*.json`
- if the browser cannot write to disk, the dashboard keeps workflow edits in memory for the current session and surfaces that limitation explicitly

Workflow write-back requirements:

- open the dashboard from `http://localhost`
- use a Chromium browser
- click `Connect data/state`
- choose the project’s `data/state/` folder

## Weekly History And Delta

Weekly state is stored locally under:

- `data/history/snapshots/`
- `data/history/latest/latest_snapshot.json`
- `data/history/latest/latest_weekly_delta.json`

What is snapshoted:

- KPI summary
- pages datasets
- quick wins
- indexing review
- top page movers
- query summary when available

Current weekly delta tracks:

- new issues this week
- resolved issues this week
- pages improved this week
- pages declined this week
- new quick wins this week
- pages removed from quick wins this week
- reason-code changes

Delta logic is transparent and heuristic:

- page movement uses weighted changes across:
  - quick-win score
  - clicks
  - impressions
  - sessions
  - conversions
  - SERP position gain
- delta is not fabricated if there is no prior weekly baseline

If only one snapshot exists, the dashboard shows that weekly delta is not available yet.

## Interpretation Layer

The project uses local, editable rules only.
No paid keyword tools or external classification APIs are required.

Rules live in:

```bash
config/seo_rules.json
```

Current interpretation layers:

- brand split
- query intent
- page segment

### Brand Classification

Current output labels:

- `brand`
- `non_brand`
- `mixed`

The brand split is based on editable `brand_terms`.

### Intent Classification

Current supported labels:

- `informational`
- `commercial`
- `navigational`
- `lead_intent`
- `unknown`

These are heuristic keyword-bucket rules, not ML predictions.

### Page Segments

Current supported page segments:

- `blog`
- `commercial`
- `other`

The contract also carries:

- `page_segment_confidence`
- `page_segment_source`
- `page_directory_group`

## Sitemap Opportunity Review

This module closes the previous sitemap / inventory gap in the project.

Inputs:

- configured sitemap URLs
- merged page datasets
- GSC bundle
- GA4 bundle
- inspection outputs when available

Primary raw output:

- `data/raw/sitemap_inventory.json`

Primary processed outputs:

- `data/processed/sitemap_opportunity_review.csv`
- `data/processed/sitemap_opportunity_review.json`

Current review signals include:

- `source_in_sitemap`
- `source_in_gsc`
- `source_in_ga4`
- `source_in_inspection`
- `indexed_status`
- `crawlable_not_indexed`
- `canonical_mismatch`
- `robots_issue`
- visibility and session status
- `thin_content_risk`
- `thin_content_signals`
- duplicate slug cluster size
- slug/topic potential
- merge candidate heuristics
- `opportunity_score`
- `recommended_action`

Current recommended actions:

- `expand_content`
- `merge_into_stronger_page`
- `redirect_301`
- `keep_and_monitor`
- `manual_review`

Important limitation:

- the sitemap review is intentionally heuristic
- merge / redirect suggestions are review candidates, not guaranteed truth
- lightweight title/fetch enrichment is limited and not a full crawler

## Scoring And Review Logic

### Quick-Win Scoring

Current scoring rules in the codebase:

- `impact_score = (gsc_impressions * 0.3) + (gsc_clicks * 0.2) + (ga4_sessions * 0.3) + (ga4_conversions * 0.2) + (1000 / gsc_position)`
- `effort_score = 1 + 2 if gsc_ctr < 0.02 + 3 if gsc_position > 20 + 1 if bounce-rate proxy > 70%`
- bounce proxy uses `1 - (ga4_engaged_sessions / ga4_sessions)` when sessions are available
- `quick_win_score = impact_score / effort_score`

Normalized `recommended_action` values:

- `rewrite`
- `expand`
- `merge`
- `request_indexing`
- `no_action`

The human-readable explanation remains in `recommended_action_text`.

### Top Page Movers

Current mover logic:

- compares `last_28_days` vs `previous_28_days`
- uses a weighted delta across:
  - clicks
  - impressions
  - sessions
  - conversions

Outputs:

- `data/processed/top_page_movers_last_28_vs_previous_28.csv`
- `data/processed/top_page_movers_last_28_vs_previous_28.json`

### Indexing Review

Current indexing review:

- is built as a formal dataset
- is scoped to `inspection_scope_prefix`
- uses inspection-derived issue flags such as:
  - verdict issues
  - canonical mismatch
  - robots issues
  - not indexed / excluded conditions

Outputs:

- `data/processed/indexing_review_last_28_days.csv`
- `data/processed/indexing_review_last_28_days.json`

## Generated Files And Directories

Important runtime directories:

- `config/`
- `data/raw/`
- `data/processed/`
- `data/history/`
- `data/state/`
- `logs/`
- `output/`

Typical generated files:

- `data/raw/gsc_bundle.json`
- `data/raw/ga4_bundle.json`
- `data/raw/gsc_inspection_top_500.json`
- `data/raw/sitemap_inventory.json`
- `data/processed/unified_pages_last_28_days.csv`
- `data/processed/unified_pages_previous_28_days.csv`
- `data/processed/unified_pages_last_90_days.csv`
- `data/processed/unified_pages_last_365_days.csv`
- `data/processed/page_queue_top_100.csv`
- `data/processed/page_queue_top_500.csv`
- `data/processed/indexing_review_last_28_days.csv`
- `data/processed/top_page_movers_last_28_vs_previous_28.csv`
- `data/processed/sitemap_opportunity_review.csv`
- `output/data.json`

## Structure Overview

- [src/main.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/main.py) — CLI entrypoint
- [src/config.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/config.py) — config loading and validation
- [src/paths.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/paths.py) — project path discovery and directory helpers
- [src/logger.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/logger.py) — app and error logging
- `src/clients/gsc_client.py` — Search Console API access and report fetch helpers
- `src/clients/ga4_client.py` — GA4 Data API access and landing-page fetch helpers
- `src/clients/inspection_client.py` — URL Inspection API client helpers
- [src/services/oauth_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/oauth_service.py) — OAuth token loading, refresh, browser login, and persistence
- [src/services/gsc_fetch_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/gsc_fetch_service.py) — GSC bundle orchestration
- [src/services/ga4_fetch_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/ga4_fetch_service.py) — GA4 bundle orchestration
- [src/services/merge_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/merge_service.py) — unified page merge layer
- [src/services/scoring_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/scoring_service.py) — quick-win scoring and queue generation
- [src/services/inspection_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/inspection_service.py) — inspection enrichment orchestration
- [src/services/sitemap_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/sitemap_service.py) — sitemap inventory and opportunity review
- [src/services/history_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/history_service.py) — weekly snapshots and weekly delta
- [src/services/interpretation_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/interpretation_service.py) — brand, intent, and segment interpretation
- [src/services/workflow_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/workflow_service.py) — workflow state overlay and keys
- [src/services/dashboard_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/dashboard_service.py) — canonical dashboard contract generation
- [src/services/crawl_service.py](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/src/services/crawl_service.py) — crawl stub only, not a full crawler
- [config/seo_rules.json](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/config/seo_rules.json) — editable local interpretation rules
- [output/seo-dashboard.html](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/output/seo-dashboard.html) — official dashboard
- [output/data.json](/Users/antonnechayevsky/Documents/SEO%20Dashboard/seo-dashboard/output/data.json) — canonical frontend contract

## Notes And Limitations

- Search Console page+query exports can be large.
- Google-side API limits can still cap returned rows even with pagination.
- GA4 uses `landingPagePlusQueryString`; normalized URL/path fields are added for merge stability.
- Weekly delta is only meaningful after at least two snapshots from different ISO weeks.
- Workflow write-back is best-effort browser behavior, not a backend save API.
- The project is local-first and file-based by design.
