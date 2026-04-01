# SEO Dashboard

SEO Dashboard is a local macOS-oriented Python project for future SEO reporting workflows. At this stage it includes local OAuth 2.0 authentication, token persistence, and smoke tests for Google Search Console and Google Analytics Data API access.

Current scope:
- local project bootstrap
- JSON settings loading and validation
- structured logging
- browser-based OAuth 2.0 installed-app flow
- Search Console API access smoke test
- GA4 Data API access smoke test
- GSC performance data fetch and export to JSON/CSV
- GA4 landing page data fetch and export to JSON/CSV
- unified page dataset merge between GSC pages and GA4 landing pages
- quick-win scoring and page queue export
- URL Inspection enrichment for top quick-win pages
- sitemap ingestion and sitemap opportunity review
- weekly historical snapshots and weekly delta tracking
- read-only local HTML dashboard for artifact visualization
- canonical dashboard data contract at `output/data.json`
- local workflow layer for statuses, notes, and filtered task export

Not included yet:
- website content editing from the dashboard
- `launchd` scheduling
- deep semantic gap analysis

## Requirements

- macOS
- Python 3.11+

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

Before running OAuth, prepare Google Cloud credentials:

1. Create or open a Google Cloud project.
2. Enable these APIs:
   - Search Console API
   - Google Analytics Data API
3. Configure the OAuth consent screen.
4. Create an OAuth Client ID of type `Desktop app`.
5. Download the JSON credentials file and place it here:

```bash
config/credentials.json
```

If your app is in testing mode, make sure the Google account you will use is added as a test user in the OAuth consent screen configuration.

## CLI Commands

Initialize folders and log files:

```bash
python -m src.main --config settings.json --command init
```

Validate configuration:

```bash
python -m src.main --config settings.json --command validate-config
```

Show absolute project paths:

```bash
python -m src.main --config settings.json --command show-paths
```

Run local browser-based OAuth authentication:

```bash
python -m src.main --config settings.json --command auth
```

Test Google Search Console access:

```bash
python -m src.main --config settings.json --command test-gsc
```

Test Google Analytics Data API access:

```bash
python -m src.main --config settings.json --command test-ga4
```

Run both API smoke tests:

```bash
python -m src.main --config settings.json --command test-all
```

Fetch the full GSC bundle:

```bash
python -m src.main --config settings.json --command fetch-gsc
```

This full GSC bundle includes:
- sitewide trends
- queries
- pages
- countries
- devices
- page+query rows for `last_28_days`

Fetch only GSC trends across all configured windows:

```bash
python -m src.main --config settings.json --command fetch-gsc-trends
```

Fetch only the last 28 days query report:

```bash
python -m src.main --config settings.json --command fetch-gsc-queries
```

Fetch only the last 28 days page report:

```bash
python -m src.main --config settings.json --command fetch-gsc-pages
```

Fetch only the last 28 days page+query report:

```bash
python -m src.main --config settings.json --command fetch-gsc-page-query
```

Fetch the full GA4 landing-page bundle:

```bash
python -m src.main --config settings.json --command fetch-ga4
```

Fetch only the last 28 days GA4 landing-page report:

```bash
python -m src.main --config settings.json --command fetch-ga4-landing
```

Merge GSC pages with GA4 landing pages into a unified dataset:

```bash
python -m src.main --config settings.json --command merge-pages
```

Build the quick-win page queue from the unified dataset:

```bash
python -m src.main --config settings.json --command score-pages
```

Inspect top queue pages with Search Console URL Inspection API and enrich the unified dataset:

```bash
python -m src.main --config settings.json --command inspect-top-pages
```

Fetch sitemap inventory and build the sitemap opportunity review layer:

```bash
python -m src.main --config settings.json --command enrich-with-sitemap
```

Generate the canonical dashboard contract used by the official local dashboard:

```bash
python -m src.main --config settings.json --command generate-dashboard
```

## Local Dashboard

The official dashboard entrypoint is:

```bash
output/seo-dashboard.html
```

The official dashboard reads one canonical frontend contract only:

- `output/data.json`

That canonical contract is generated from backend artifacts and currently includes these sections:
- `sitewide_trends`
- `queries`
- `pages`
- `top_page_movers`
- `indexing_review`
- `sitemap_opportunity_review`
- `weekly_delta`

The official dashboard now also includes:
- a real top-bar date range selector for `28 days`, `90 days`, and `365 days`
- RU/EN UI localization with Russian as the default language
- clickable page links in the major page-based tables
- first-class visible sections for:
  - `queries`
  - `pages` (shown as the unified pages explorer)
  - `top_page_movers`
  - `indexing_review`
  - `sitemap_opportunity_review`
  - `weekly_delta`
- localized validation and empty states that distinguish:
  - no data generated yet
  - unsupported date range for a section
  - partial data available
  - no issues found
- a lightweight local workflow layer with:
  - statuses: `new`, `planned`, `in_progress`, `done`, `ignored`, `recheck_later`
  - page and issue notes
  - filtered dataset export
  - saved working views including planned and done

The backend artifacts that feed the contract include:
- `data/raw/gsc_bundle.json`
- `data/raw/ga4_bundle.json`
- `data/raw/sitemap_inventory.json`
- `data/processed/unified_pages_last_28_days.csv`
- `data/processed/page_queue_top_100.csv`
- `data/raw/gsc_inspection_top_500.json`
- `data/processed/sitemap_opportunity_review.csv`
- `data/processed/sitemap_opportunity_review.json`

Sitemap configuration:
- `sitemap_url` still works as a single primary sitemap value
- `sitemap_urls` can be used to configure one or more sitemap or sitemap-index URLs
- the project uses local/raw sitemap inventory as the source of truth for the inventory review layer; `generate-dashboard` will not silently refetch sitemap XML

Sitemap review outputs:
- `data/raw/sitemap_inventory.json`
  - raw canonical sitemap inventory collected from the configured sitemap URLs
- `data/processed/sitemap_opportunity_review.csv`
  - `last_28_days` review export for spreadsheet-style triage
- `data/processed/sitemap_opportunity_review.json`
  - review rows by window (`last_28_days`, `last_90_days`, `last_365_days`)

Sitemap opportunity review logic is heuristic and local-only:
- source flags are derived from sitemap inventory, merged page datasets, and inspection results
- thin-content signals are transparent rule-based proxies, not ML judgments
- merge / redirect suggestions are heuristic and should be treated as review candidates, not automatic truth

Historical weekly state is persisted locally under:
- `data/history/snapshots/` - immutable timestamped weekly snapshot files
- `data/history/latest/latest_snapshot.json` - convenience copy of the newest snapshot
- `data/history/latest/latest_weekly_delta.json` - most recent computed weekly delta

Workflow state is persisted locally under:
- `data/state/workflow_statuses.json`
- `data/state/notes.json`

Workflow file shape:
- `workflow_statuses.json`
  - `version`
  - `updated_at`
  - `records`
- `notes.json`
  - `version`
  - `updated_at`
  - `records`

Stable identity strategy:
- page records use `page::<normalized_page_path>`
- issue records use `issue::<normalized_page_path>::<issue_type_signature>`
- dashboard regeneration and fresh GSC/GA4 fetches do not overwrite these local state files

Workflow editing notes:
- the dashboard reads workflow state from `data/state/*.json` automatically
- to write changes back from the browser, open the dashboard through `localhost` and use a Chromium browser with File System Access API support
- click `Connect data/state`, choose the project's `data/state` folder, then statuses and notes will be written back locally
- if write access is not available, the dashboard keeps changes only in memory for the current session and tells you explicitly

`generate-dashboard` now also:
- captures a new historical snapshot of the canonical datasets
- compares the newest snapshot against the most recent prior weekly snapshot
- exposes the result through the `weekly_delta` section in `output/data.json`

If there is only one snapshot so far, the dashboard shows a clear message that no prior weekly baseline exists yet and that delta will appear after the next weekly run.

Open directly in a browser if local file fetch works in your browser, or run a simple local server:

```bash
cd seo-dashboard
python3 -m http.server 8000
```

Then open:

```bash
http://localhost:8000/output/seo-dashboard.html
```

Recommended local workflow:

```bash
python -m src.main --config settings.json --command fetch-gsc
python -m src.main --config settings.json --command fetch-ga4
python -m src.main --config settings.json --command merge-pages
python -m src.main --config settings.json --command score-pages
python -m src.main --config settings.json --command inspect-top-pages
python -m src.main --config settings.json --command generate-dashboard
```

Legacy dashboard files remain only for reference and are not used by default:
- `output/dashboard.html`
- `output/seo-dashboard-old.html`

## Configuration Notes

- `settings.example.json` is a sample file and should be copied to `settings.json` before local use.
- On the first `auth`, `test-gsc`, `test-ga4`, or `test-all` run, a browser window may open for manual Google login and consent.
- Successful authentication stores and reuses `config/token.json`.
- If the token expires and a refresh token is available, the project refreshes it automatically.
- `config/credentials.json` and `config/token.json` are intentionally excluded from git and must stay local.
- `fetch-gsc-page-query` can return a large dataset.
- Search Console API supports `rowLimit` up to 25,000 rows per request, and this project uses `startRow` pagination for additional batches.
- Even with correct pagination, Search Console API can still return only the top available rows because of Google-side data limits.
- GA4 uses the `landingPagePlusQueryString` dimension, which can include query strings.
- For future merge with GSC pages, the project stores normalized landing-page URLs and paths alongside the original GA4 dimension value.
- `recommended_action` is normalized to this enum:
  - `rewrite`
  - `expand`
  - `merge`
  - `request_indexing`
  - `no_action`
- `recommended_action_text` stores the human-readable explanation shown in queue and review outputs.
- Merge strategy:
  - primary key: `normalized_page_path`
  - fallback key: `normalized_page_url`
  - unmatched rows are preserved as `gsc_only` or `ga4_only`
- Top page movers strategy:
  - compares `last_28_days` against `previous_28_days`
  - uses a weighted delta across clicks, impressions, sessions, and conversions
  - exports to:
    - `data/processed/top_page_movers_last_28_vs_previous_28.csv`
    - `data/processed/top_page_movers_last_28_vs_previous_28.json`
- Indexing review strategy:
  - built as a formal dataset filtered to the configured inspection scope
  - flags verdict issues, canonical mismatches, robots blocks, and related inspection states
  - exports to:
    - `data/processed/indexing_review_last_28_days.csv`
    - `data/processed/indexing_review_last_28_days.json`
- Weekly delta strategy:
  - stores immutable local snapshots instead of overwriting prior state
  - compares the latest snapshot against the most recent prior snapshot from a different ISO week
  - tracks:
    - new issues this week
    - resolved issues this week
    - pages improved this week
    - pages declined this week
    - new quick wins this week
    - pages removed from quick wins this week
    - reason code changes
  - page movement uses a transparent weighted delta across quick-win score, clicks, impressions, sessions, conversions, and SERP position gain
  - exports to:
    - `data/history/snapshots/*.json`
    - `data/history/latest/latest_snapshot.json`
    - `data/history/latest/latest_weekly_delta.json`
- Scoring strategy:
  - `impact_score = (gsc_impressions * 0.3) + (gsc_clicks * 0.2) + (ga4_sessions * 0.3) + (ga4_conversions * 0.2) + (1000 / gsc_position)`
  - `effort_score = 1 + 2 if gsc_ctr < 0.02 + 3 if gsc_position > 20 + 1 if bounce-rate proxy > 70%`
  - bounce-rate proxy is calculated as `1 - (ga4_engaged_sessions / ga4_sessions)` when sessions are available
  - `quick_win_score = impact_score / effort_score`
- Queue sort order:
  - `quick_win_score` descending
  - `ga4_conversions` descending
  - `gsc_impressions` descending
- Queue output files:
  - `data/processed/page_queue_top_100.csv`
  - `data/processed/page_queue_top_100.json`
- URL Inspection output files:
  - `data/raw/gsc_inspection_top_500.json`
  - `data/processed/unified_pages_inspected.csv`
- URL Inspection quota handling:
  - daily property quota: 2000 URLs
  - per-minute quota: 600 URLs
  - this project processes up to 100 URLs per batch and sleeps 10 seconds between batches
  - if quota is exceeded mid-run, collected inspection results are preserved and the remaining URLs stay unenriched

## Dashboard Features

- single official HTML entrypoint with embedded CSS and JavaScript
- one canonical frontend data contract at `output/data.json`
- KPI cards for GSC and GA4
- GSC sitewide trend chart with Plotly
- date range selector for `28 days`, `90 days`, and `365 days`
- RU/EN localization with Russian as the default UI language
- top quick wins table
- queries section
- rule-based brand vs non-brand query split
- rule-based query intent classification
- page segmentation for blog, commercial, and other URL groups
- saved views for non-brand opportunities, blog-only, commercial pages, indexing issues, and quick wins
- top page movers section
- weekly delta section
- indexing review section
- unified pages explorer section
- clickable page-path links in the major page-based tables
- sticky multi-filter bar
- light and dark mode toggle
- graceful missing-file and fetch-blocked states

## Structure Overview

- `src/main.py` - CLI entrypoint
- `src/config.py` - settings loading and validation
- `src/logger.py` - app and error loggers
- `src/clients/gsc_client.py` - Search Console smoke tests plus performance data fetch helpers
- `src/clients/ga4_client.py` - GA4 smoke tests plus landing-page data fetch helpers
- `src/services/oauth_service.py` - OAuth token loading, refresh, browser login, and persistence
- `src/services/gsc_fetch_service.py` - high-level GSC fetch orchestration and export layer
- `src/services/ga4_fetch_service.py` - high-level GA4 landing-page fetch orchestration and export layer
- `src/services/merge_service.py` - normalization and merge layer for unified page-level data
- `src/services/scoring_service.py` - impact/effort scoring, reason codes, and quick-win queue generation
- `src/clients/inspection_client.py` - URL Inspection API client helpers, parsing, and quota-aware batching
- `src/services/inspection_service.py` - queue-driven inspection orchestration and unified dataset enrichment
- `src/services/dashboard_service.py` - canonical dashboard data-contract generation layer
- `src/services/history_service.py` - local historical snapshot storage and weekly delta generation
- `src/services/interpretation_service.py` - local rule-based brand, intent, and page-segment interpretation layer
- `config/seo_rules.json` - editable local rules for brand terms, intent heuristics, and page segmentation
- `output/seo-dashboard.html` - official read-only static dashboard for local artifact visualization
- `output/data.json` - canonical dashboard contract consumed by the official dashboard
- `data/history/` - local snapshot and latest-delta storage
- `src/utils/` - reusable helpers
- `data/`, `logs/`, `output/` - runtime directories

## Interpretation Rules

The dashboard uses only local, editable rules. No paid keyword APIs or external SEO tools are required.

- brand split is based on `config/seo_rules.json -> brand_terms`
- query intent is based on transparent keyword buckets in `config/seo_rules.json -> intent_rules`
- page segment is based on directory/path rules in `config/seo_rules.json -> page_segments`

Current canonical contract fields include:

- query rows:
  - `query_brand_classification`
  - `query_brand_confidence`
  - `query_brand_matches`
  - `query_brand_source`
  - `query_intent`
  - `query_intent_confidence`
  - `query_intent_matches`
  - `query_intent_source`
- page-based rows where applicable:
  - `page_segment`
  - `page_segment_confidence`
  - `page_segment_source`
  - `page_directory_group`

The intent labels are heuristic only:

- `informational`
- `commercial`
- `navigational`
- `lead_intent`
- `unknown`

If no previous artifact exists for comparison or a rule does not match clearly, the dashboard shows that limitation explicitly instead of pretending the classification is certain.

## Example `ga4_bundle.json`

```json
{
  "property_resource": "properties/350621663",
  "site_url": "https://international-surrogacy.com/",
  "generated_at": "2026-03-31T12:00:00+00:00",
  "date_windows": {
    "last_28_days": {"start_date": "2026-03-04", "end_date": "2026-03-31"}
  },
  "landing_page_reports": {
    "last_28_days": [
      {
        "date_range": "last_28_days",
        "landing_page_plus_query_string": "https://international-surrogacy.com/blog/example",
        "landing_page_plus_query_string_original": "/blog/example/",
        "normalized_page_url": "https://international-surrogacy.com/blog/example",
        "normalized_page_path": "/blog/example",
        "sessions": 42.0,
        "engaged_sessions": 30.0,
        "conversions": 3.0
      }
    ]
  },
  "summaries": {
    "landing_last_28_days": {
      "report_name": "landing_page_report",
      "window_name": "last_28_days",
      "start_date": "2026-03-04",
      "end_date": "2026-03-31",
      "row_count": 123,
      "output_path": "data/raw/ga4_landing_last_28_days.csv",
      "skipped_export": false
    }
  },
  "output_files": [
    "data/raw/ga4_landing_last_28_days.csv",
    "data/raw/ga4_bundle.json"
  ]
}
```

## Example unified row

```json
{
  "page_original_gsc": "https://international-surrogacy.com/blog/example/",
  "page_original_ga4": "/blog/example/?utm_source=test",
  "normalized_page_url": "https://international-surrogacy.com/blog/example",
  "normalized_page_path": "/blog/example",
  "gsc_clicks": 120.0,
  "gsc_impressions": 2200.0,
  "gsc_ctr": 0.0545,
  "gsc_position": 11.4,
  "ga4_sessions": 85.0,
  "ga4_engaged_sessions": 60.0,
  "ga4_conversions": 4.0,
  "data_source_match_type": "path_match"
}
```

## Example scored row

```json
{
  "normalized_page_path": "/blog/example",
  "normalized_page_url": "https://international-surrogacy.com/blog/example",
  "page_original_gsc": "https://international-surrogacy.com/blog/example/",
  "page_original_ga4": "/blog/example/?utm_source=test",
  "gsc_clicks": 120.0,
  "gsc_impressions": 2200.0,
  "gsc_ctr": 0.018,
  "gsc_position": 11.4,
  "ga4_sessions": 85.0,
  "ga4_engaged_sessions": 60.0,
  "ga4_conversions": 4.0,
  "ga4_bounce_rate_proxy": 0.2941,
  "impact_score": 828.32,
  "effort_score": 3.0,
  "quick_win_score": 276.11,
  "reason_code": "high_impressions_low_ctr",
  "recommended_action": "rewrite",
  "recommended_action_text": "Rewrite title and meta description to improve CTR.",
  "data_source_match_type": "path_match"
}
```

## Queue reason codes

- `noindex_detected` - inspection indicates a noindex-related indexing state
- `canonical_mismatch` - Google canonical and user canonical differ
- `robots_txt_blocked` - inspection indicates robots.txt blocking or fetch blocking
- `not_indexed_crawlable` - page is crawlable but still not indexed
- `high_impressions_low_ctr` - impressions above 1000 and CTR below 2%
- `deep_serp_opportunity` - average position deeper than 15 with at least 500 impressions
- `traffic_without_conversions` - sessions above 100 and no conversions
- `new_page_traffic` - sessions above 50 but impressions below 100
- `underperforming_content` - more than 10 clicks but average position still worse than 10
- `no_data` - fallback when no stronger rule matches

## Example inspection result

```json
{
  "inspected_url": "https://international-surrogacy.com/blog/example",
  "verdict": "PASS",
  "coverage_state": "Submitted and indexed",
  "indexing_state": "INDEXING_ALLOWED",
  "last_crawl_time": "2026-03-30T08:42:12Z",
  "page_fetch_state": "SUCCESSFUL",
  "robots_txt_state": "ALLOWED",
  "google_canonical": "https://international-surrogacy.com/blog/example",
  "user_canonical": "https://international-surrogacy.com/blog/example",
  "error_message": ""
}
```

## Example inspected unified row

```json
{
  "normalized_page_path": "/blog/example",
  "normalized_page_url": "https://international-surrogacy.com/blog/example",
  "inspection_verdict": "PASS",
  "inspection_coverage_state": "Submitted and indexed",
  "inspection_indexing_state": "INDEXING_ALLOWED",
  "inspection_page_fetch_state": "SUCCESSFUL",
  "inspection_robots_txt_state": "ALLOWED",
  "inspection_google_canonical": "https://international-surrogacy.com/blog/example",
  "inspection_user_canonical": "https://international-surrogacy.com/blog/example",
  "inspection_effort_multiplier": 1.0,
  "impact_score": 828.32,
  "effort_score": 3.0,
  "quick_win_score": 276.11,
  "reason_code": "high_impressions_low_ctr",
  "recommended_action": "rewrite",
  "recommended_action_text": "Rewrite title and meta description to improve CTR."
}
```

## Next Step

The next implementation step can focus separately on:

1. sitemap and crawl enrichment
2. scoring refinements and action automation
3. richer dashboard interactions and workflow status handling
