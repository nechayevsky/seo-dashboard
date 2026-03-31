from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from .config import AppConfig, ConfigError, load_config, read_config_data, validate_config_data
from .logger import setup_logging
from .models import (
    GA4ReportBundle,
    GA4SmokeTestResult,
    GSCReportBundle,
    GSCSmokeTestResult,
    InspectionSummary,
    InitReport,
    MergeSummary,
    QuickWinQueueRow,
    ScoringSummary,
)
from .paths import ProjectPaths
from .utils.io_utils import touch_file

CONFIGURED_PATH_FIELDS = (
    "output_html",
    "output_data_json",
    "log_app_file",
    "log_error_file",
    "google_oauth_credentials_file",
    "google_oauth_token_file",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SEO Dashboard CLI.")
    parser.add_argument("--config", required=True, help="Path to the JSON settings file.")
    parser.add_argument(
        "--command",
        required=True,
        choices=(
            "init",
            "validate-config",
            "show-paths",
            "auth",
            "test-gsc",
            "test-ga4",
            "test-all",
            "fetch-gsc",
            "fetch-gsc-trends",
            "fetch-gsc-queries",
            "fetch-gsc-pages",
            "fetch-gsc-page-query",
            "fetch-ga4",
            "fetch-ga4-landing",
            "merge-pages",
            "score-pages",
            "inspect-top-pages",
            "generate-dashboard",
            "enrich-with-sitemap",
        ),
        help="Command to execute.",
    )
    return parser


def print_validation_errors(errors: list[str]) -> None:
    print("Configuration errors:")
    for error in errors:
        print(f"- {error}")


def load_runtime_context(
    config_path: Path,
    project_paths: ProjectPaths,
    action_name: str,
) -> tuple[AppConfig, logging.Logger, logging.Logger] | None:
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        print(f"{action_name} failed.")
        print(str(exc))
        if exc.errors:
            print_validation_errors(exc.errors)
        return None

    app_logger, error_logger = setup_logging(project_paths, config)
    return config, app_logger, error_logger


def print_auth_summary(success: bool, config: AppConfig, error_message: str | None = None) -> None:
    print(f"oauth: {'ok' if success else 'fail'}")
    print(f"credentials file: {config.resolve_path(config.google_oauth_credentials_file)}")
    print(f"token file: {config.resolve_path(config.google_oauth_token_file)}")
    if error_message:
        print(f"error: {error_message}")


def print_gsc_summary(result: GSCSmokeTestResult) -> None:
    print("Google Search Console smoke test")
    print(f"access: {'ok' if result.access_ok else 'fail'}")
    print(f"target site found: {'yes' if result.target_site_found else 'no'}")
    print(f"total accessible sites: {result.total_accessible_sites}")
    print(f"sitemaps found: {result.sitemaps_found}")
    if result.error_message:
        print(f"error: {result.error_message}")


def print_ga4_summary(result: GA4SmokeTestResult) -> None:
    print("Google Analytics Data API smoke test")
    print(f"access: {'ok' if result.access_ok else 'fail'}")
    print(f"property: {result.property_resource}")
    print(f"rows returned: {result.rows_returned}")
    if result.sample_rows:
        print("sample rows:")
        for row in result.sample_rows:
            print(f"- date={row.get('date', '')}, sessions={row.get('sessions', '')}")
    if result.error_message:
        print(f"error: {result.error_message}")


def print_gsc_fetch_summary(bundle: GSCReportBundle) -> None:
    print("Google Search Console fetch")
    print(f"site_url: {bundle.site_url}")
    print("date windows used:")
    for window_name, date_range in bundle.date_windows.items():
        print(f"- {window_name}: {date_range.start_date} -> {date_range.end_date}")

    def _print_windowed_counts(label: str, key_prefix: str) -> None:
        matching = {
            window_name: bundle.summaries.get(f"{key_prefix}_{window_name}")
            for window_name in ("last_28_days", "previous_28_days", "last_90_days", "last_365_days")
        }
        if any(matching.values()):
            print(f"{label} rows count per window:")
            for window_name, summary in matching.items():
                if summary:
                    print(f"- {window_name}: {summary.row_count}")

    _print_windowed_counts("trends", "sitewide")
    _print_windowed_counts("queries", "queries")
    _print_windowed_counts("pages", "pages")
    _print_windowed_counts("countries", "countries")
    _print_windowed_counts("devices", "devices")

    page_query_summary = bundle.summaries.get("page_query_last_28_days")
    if page_query_summary:
        print(f"page_query rows (last_28_days): {page_query_summary.row_count}")

    print("output files saved:")
    if bundle.output_files:
        for output_path in bundle.output_files:
            print(f"- {output_path}")
    else:
        print("- none")


def print_ga4_fetch_summary(bundle: GA4ReportBundle) -> None:
    print("Google Analytics Data API fetch")
    print(f"property: {bundle.property_resource}")
    print("date windows used:")
    for window_name, date_range in bundle.date_windows.items():
        print(f"- {window_name}: {date_range.start_date} -> {date_range.end_date}")

    print("landing pages rows count per window:")
    for window_name in ("last_28_days", "previous_28_days", "last_90_days", "last_365_days"):
        summary = bundle.summaries.get(f"landing_{window_name}")
        if summary:
            print(f"- {window_name}: {summary.row_count}")

    print("output files saved:")
    if bundle.output_files:
        for output_path in bundle.output_files:
            print(f"- {output_path}")
    else:
        print("- none")


def print_merge_summary(summary: MergeSummary) -> None:
    print("Unified pages merge")
    print(f"gsc rows loaded: {summary.gsc_rows_loaded}")
    print(f"ga4 rows loaded: {summary.ga4_rows_loaded}")
    print(f"merged rows: {summary.merged_rows}")
    print(f"path matches: {summary.path_matches}")
    print(f"url matches: {summary.url_matches}")
    print(f"gsc only rows: {summary.gsc_only_rows}")
    print(f"ga4 only rows: {summary.ga4_only_rows}")
    print("output files saved:")
    if summary.output_files:
        for output_path in summary.output_files:
            print(f"- {output_path}")
    else:
        print("- none")


def print_scoring_summary(
    summary: ScoringSummary,
    queue_rows: list[QuickWinQueueRow],
) -> None:
    print("Quick wins scoring")
    print(f"unified rows loaded: {summary.unified_rows_loaded}")
    print(f"scored rows: {summary.scored_rows}")
    print(f"queue rows: {summary.queue_rows}")
    print("reason code counts:")
    if summary.reason_code_counts:
        for reason_code, count in sorted(
            summary.reason_code_counts.items(),
            key=lambda item: (-item[1], item[0]),
        ):
            print(f"- {reason_code}: {count}")
    else:
        print("- none")

    if queue_rows:
        print("top queue preview:")
        for row in queue_rows[:3]:
            print(
                f"- {row.normalized_page_path} | quick_win_score={row.quick_win_score:.2f} | "
                f"reason={row.reason_code} | action={row.recommended_action}"
            )

    print("output files saved:")
    if summary.output_files:
        for output_path in summary.output_files:
            print(f"- {output_path}")
    else:
        print("- none")


def print_inspection_summary(summary: InspectionSummary) -> None:
    print("URL inspection enrichment")
    print(f"queue rows loaded: {summary.queue_rows_loaded}")
    print(f"urls selected: {summary.urls_selected}")
    print(f"inspection results: {summary.inspection_results_count}")
    print(f"successful inspections: {summary.successful_inspections}")
    print(f"reused cached results: {summary.reused_cached_results}")
    print(f"fresh requests made: {summary.fresh_requests_made}")
    print(f"quota capped: {'yes' if summary.quota_capped else 'no'}")
    print("output files saved:")
    if summary.output_files:
        for output_path in summary.output_files:
            print(f"- {output_path}")
    else:
        print("- none")


def run_init(config_path: Path, project_paths: ProjectPaths) -> int:
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        print("Initialization failed.")
        print(str(exc))
        if exc.errors:
            print_validation_errors(exc.errors)
        return 1

    report = InitReport()
    report.created_directories.extend(
        str(path) for path in project_paths.ensure_base_directories()
    )
    report.created_directories.extend(
        str(path)
        for path in project_paths.ensure_parents_for_targets(
            (
                config.output_html,
                config.output_data_json,
                config.log_app_file,
                config.log_error_file,
                config.google_oauth_credentials_file,
                config.google_oauth_token_file,
            )
        )
    )

    for log_target in (config.log_app_file, config.log_error_file):
        resolved_log_path = project_paths.resolve(log_target)
        if not resolved_log_path.exists():
            report.initialized_files.append(str(resolved_log_path))
        touch_file(resolved_log_path)

    app_logger, _ = setup_logging(project_paths, config)
    app_logger.info("Project initialization completed for %s", config.project_name)

    print("Initialization complete.")
    print(f"Project: {config.project_name}")
    print(f"Site URL: {config.site_url}")
    print(f"Project root: {project_paths.project_root}")

    if report.created_directories:
        print("Created directories:")
        for directory in report.created_directories:
            print(f"- {directory}")
    else:
        print("Created directories: none")

    if report.initialized_files:
        print("Initialized log files:")
        for log_file in report.initialized_files:
            print(f"- {log_file}")
    else:
        print("Initialized log files: already present")

    return 0


def run_validate_config(config_path: Path) -> int:
    try:
        payload = read_config_data(config_path)
    except ConfigError as exc:
        print(str(exc))
        return 1

    validation_result = validate_config_data(payload)
    if not validation_result.is_valid:
        print("Config validation failed.")
        print_validation_errors(validation_result.errors)
        return 1

    config = AppConfig.from_dict(payload, config_path)
    print("Config is valid.")
    print(f"project_name: {config.project_name}")
    print(f"site_url: {config.site_url}")
    print(f"ga4_property_id: {config.ga4_property_id}")
    print(f"default_language: {config.default_language}")
    return 0


def run_show_paths(config_path: Path, project_paths: ProjectPaths) -> int:
    entries: dict[str, str] = project_paths.as_dict()
    entries["config_file"] = str(config_path.resolve())

    try:
        payload: dict[str, Any] = read_config_data(config_path)
    except ConfigError as exc:
        print(f"Warning: {exc}")
        payload = {}

    for field_name in CONFIGURED_PATH_FIELDS:
        raw_value = payload.get(field_name)
        if isinstance(raw_value, str) and raw_value.strip():
            entries[field_name] = str(project_paths.resolve(raw_value))

    for name, value in entries.items():
        print(f"{name}: {value}")

    return 0


def run_auth(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Authentication")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.oauth_service import get_google_credentials_from_config

        credentials = get_google_credentials_from_config(config, app_logger)
        print_auth_summary(success=True, config=config)
        app_logger.info(
            "OAuth authentication command completed with %s scope(s).",
            len(credentials.scopes or []),
        )
        return 0
    except Exception as exc:
        error_logger.error("OAuth authentication failed: %s", exc)
        print_auth_summary(success=False, config=config, error_message=str(exc))
        return 1


def run_test_gsc(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "GSC smoke test")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .clients.gsc_client import run_gsc_smoke_test
        from .services.oauth_service import get_google_credentials_from_config

        credentials = get_google_credentials_from_config(config, app_logger)
        result = run_gsc_smoke_test(credentials, config.site_url, app_logger)
    except Exception as exc:
        error_logger.error("GSC smoke test failed before completion: %s", exc)
        result = GSCSmokeTestResult(
            target_site=config.site_url,
            error_message=str(exc),
        )

    print_gsc_summary(result)
    return 0 if result.access_ok else 1


def run_test_ga4(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "GA4 smoke test")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .clients.ga4_client import run_ga4_smoke_test
        from .services.oauth_service import get_google_credentials_from_config

        credentials = get_google_credentials_from_config(config, app_logger)
        result = run_ga4_smoke_test(credentials, config.ga4_property_resource, app_logger)
    except Exception as exc:
        error_logger.error("GA4 smoke test failed before completion: %s", exc)
        result = GA4SmokeTestResult(
            property_resource=config.ga4_property_resource,
            error_message=str(exc),
        )

    print_ga4_summary(result)
    return 0 if result.access_ok else 1


def run_test_all(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Combined API smoke test")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .clients.ga4_client import run_ga4_smoke_test
        from .clients.gsc_client import run_gsc_smoke_test
        from .services.oauth_service import get_google_credentials_from_config

        credentials = get_google_credentials_from_config(config, app_logger)
    except Exception as exc:
        error_logger.error("Combined API smoke test failed during authentication: %s", exc)
        print("Combined API smoke test")
        print("overall: fail")
        print(f"error: {exc}")
        return 1

    gsc_result = run_gsc_smoke_test(credentials, config.site_url, app_logger)
    ga4_result = run_ga4_smoke_test(credentials, config.ga4_property_resource, app_logger)

    print_gsc_summary(gsc_result)
    print()
    print_ga4_summary(ga4_result)
    print()

    overall_success = gsc_result.access_ok and ga4_result.access_ok
    print("Combined API smoke test")
    print(f"overall: {'ok' if overall_success else 'fail'}")

    return 0 if overall_success else 1


def run_fetch_bundle_command(
    config_path: Path,
    project_paths: ProjectPaths,
    action_name: str,
    fetch_method_name: str,
) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, action_name)
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.gsc_fetch_service import GSCFetchService

        fetch_service = GSCFetchService(config, project_paths, app_logger)
        fetch_method = getattr(fetch_service, fetch_method_name)
        bundle = fetch_method()
    except Exception as exc:
        error_logger.error("%s failed: %s", action_name, exc)
        print("Google Search Console fetch")
        print("status: fail")
        print(f"error: {exc}")
        return 1

    print("status: ok")
    print_gsc_fetch_summary(bundle)
    return 0


def run_fetch_ga4_command(
    config_path: Path,
    project_paths: ProjectPaths,
    action_name: str,
    fetch_method_name: str,
) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, action_name)
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.ga4_fetch_service import GA4FetchService

        fetch_service = GA4FetchService(config, project_paths, app_logger)
        fetch_method = getattr(fetch_service, fetch_method_name)
        bundle = fetch_method()
    except Exception as exc:
        error_logger.error("%s failed: %s", action_name, exc)
        print("Google Analytics Data API fetch")
        print("status: fail")
        print(f"error: {exc}")
        return 1

    print("status: ok")
    print_ga4_fetch_summary(bundle)
    return 0


def run_merge_pages(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Page merge")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.merge_service import MergeService

        merge_service = MergeService(config, project_paths, app_logger)
        _, summary = merge_service.build_unified_pages_dataset()
    except Exception as exc:
        error_logger.error("Page merge failed: %s", exc)
        print("Unified pages merge")
        print("status: fail")
        print(f"error: {exc}")
        return 1

    print("status: ok")
    print_merge_summary(summary)
    return 0


def run_score_pages(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Page scoring")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.scoring_service import ScoringService

        scoring_service = ScoringService(config, project_paths, app_logger)
        _, queue_rows, summary = scoring_service.build_page_queue(top_n=100)
    except Exception as exc:
        error_logger.error("Page scoring failed: %s", exc)
        print("Quick wins scoring")
        print("status: fail")
        print(f"error: {exc}")
        return 1

    print("status: ok")
    print_scoring_summary(summary, queue_rows)
    return 0


def run_inspect_top_pages(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "URL inspection enrichment")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.inspection_service import InspectionService

        inspection_service = InspectionService(config, project_paths, app_logger)
        _, _, summary = inspection_service.inspect_top_pages(top_n=500)
    except Exception as exc:
        error_logger.error("URL inspection enrichment failed: %s", exc)
        print("URL inspection enrichment")
        print("status: fail")
        print(f"error: {exc}")
        return 1

    print("status: ok")
    print_inspection_summary(summary)
    return 0


def run_generate_dashboard(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Dashboard generation")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.dashboard_service import DashboardService

        dashboard_service = DashboardService(config, project_paths, app_logger)
        result = dashboard_service.generate()
        print("Dashboard generation")
        print("status: ok")
        print(f"official dashboard: {result['html_path']}")
        print(f"canonical data contract: {result['data_path']}")
        if result["output_files"]:
            print("written artifacts:")
            for output_path in result["output_files"]:
                print(f"- {output_path}")

        validation = result.get("validation", {})
        missing_sections = validation.get("missing_sections", [])
        missing_files = validation.get("missing_files", [])
        warnings = validation.get("warnings", [])

        print(f"validation ready: {'yes' if validation.get('is_ready') else 'partial'}")
        if missing_files:
            print("missing files:")
            for relative_path in missing_files:
                print(f"- {relative_path}")
        if missing_sections:
            print("missing sections:")
            for section_name in missing_sections:
                print(f"- {section_name}")
        if warnings:
            print("warnings:")
            for warning in warnings:
                print(f"- {warning}")
        return 0
    except Exception as exc:
        error_logger.error("Dashboard generation failed: %s", exc)
        print("Dashboard generation")
        print("status: fail")
        print(f"error: {exc}")
        return 1


def run_enrich_with_sitemap(config_path: Path, project_paths: ProjectPaths) -> int:
    runtime_context = load_runtime_context(config_path, project_paths, "Sitemap enrichment")
    if runtime_context is None:
        return 1

    config, app_logger, error_logger = runtime_context

    try:
        from .services.sitemap_service import SitemapService

        sitemap_service = SitemapService(config, project_paths, app_logger)
        output_path = sitemap_service.enrich_data()
        if output_path.startswith("Error:"):
            print(output_path)
            return 1
        print(f"Sitemap enrichment completed: {output_path}")
        return 0
    except Exception as exc:
        error_logger.error("Sitemap enrichment failed: %s", exc)
        print("Sitemap enrichment")
        print("status: fail")
        print(f"error: {exc}")
        return 1


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    project_paths = ProjectPaths.discover()
    config_path = Path(args.config).expanduser()

    if args.command == "init":
        return run_init(config_path, project_paths)
    if args.command == "validate-config":
        return run_validate_config(config_path)
    if args.command == "show-paths":
        return run_show_paths(config_path, project_paths)
    if args.command == "auth":
        return run_auth(config_path, project_paths)
    if args.command == "test-gsc":
        return run_test_gsc(config_path, project_paths)
    if args.command == "test-ga4":
        return run_test_ga4(config_path, project_paths)
    if args.command == "test-all":
        return run_test_all(config_path, project_paths)
    if args.command == "fetch-gsc":
        return run_fetch_bundle_command(
            config_path,
            project_paths,
            "GSC fetch",
            "fetch_bundle",
        )
    if args.command == "fetch-gsc-trends":
        return run_fetch_bundle_command(
            config_path,
            project_paths,
            "GSC trends fetch",
            "fetch_trends_bundle",
        )
    if args.command == "fetch-gsc-queries":
        return run_fetch_bundle_command(
            config_path,
            project_paths,
            "GSC query fetch",
            "fetch_queries_bundle",
        )
    if args.command == "fetch-gsc-pages":
        return run_fetch_bundle_command(
            config_path,
            project_paths,
            "GSC page fetch",
            "fetch_pages_bundle",
        )
    if args.command == "fetch-gsc-page-query":
        return run_fetch_bundle_command(
            config_path,
            project_paths,
            "GSC page+query fetch",
            "fetch_page_query_bundle",
        )
    if args.command == "fetch-ga4":
        return run_fetch_ga4_command(
            config_path,
            project_paths,
            "GA4 fetch",
            "fetch_bundle",
        )
    if args.command == "fetch-ga4-landing":
        return run_fetch_ga4_command(
            config_path,
            project_paths,
            "GA4 landing page fetch",
            "fetch_landing_bundle",
        )
    if args.command == "merge-pages":
        return run_merge_pages(config_path, project_paths)
    if args.command == "score-pages":
        return run_score_pages(config_path, project_paths)
    if args.command == "inspect-top-pages":
        return run_inspect_top_pages(config_path, project_paths)
    if args.command == "generate-dashboard":
        return run_generate_dashboard(config_path, project_paths)
    if args.command == "enrich-with-sitemap":
        return run_enrich_with_sitemap(config_path, project_paths)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
