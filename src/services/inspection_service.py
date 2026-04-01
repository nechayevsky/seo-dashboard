from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from ..clients.inspection_client import (
    DEFAULT_BATCH_SIZE,
    INSPECTION_DAILY_QUOTA,
    batch_inspect_urls,
    build_inspection_service,
)
from ..config import AppConfig
from ..logger import APP_LOGGER_NAME
from ..models import InspectionResult, InspectionSummary, UnifiedPageWithInspection
from ..paths import ProjectPaths
from ..utils.io_utils import read_csv_file, read_json_file, write_csv_file, write_json_file
from .oauth_service import get_google_credentials_from_config
from .scoring_service import score_page_row
from .sitemap_service import SitemapService

INSPECTED_UNIFIED_FIELDNAMES = [
    "page_original_gsc",
    "page_original_ga4",
    "normalized_page_url",
    "normalized_page_path",
    "gsc_clicks",
    "gsc_impressions",
    "gsc_ctr",
    "gsc_position",
    "ga4_sessions",
    "ga4_engaged_sessions",
    "ga4_conversions",
    "data_source_match_type",
    "inspection_verdict",
    "inspection_coverage_state",
    "inspection_indexing_state",
    "inspection_last_crawl_time",
    "inspection_page_fetch_state",
    "inspection_robots_txt_state",
    "inspection_google_canonical",
    "inspection_user_canonical",
    "inspection_error_message",
    "ga4_bounce_rate_proxy",
    "inspection_effort_multiplier",
    "impact_score",
    "effort_score",
    "quick_win_score",
    "reason_code",
    "recommended_action",
    "recommended_action_text",
]


class InspectionServiceError(Exception):
    """Raised when URL Inspection enrichment cannot be completed."""


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _default_logger(logger: Any | None = None) -> Any:
    return logger or logging.getLogger(APP_LOGGER_NAME)


def _load_csv_or_raise(path: str | Path, label: str) -> list[dict[str, str]]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise InspectionServiceError(f"{label} not found: {resolved_path}")
    return read_csv_file(resolved_path)


def _load_or_build_queue_rows(
    app_config: AppConfig,
    project_paths: ProjectPaths,
    top_n: int,
    logger: Any,
    overwrite: bool,
) -> list[dict[str, Any]]:
    requested_queue_path = app_config.resolve_path(f"data/processed/page_queue_top_{top_n}.csv")
    if requested_queue_path.exists() and not overwrite:
        return read_csv_file(requested_queue_path)

    fallback_queue_path = app_config.resolve_path("data/processed/page_queue_top_100.csv")
    if top_n <= 100 and fallback_queue_path.exists() and not overwrite:
        return read_csv_file(fallback_queue_path)

    from .scoring_service import ScoringService

    scoring_service = ScoringService(app_config, project_paths, logger)
    _, queue_rows, _ = scoring_service.build_page_queue(top_n=top_n, overwrite=overwrite)
    return [row.to_dict() for row in queue_rows]


def _load_cached_results(path: str | Path) -> dict[str, InspectionResult]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        return {}

    try:
        payload = read_json_file(resolved_path)
    except Exception:
        return {}

    generated_at = str(payload.get("generated_at", "")).strip()
    cached_results: dict[str, InspectionResult] = {}
    for row in payload.get("results", []):
        if not isinstance(row, dict):
            continue
        inspected_url = str(row.get("inspected_url", "")).strip()
        if not inspected_url:
            continue
        cached_results[inspected_url] = InspectionResult(
            inspected_url=inspected_url,
            verdict=str(row.get("verdict", "")).strip(),
            coverage_state=str(row.get("coverage_state", "")).strip(),
            indexing_state=str(row.get("indexing_state", "")).strip(),
            last_crawl_time=str(row.get("last_crawl_time", "")).strip(),
            page_fetch_state=str(row.get("page_fetch_state", "")).strip(),
            robots_txt_state=str(row.get("robots_txt_state", "")).strip(),
            google_canonical=str(row.get("google_canonical", "")).strip(),
            user_canonical=str(row.get("user_canonical", "")).strip(),
            error_message=str(row.get("error_message", "")).strip(),
            inspected_at=str(row.get("inspected_at", "")).strip() or generated_at,
            source_type=str(row.get("source_type", "")).strip(),
        )
    return cached_results


def _sorted_queue_rows(queue_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        queue_rows,
        key=lambda row: (
            -_to_float(row.get("quick_win_score")),
            -_to_float(row.get("ga4_conversions")),
            -_to_float(row.get("gsc_impressions")),
            str(row.get("normalized_page_path", "")),
        ),
    )


def _inspection_result_by_url(results: Iterable[InspectionResult]) -> dict[str, InspectionResult]:
    return {
        result.inspected_url: result
        for result in results
        if result.inspected_url
    }


def _inspection_result_by_path(results: Iterable[InspectionResult]) -> dict[str, InspectionResult]:
    mapped: dict[str, InspectionResult] = {}

    for result in results:
        inspected_url = str(result.inspected_url or "").strip()
        if not inspected_url:
            continue
        path = urlparse(inspected_url).path or "/"
        mapped[path] = result

    return mapped


def _normalize_scope_path(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return path.rstrip("/") + "/"


def _is_in_scope(url: str, site_url: str, scope_prefix: str) -> bool:
    parsed_url = urlparse(str(url).strip())
    parsed_site = urlparse(site_url)
    if parsed_url.netloc and parsed_url.netloc != parsed_site.netloc:
        return False

    path = parsed_url.path or "/"
    normalized_scope = _normalize_scope_path(scope_prefix)
    if normalized_scope == "/":
        return True
    return path == normalized_scope.rstrip("/") or path.startswith(normalized_scope)


def _is_recent_enough(result: InspectionResult, refresh_days: int) -> bool:
    inspected_at = str(result.inspected_at or "").strip()
    if not inspected_at:
        return False
    try:
        inspected_dt = datetime.fromisoformat(inspected_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    age_days = (datetime.now(timezone.utc) - inspected_dt.astimezone(timezone.utc)).days
    return age_days < refresh_days


def save_inspection_results(
    results: Iterable[InspectionResult],
    output_path: str | Path,
    overwrite: bool = True,
) -> Path | None:
    materialized_results = list(results)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "results": [result.to_dict() for result in materialized_results],
    }
    return write_json_file(output_path, payload, overwrite=overwrite)


def enrich_unified_dataset_with_inspection(
    unified_df: Iterable[dict[str, Any]],
    inspection_results: Iterable[InspectionResult],
) -> list[UnifiedPageWithInspection]:
    results_by_url = _inspection_result_by_url(inspection_results)
    results_by_path = _inspection_result_by_path(inspection_results)
    enriched_rows: list[UnifiedPageWithInspection] = []

    for row in unified_df:
        normalized_page_url = str(row.get("normalized_page_url", "")).strip()
        normalized_page_path = str(row.get("normalized_page_path", "")).strip()
        inspection_result = results_by_url.get(normalized_page_url) or results_by_path.get(normalized_page_path)

        enriched_payload: dict[str, Any] = dict(row)
        enriched_payload.update(
            {
                "inspection_verdict": inspection_result.verdict if inspection_result else "",
                "inspection_coverage_state": inspection_result.coverage_state if inspection_result else "",
                "inspection_indexing_state": inspection_result.indexing_state if inspection_result else "",
                "inspection_last_crawl_time": inspection_result.last_crawl_time if inspection_result else "",
                "inspection_page_fetch_state": inspection_result.page_fetch_state if inspection_result else "",
                "inspection_robots_txt_state": inspection_result.robots_txt_state if inspection_result else "",
                "inspection_google_canonical": inspection_result.google_canonical if inspection_result else "",
                "inspection_user_canonical": inspection_result.user_canonical if inspection_result else "",
                "inspection_error_message": inspection_result.error_message if inspection_result else "",
            }
        )

        scored_row = score_page_row(enriched_payload)
        enriched_rows.append(
            UnifiedPageWithInspection(
                page_original_gsc=str(row.get("page_original_gsc", "")).strip(),
                page_original_ga4=str(row.get("page_original_ga4", "")).strip(),
                normalized_page_url=normalized_page_url,
                normalized_page_path=normalized_page_path or scored_row.normalized_page_path,
                gsc_clicks=_to_float(row.get("gsc_clicks")),
                gsc_impressions=_to_float(row.get("gsc_impressions")),
                gsc_ctr=_to_float(row.get("gsc_ctr")),
                gsc_position=_to_float(row.get("gsc_position")),
                ga4_sessions=_to_float(row.get("ga4_sessions")),
                ga4_engaged_sessions=_to_float(row.get("ga4_engaged_sessions")),
                ga4_conversions=_to_float(row.get("ga4_conversions")),
                data_source_match_type=str(row.get("data_source_match_type", "")).strip(),
                inspection_verdict=enriched_payload["inspection_verdict"],
                inspection_coverage_state=enriched_payload["inspection_coverage_state"],
                inspection_indexing_state=enriched_payload["inspection_indexing_state"],
                inspection_last_crawl_time=enriched_payload["inspection_last_crawl_time"],
                inspection_page_fetch_state=enriched_payload["inspection_page_fetch_state"],
                inspection_robots_txt_state=enriched_payload["inspection_robots_txt_state"],
                inspection_google_canonical=enriched_payload["inspection_google_canonical"],
                inspection_user_canonical=enriched_payload["inspection_user_canonical"],
                inspection_error_message=enriched_payload["inspection_error_message"],
                ga4_bounce_rate_proxy=scored_row.ga4_bounce_rate_proxy,
                inspection_effort_multiplier=scored_row.inspection_effort_multiplier,
                impact_score=scored_row.impact_score,
                effort_score=scored_row.effort_score,
                quick_win_score=scored_row.quick_win_score,
                reason_code=scored_row.reason_code,
                recommended_action=scored_row.recommended_action,
                recommended_action_text=scored_row.recommended_action_text,
            )
        )

    return enriched_rows


def _load_sitemap_urls(app_config: AppConfig, project_paths: ProjectPaths, logger: Any) -> set[str]:
    configured_sitemaps = [url for url in getattr(app_config, "sitemap_urls", ()) if str(url).strip()]
    if not configured_sitemaps and getattr(app_config, "sitemap_url", "").strip():
        configured_sitemaps = [app_config.sitemap_url]

    if not configured_sitemaps:
        return set()
    try:
        sitemap_service = SitemapService(app_config, project_paths, logger)
        sitemap_urls: set[str] = set()
        for sitemap_url in configured_sitemaps:
            sitemap_urls.update(sitemap_service.fetch_all_urls(sitemap_url))
        return {url.rstrip("/") for url in sitemap_urls if _is_in_scope(url, app_config.site_url, app_config.inspection_scope_prefix)}
    except Exception as exc:
        logger.warning("Sitemap URL prioritization could not be loaded: %s", exc)
        return set()


def _select_urls_from_queue(
    queue_rows: list[dict[str, Any]],
    unified_rows: list[dict[str, Any]],
    app_config: AppConfig,
    top_n: int,
    sitemap_urls: set[str],
    logger: Any,
) -> tuple[list[tuple[str, str]], bool]:
    unified_by_path = {
        str(row.get("normalized_page_path", "")).strip(): str(row.get("normalized_page_url", "")).strip()
        for row in unified_rows
        if str(row.get("normalized_page_path", "")).strip()
    }

    selected: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    effective_limit = min(top_n, app_config.inspection_daily_limit, INSPECTION_DAILY_QUOTA)
    quota_capped = top_n > effective_limit

    sitemap_first: list[tuple[str, str]] = []
    crawl_only: list[tuple[str, str]] = []

    for queue_row in _sorted_queue_rows(queue_rows):
        normalized_page_path = str(queue_row.get("normalized_page_path", "")).strip()
        normalized_page_url = unified_by_path.get(normalized_page_path, "").strip()
        if not normalized_page_url:
            logger.warning(
                "Queue row %s was skipped because no normalized_page_url was found in unified dataset.",
                normalized_page_path,
            )
            continue
        if normalized_page_url in seen_urls:
            continue
        if not _is_in_scope(normalized_page_url, app_config.site_url, app_config.inspection_scope_prefix):
            continue

        seen_urls.add(normalized_page_url)
        source_type = "sitemap" if normalized_page_url.rstrip("/") in sitemap_urls else "crawl_only"
        if source_type == "sitemap":
            sitemap_first.append((normalized_page_url, source_type))
        else:
            crawl_only.append((normalized_page_url, source_type))

    selected = (sitemap_first + crawl_only)[:effective_limit]
    return selected, quota_capped


def inspect_top_queue_pages(
    app_config: AppConfig,
    project_paths: ProjectPaths,
    top_n: int = 500,
    logger: Any | None = None,
    overwrite: bool = True,
) -> tuple[list[InspectionResult], list[UnifiedPageWithInspection], InspectionSummary]:
    active_logger = _default_logger(logger)
    unified_csv_path = app_config.resolve_path("data/processed/unified_pages_last_28_days.csv")
    inspection_json_path = app_config.resolve_path("data/raw/gsc_inspection_top_500.json")
    inspected_unified_csv_path = app_config.resolve_path("data/processed/unified_pages_inspected.csv")

    queue_rows = _load_or_build_queue_rows(
        app_config,
        project_paths,
        top_n=top_n,
        logger=active_logger,
        overwrite=overwrite,
    )
    unified_rows = _load_csv_or_raise(unified_csv_path, "Unified pages CSV")
    sitemap_urls = _load_sitemap_urls(app_config, project_paths, active_logger)
    selected_url_pairs, quota_capped = _select_urls_from_queue(
        queue_rows,
        unified_rows,
        app_config,
        top_n,
        sitemap_urls,
        active_logger,
    )
    candidate_urls = [url for url, _ in selected_url_pairs]

    active_logger.info(
        "Preparing URL inspection for %s URL(s) selected from %s queue rows within scope %s.",
        len(candidate_urls),
        len(queue_rows),
        app_config.inspection_scope_prefix,
    )

    cached_results = _load_cached_results(inspection_json_path)
    reusable_results: list[InspectionResult] = []
    urls_to_request: list[str] = []
    source_type_by_url = dict(selected_url_pairs)

    for url in candidate_urls:
        cached_result = cached_results.get(url)
        if cached_result and _is_recent_enough(cached_result, app_config.crawl_frequency_days):
            reusable_results.append(cached_result)
        else:
            urls_to_request.append(url)

    fresh_results: list[InspectionResult] = []
    if urls_to_request:
        credentials = get_google_credentials_from_config(app_config, active_logger)
        inspection_service = build_inspection_service(credentials)
        fresh_results = batch_inspect_urls(
            inspection_service,
            app_config.site_url,
            urls_to_request,
            batch_size=DEFAULT_BATCH_SIZE,
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        for result in fresh_results:
            result.inspected_at = now_iso
            result.source_type = source_type_by_url.get(result.inspected_url, "")

    for result in reusable_results:
        result.source_type = source_type_by_url.get(result.inspected_url, result.source_type)

    merged_results_by_url = {**cached_results}
    for result in reusable_results + fresh_results:
        merged_results_by_url[result.inspected_url] = result
    final_results = [merged_results_by_url[url] for url in candidate_urls if url in merged_results_by_url]

    written_inspection_json = save_inspection_results(final_results, inspection_json_path, overwrite=overwrite)

    enriched_rows = enrich_unified_dataset_with_inspection(unified_rows, final_results)
    written_inspected_csv = write_csv_file(
        inspected_unified_csv_path,
        [row.to_dict() for row in enriched_rows],
        INSPECTED_UNIFIED_FIELDNAMES,
        overwrite=overwrite,
    )

    summary = InspectionSummary(
        queue_rows_loaded=len(queue_rows),
        urls_selected=len(candidate_urls),
        inspection_results_count=len(final_results),
        successful_inspections=sum(1 for result in final_results if not result.error_message),
        quota_capped=quota_capped,
        reused_cached_results=len(reusable_results),
        fresh_requests_made=len(urls_to_request),
        output_files=[
            str(path)
            for path in (written_inspection_json, written_inspected_csv)
            if path is not None
        ],
    )

    active_logger.info(
        "Inspection enrichment completed with %s successful inspection result(s), %s reused from cache, %s fetched fresh.",
        summary.successful_inspections,
        summary.reused_cached_results,
        summary.fresh_requests_made,
    )
    return final_results, enriched_rows, summary


@dataclass(slots=True)
class InspectionService:
    app_config: AppConfig
    paths: ProjectPaths
    logger: Any

    def status(self) -> str:
        return (
            "Inspection service is prepared to enrich the unified dataset with URL Inspection "
            f"results under {self.paths.data_processed_dir}."
        )

    def inspect_top_pages(
        self,
        top_n: int = 500,
        overwrite: bool = True,
    ) -> tuple[list[InspectionResult], list[UnifiedPageWithInspection], InspectionSummary]:
        return inspect_top_queue_pages(
            self.app_config,
            self.paths,
            top_n=top_n,
            logger=self.logger,
            overwrite=overwrite,
        )
