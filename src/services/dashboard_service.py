from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..logger import APP_LOGGER_NAME
from ..models import IndexingReviewRow, InspectionResult, PageMoverRow
from ..paths import ProjectPaths
from ..utils.date_utils import get_date_ranges
from ..utils.io_utils import read_csv_file, read_json_file, write_csv_file, write_json_file
from .history_service import HistoryService
from .inspection_service import enrich_unified_dataset_with_inspection
from .interpretation_service import InterpretationService, RULES_FILENAME
from .merge_service import MergeService, WINDOW_TO_SUFFIX
from .scoring_service import ScoringService, score_page_row
from .workflow_service import WORKFLOW_STATUSES, WorkflowService

TOP_MOVERS_FIELDNAMES = [
    "normalized_page_path",
    "normalized_page_url",
    "page_segment",
    "page_segment_confidence",
    "page_segment_source",
    "page_directory_group",
    "current_gsc_clicks",
    "previous_gsc_clicks",
    "current_gsc_impressions",
    "previous_gsc_impressions",
    "current_ga4_sessions",
    "previous_ga4_sessions",
    "current_ga4_conversions",
    "previous_ga4_conversions",
    "click_delta",
    "impression_delta",
    "sessions_delta",
    "conversions_delta",
    "mover_score",
    "movement_direction",
    "workflow_scope",
    "workflow_page_key",
    "workflow_issue_key",
    "workflow_record_key",
    "workflow_status",
    "workflow_status_explicit",
    "workflow_status_source",
    "workflow_status_updated_at",
    "workflow_note",
    "workflow_note_present",
    "workflow_note_source",
    "workflow_note_updated_at",
]

INDEXING_REVIEW_FIELDNAMES = [
    "normalized_page_path",
    "normalized_page_url",
    "page_segment",
    "page_segment_confidence",
    "page_segment_source",
    "page_directory_group",
    "inspection_verdict",
    "inspection_coverage_state",
    "inspection_indexing_state",
    "inspection_page_fetch_state",
    "inspection_robots_txt_state",
    "inspection_google_canonical",
    "inspection_user_canonical",
    "inspection_last_crawl_time",
    "inspection_error_message",
    "has_issue",
    "issue_types",
    "recommended_action",
    "recommended_action_text",
    "source_type",
    "workflow_scope",
    "workflow_page_key",
    "workflow_issue_key",
    "workflow_record_key",
    "workflow_status",
    "workflow_status_explicit",
    "workflow_status_source",
    "workflow_status_updated_at",
    "workflow_note",
    "workflow_note_present",
    "workflow_note_source",
    "workflow_note_updated_at",
]


def _default_logger(logger: Any | None = None) -> Any:
    return logger or logging.getLogger(APP_LOGGER_NAME)


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return read_json_file(path)
    except Exception:
        return None


def _read_csv_if_exists(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        return read_csv_file(path)
    except Exception:
        return []


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _string(value: Any) -> str:
    return str(value or "").strip()


def _issue_types_for_row(row: dict[str, Any]) -> list[str]:
    issue_types: list[str] = []
    verdict = _string(row.get("inspection_verdict")).upper()
    coverage = _string(row.get("inspection_coverage_state"))
    indexing = _string(row.get("inspection_indexing_state"))
    robots = _string(row.get("inspection_robots_txt_state")).upper()
    google_canonical = _string(row.get("inspection_google_canonical"))
    user_canonical = _string(row.get("inspection_user_canonical"))
    error_message = _string(row.get("inspection_error_message"))

    if verdict and verdict != "PASS":
        issue_types.append("verdict_issue")
    if robots and robots != "ALLOWED":
        issue_types.append("robots_txt_blocked")
    if google_canonical and user_canonical and google_canonical != user_canonical:
        issue_types.append("canonical_mismatch")
    combined = " ".join(part.lower() for part in (coverage, indexing, error_message) if part)
    if "not indexed" in combined:
        issue_types.append("not_indexed")
    if "noindex" in combined:
        issue_types.append("noindex_detected")
    if error_message:
        issue_types.append("inspection_error")
    return issue_types


def _canonical_review_source_type(row: dict[str, Any]) -> str:
    return "inspection" if _string(row.get("inspection_verdict")) else "uninspected"


def _enrich_scored_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        scored_row = score_page_row(row).to_dict()
        enriched_rows.append({**row, **scored_row})
    return enriched_rows


def build_top_page_movers(
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
    top_n: int = 100,
) -> list[PageMoverRow]:
    previous_by_path = {
        _string(row.get("normalized_page_path")): row
        for row in previous_rows
        if _string(row.get("normalized_page_path"))
    }
    all_paths = {
        _string(row.get("normalized_page_path"))
        for row in current_rows + previous_rows
        if _string(row.get("normalized_page_path"))
    }

    movers: list[PageMoverRow] = []
    for path in all_paths:
        current_row = next((row for row in current_rows if _string(row.get("normalized_page_path")) == path), {})
        previous_row = previous_by_path.get(path, {})
        current_clicks = _to_float(current_row.get("gsc_clicks"))
        previous_clicks = _to_float(previous_row.get("gsc_clicks"))
        current_impressions = _to_float(current_row.get("gsc_impressions"))
        previous_impressions = _to_float(previous_row.get("gsc_impressions"))
        current_sessions = _to_float(current_row.get("ga4_sessions"))
        previous_sessions = _to_float(previous_row.get("ga4_sessions"))
        current_conversions = _to_float(current_row.get("ga4_conversions"))
        previous_conversions = _to_float(previous_row.get("ga4_conversions"))

        click_delta = current_clicks - previous_clicks
        impression_delta = current_impressions - previous_impressions
        sessions_delta = current_sessions - previous_sessions
        conversions_delta = current_conversions - previous_conversions
        mover_score = (
            (click_delta * 0.4)
            + (impression_delta * 0.1)
            + (sessions_delta * 0.3)
            + (conversions_delta * 10.0)
        )
        if mover_score == 0 and not any((current_clicks, previous_clicks, current_sessions, previous_sessions)):
            continue

        movers.append(
            PageMoverRow(
                normalized_page_path=path,
                normalized_page_url=_string(current_row.get("normalized_page_url") or previous_row.get("normalized_page_url")),
                current_gsc_clicks=current_clicks,
                previous_gsc_clicks=previous_clicks,
                current_gsc_impressions=current_impressions,
                previous_gsc_impressions=previous_impressions,
                current_ga4_sessions=current_sessions,
                previous_ga4_sessions=previous_sessions,
                current_ga4_conversions=current_conversions,
                previous_ga4_conversions=previous_conversions,
                click_delta=click_delta,
                impression_delta=impression_delta,
                sessions_delta=sessions_delta,
                conversions_delta=conversions_delta,
                mover_score=mover_score,
                movement_direction="up" if mover_score >= 0 else "down",
            )
        )

    ranked = sorted(movers, key=lambda row: (-abs(row.mover_score), row.normalized_page_path))
    return ranked[:top_n]


def build_indexing_review_rows(
    inspected_rows: list[dict[str, Any]],
    scope_prefix: str,
) -> list[IndexingReviewRow]:
    scope_prefix = scope_prefix.rstrip("/") or "/"
    review_rows: list[IndexingReviewRow] = []
    for row in inspected_rows:
        normalized_path = _string(row.get("normalized_page_path"))
        if normalized_path and scope_prefix != "/" and normalized_path != scope_prefix and not normalized_path.startswith(f"{scope_prefix}/"):
            continue

        issue_types = _issue_types_for_row(row)
        review_rows.append(
            IndexingReviewRow(
                normalized_page_path=normalized_path,
                normalized_page_url=_string(row.get("normalized_page_url")),
                inspection_verdict=_string(row.get("inspection_verdict")),
                inspection_coverage_state=_string(row.get("inspection_coverage_state")),
                inspection_indexing_state=_string(row.get("inspection_indexing_state")),
                inspection_page_fetch_state=_string(row.get("inspection_page_fetch_state")),
                inspection_robots_txt_state=_string(row.get("inspection_robots_txt_state")),
                inspection_google_canonical=_string(row.get("inspection_google_canonical")),
                inspection_user_canonical=_string(row.get("inspection_user_canonical")),
                inspection_last_crawl_time=_string(row.get("inspection_last_crawl_time")),
                inspection_error_message=_string(row.get("inspection_error_message")),
                has_issue=bool(issue_types),
                issue_types=issue_types,
                recommended_action=_string(row.get("recommended_action")) or "no_action",
                recommended_action_text=_string(row.get("recommended_action_text")),
                source_type=_canonical_review_source_type(row),
            )
        )

    return sorted(
        review_rows,
        key=lambda row: (
            not row.has_issue,
            row.normalized_page_path,
        ),
    )


@dataclass(slots=True)
class DashboardService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any | None = None

    def _load_gsc_bundle(self) -> dict[str, Any] | None:
        return _read_json_if_exists(self.paths.data_raw_dir / "gsc_bundle.json")

    def _load_ga4_bundle(self) -> dict[str, Any] | None:
        return _read_json_if_exists(self.paths.data_raw_dir / "ga4_bundle.json")

    def _load_unified_rows(self, window_name: str, overwrite: bool) -> list[dict[str, Any]]:
        if overwrite:
            gsc_csv_path = self.paths.data_raw_dir / f"gsc_pages_{WINDOW_TO_SUFFIX[window_name]}.csv"
            ga4_csv_path = self.paths.data_raw_dir / f"ga4_landing_{WINDOW_TO_SUFFIX[window_name]}.csv"
            if gsc_csv_path.exists() and ga4_csv_path.exists():
                merge_service = MergeService(self.config, self.paths, _default_logger(self.logger))
                rows, _ = merge_service.build_unified_pages_dataset(window_name=window_name, overwrite=True)
                return [row.to_dict() for row in rows]

        unified_json_path = self.paths.data_processed_dir / f"unified_pages_{WINDOW_TO_SUFFIX[window_name]}.json"
        payload = _read_json_if_exists(unified_json_path)
        if payload and isinstance(payload.get("rows"), list):
            return payload["rows"]

        gsc_csv_path = self.paths.data_raw_dir / f"gsc_pages_{WINDOW_TO_SUFFIX[window_name]}.csv"
        ga4_csv_path = self.paths.data_raw_dir / f"ga4_landing_{WINDOW_TO_SUFFIX[window_name]}.csv"
        if not gsc_csv_path.exists() or not ga4_csv_path.exists():
            return []

        merge_service = MergeService(self.config, self.paths, _default_logger(self.logger))
        rows, _ = merge_service.build_unified_pages_dataset(window_name=window_name, overwrite=overwrite)
        return [row.to_dict() for row in rows]

    def _load_or_build_queue_rows(self, overwrite: bool) -> list[dict[str, Any]]:
        if overwrite:
            unified_csv_path = self.paths.data_processed_dir / "unified_pages_last_28_days.csv"
            if unified_csv_path.exists():
                scoring_service = ScoringService(self.config, self.paths, _default_logger(self.logger))
                _, rows, _ = scoring_service.build_page_queue(window_name="last_28_days", top_n=100, overwrite=True)
                return [row.to_dict() for row in rows]

        queue_json_path = self.paths.data_processed_dir / "page_queue_top_100.json"
        payload = _read_json_if_exists(queue_json_path)
        if payload and isinstance(payload.get("rows"), list):
            return payload["rows"]

        scoring_service = ScoringService(self.config, self.paths, _default_logger(self.logger))
        _, rows, _ = scoring_service.build_page_queue(window_name="last_28_days", top_n=100, overwrite=overwrite)
        return [row.to_dict() for row in rows]

    def _load_or_build_inspected_rows(self, overwrite: bool) -> list[dict[str, Any]]:
        if overwrite:
            unified_rows = self._load_unified_rows("last_28_days", overwrite=True)
            inspection_payload = _read_json_if_exists(self.paths.data_raw_dir / "gsc_inspection_top_500.json")
            if inspection_payload and inspection_payload.get("results"):
                inspection_results = [
                    InspectionResult(
                        inspected_url=_string(row.get("inspected_url")),
                        verdict=_string(row.get("verdict")),
                        coverage_state=_string(row.get("coverage_state")),
                        indexing_state=_string(row.get("indexing_state")),
                        last_crawl_time=_string(row.get("last_crawl_time")),
                        page_fetch_state=_string(row.get("page_fetch_state")),
                        robots_txt_state=_string(row.get("robots_txt_state")),
                        google_canonical=_string(row.get("google_canonical")),
                        user_canonical=_string(row.get("user_canonical")),
                        error_message=_string(row.get("error_message")),
                        inspected_at=_string(row.get("inspected_at") or inspection_payload.get("generated_at")),
                        source_type=_string(row.get("source_type")),
                    )
                    for row in inspection_payload.get("results", [])
                    if isinstance(row, dict) and _string(row.get("inspected_url"))
                ]
                return [row.to_dict() for row in enrich_unified_dataset_with_inspection(unified_rows, inspection_results)]

        inspected_csv_path = self.paths.data_processed_dir / "unified_pages_inspected.csv"
        rows = _read_csv_if_exists(inspected_csv_path)
        if rows:
            return rows

        unified_rows = self._load_unified_rows("last_28_days", overwrite=overwrite)
        inspection_payload = _read_json_if_exists(self.paths.data_raw_dir / "gsc_inspection_top_500.json")
        results = []
        for row in (inspection_payload or {}).get("results", []):
            if not isinstance(row, dict):
                continue
            results.append(
                type("InspectionResultProxy", (), row)  # pragma: no cover
            )
        # If the raw inspection file exists but rows are empty/cannot be materialized, return the base rows.
        if not inspection_payload or not inspection_payload.get("results"):
            return unified_rows

        # Materialize using the real model shape expected by enrich_unified_dataset_with_inspection.
        inspection_results = [
            InspectionResult(
                inspected_url=_string(row.get("inspected_url")),
                verdict=_string(row.get("verdict")),
                coverage_state=_string(row.get("coverage_state")),
                indexing_state=_string(row.get("indexing_state")),
                last_crawl_time=_string(row.get("last_crawl_time")),
                page_fetch_state=_string(row.get("page_fetch_state")),
                robots_txt_state=_string(row.get("robots_txt_state")),
                google_canonical=_string(row.get("google_canonical")),
                user_canonical=_string(row.get("user_canonical")),
                error_message=_string(row.get("error_message")),
                inspected_at=_string(row.get("inspected_at") or inspection_payload.get("generated_at")),
                source_type=_string(row.get("source_type")),
            )
            for row in inspection_payload.get("results", [])
            if isinstance(row, dict) and _string(row.get("inspected_url"))
        ]
        return [row.to_dict() for row in enrich_unified_dataset_with_inspection(unified_rows, inspection_results)]

    def _build_kpis(
        self,
        sitewide_trends: dict[str, list[dict[str, Any]]],
        pages_by_window: dict[str, list[dict[str, Any]]],
    ) -> dict[str, dict[str, float]]:
        kpis: dict[str, dict[str, float]] = {}
        for window_name, rows in pages_by_window.items():
            trend_rows = sitewide_trends.get(window_name, [])
            if trend_rows:
                total_clicks = sum(_to_float(row.get("clicks")) for row in trend_rows)
                total_impressions = sum(_to_float(row.get("impressions")) for row in trend_rows)
                avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
                avg_position = (
                    sum(_to_float(row.get("position")) for row in trend_rows) / len(trend_rows)
                    if trend_rows
                    else 0.0
                )
            else:
                total_clicks = sum(_to_float(row.get("gsc_clicks")) for row in rows)
                total_impressions = sum(_to_float(row.get("gsc_impressions")) for row in rows)
                avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
                avg_position = (
                    sum(_to_float(row.get("gsc_position")) for row in rows) / len(rows)
                    if rows
                    else 0.0
                )

            kpis[window_name] = {
                "gsc_clicks": total_clicks,
                "gsc_impressions": total_impressions,
                "avg_ctr": avg_ctr,
                "avg_position": avg_position,
                "ga4_sessions": sum(_to_float(row.get("ga4_sessions")) for row in rows),
                "ga4_conversions": sum(_to_float(row.get("ga4_conversions")) for row in rows),
            }
        return kpis

    def _build_validation(
        self,
        gsc_bundle: dict[str, Any] | None,
        ga4_bundle: dict[str, Any] | None,
        pages_by_window: dict[str, list[dict[str, Any]]],
        top_page_movers: list[PageMoverRow],
        indexing_review: list[IndexingReviewRow],
    ) -> dict[str, Any]:
        missing_sections: list[str] = []
        warnings: list[str] = []
        missing_files: list[str] = []

        for relative_path in ("data/raw/gsc_bundle.json", "data/raw/ga4_bundle.json"):
            if not self.paths.resolve(relative_path).exists():
                missing_files.append(relative_path)

        if not gsc_bundle:
            missing_sections.append("sitewide_trends")
            missing_sections.append("queries")
            warnings.append("GSC bundle is missing. Run fetch-gsc to populate trends, queries, countries, and devices.")
        if not ga4_bundle:
            warnings.append("GA4 bundle is missing. KPI cards may fall back to merged page data.")
        if not any(pages_by_window.values()):
            missing_sections.append("pages")
        if not top_page_movers:
            missing_sections.append("top_page_movers")
            warnings.append("Top page movers require both last_28_days and previous_28_days merged page datasets.")
        if not indexing_review:
            missing_sections.append("indexing_review")
            warnings.append("Indexing review is empty. Run inspect-top-pages after generating queue and unified datasets.")

        return {
            "missing_files": missing_files,
            "missing_sections": missing_sections,
            "warnings": warnings,
            "is_ready": not missing_sections,
        }

    def build_dashboard_data(self, overwrite: bool = True) -> tuple[dict[str, Any], list[str]]:
        active_logger = _default_logger(self.logger)
        date_windows = {
            name: date_range.to_dict()
            for name, date_range in get_date_ranges(self.config).items()
        }

        gsc_bundle = self._load_gsc_bundle()
        ga4_bundle = self._load_ga4_bundle()
        interpretation_service = InterpretationService(self.config, self.paths, active_logger)
        workflow_service = WorkflowService(self.config, self.paths, active_logger)
        pages_by_window = {
            window_name: self._load_unified_rows(window_name, overwrite=overwrite)
            for window_name in ("last_28_days", "previous_28_days", "last_90_days", "last_365_days")
        }
        quick_wins = self._load_or_build_queue_rows(overwrite=overwrite)
        inspected_rows = self._load_or_build_inspected_rows(overwrite=overwrite)
        if inspected_rows:
            pages_by_window["last_28_days"] = inspected_rows

        pages_by_window = {
            window_name: _enrich_scored_rows(interpretation_service.enrich_page_rows(rows))
            for window_name, rows in pages_by_window.items()
        }
        page_attribute_maps = {
            window_name: interpretation_service.attribute_map_by_path(rows)
            for window_name, rows in pages_by_window.items()
        }
        last_28_page_attributes = page_attribute_maps.get("last_28_days", {})
        combined_top_mover_attributes = interpretation_service.attribute_map_by_path(
            [
                *pages_by_window.get("last_28_days", []),
                *pages_by_window.get("previous_28_days", []),
            ]
        )
        quick_wins = interpretation_service.enrich_rows_with_page_attributes(quick_wins, last_28_page_attributes)

        top_page_movers = build_top_page_movers(
            pages_by_window.get("last_28_days", []),
            pages_by_window.get("previous_28_days", []),
            top_n=100,
        )
        indexing_review = build_indexing_review_rows(
            inspected_rows,
            self.config.inspection_scope_prefix,
        )
        top_page_mover_rows = interpretation_service.enrich_rows_with_page_attributes(
            [row.to_dict() for row in top_page_movers],
            combined_top_mover_attributes,
        )
        indexing_review_rows = interpretation_service.enrich_rows_with_page_attributes(
            [row.to_dict() for row in indexing_review],
            last_28_page_attributes,
        )

        pages_by_window = {
            window_name: workflow_service.apply_to_rows(rows, scope="page")
            for window_name, rows in pages_by_window.items()
        }
        quick_wins = workflow_service.apply_to_rows(quick_wins, scope="page")
        top_page_mover_rows = workflow_service.apply_to_rows(top_page_mover_rows, scope="page")
        indexing_review_rows = workflow_service.apply_to_rows(indexing_review_rows, scope="issue")

        top_movers_csv = self.paths.data_processed_dir / "top_page_movers_last_28_vs_previous_28.csv"
        top_movers_json = self.paths.data_processed_dir / "top_page_movers_last_28_vs_previous_28.json"
        indexing_csv = self.paths.data_processed_dir / "indexing_review_last_28_days.csv"
        indexing_json = self.paths.data_processed_dir / "indexing_review_last_28_days.json"

        output_files: list[str] = workflow_service.ensure_state_files()
        written = write_csv_file(top_movers_csv, top_page_mover_rows, TOP_MOVERS_FIELDNAMES, overwrite=overwrite)
        if written:
            output_files.append(str(written))
        written = write_json_file(top_movers_json, {"rows": top_page_mover_rows}, overwrite=overwrite)
        if written:
            output_files.append(str(written))
        written = write_csv_file(indexing_csv, indexing_review_rows, INDEXING_REVIEW_FIELDNAMES, overwrite=overwrite)
        if written:
            output_files.append(str(written))
        written = write_json_file(indexing_json, {"rows": indexing_review_rows}, overwrite=overwrite)
        if written:
            output_files.append(str(written))

        sitewide_trends = (gsc_bundle or {}).get("sitewide_trends", {})
        raw_queries = (gsc_bundle or {}).get("query_reports") or {
            "last_28_days": (gsc_bundle or {}).get("query_report", []),
        }
        queries = {
            window_name: interpretation_service.enrich_query_rows(rows)
            for window_name, rows in raw_queries.items()
            if isinstance(rows, list)
        }
        countries = (gsc_bundle or {}).get("country_reports", {})
        devices = (gsc_bundle or {}).get("device_reports", {})
        kpis = self._build_kpis(sitewide_trends, pages_by_window)

        payload = {
            "metadata": {
                "project_name": self.config.project_name,
                "site_url": self.config.site_url,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "contract_version": "1.3",
                "default_language": self.config.default_language,
                "default_window": "last_28_days",
                "official_dashboard_path": self.config.output_html,
                "official_data_path": self.config.output_data_json,
                "pages_section_mode": "unified_pages",
                "page_windows_with_inspection": ["last_28_days"],
                "interpretation_rules_path": str(self.paths.config_dir / RULES_FILENAME),
                "interpretation_layers": ["brand_split", "query_intent", "page_segment"],
                "workflow_statuses": list(WORKFLOW_STATUSES),
                "workflow_state_paths": {
                    "statuses": "data/state/workflow_statuses.json",
                    "notes": "data/state/notes.json",
                },
                "history_snapshot_dir": str(self.paths.data_history_snapshots_dir),
                "history_latest_dir": str(self.paths.data_history_latest_dir),
            },
            "windows": date_windows,
            "kpis": kpis,
            "sections": {
                "sitewide_trends": sitewide_trends,
                "queries": queries,
                "pages": pages_by_window,
                "top_page_movers": {
                    "last_28_vs_previous_28": top_page_mover_rows,
                },
                "indexing_review": {
                    "last_28_days": indexing_review_rows,
                },
                "quick_wins": {
                    "last_28_days": quick_wins,
                },
                "countries": countries,
                "devices": devices,
                "workflow": {
                    "summary": {
                        "pages_last_28_days": workflow_service.summary_for_rows(pages_by_window.get("last_28_days", [])),
                        "quick_wins_last_28_days": workflow_service.summary_for_rows(quick_wins),
                        "indexing_review_last_28_days": workflow_service.summary_for_rows(indexing_review_rows),
                    },
                },
            },
            "validation": self._build_validation(
                gsc_bundle,
                ga4_bundle,
                pages_by_window,
                top_page_movers,
                indexing_review,
            ),
        }

        history_service = HistoryService(self.config, self.paths, active_logger)
        weekly_delta, history_output_files = history_service.build_weekly_delta(payload, overwrite=overwrite)
        payload["sections"]["weekly_delta"] = weekly_delta
        output_files.extend(history_output_files)

        data_json_path = self.paths.resolve(self.config.output_data_json)
        written_data_json = write_json_file(data_json_path, payload, overwrite=overwrite)
        if written_data_json:
            output_files.append(str(written_data_json))
        return payload, output_files

    def generate(self, overwrite: bool = True) -> dict[str, Any]:
        active_logger = _default_logger(self.logger)
        output_html = self.paths.resolve(self.config.output_html)
        payload, output_files = self.build_dashboard_data(overwrite=overwrite)

        active_logger.info("Canonical dashboard data written for %s file(s).", len(output_files))
        if not output_html.exists():
            active_logger.warning(
                "Official dashboard HTML was not found at %s. "
                "The data contract was generated, but the static dashboard file is missing.",
                output_html,
            )

        validation = payload.get("validation", {})
        if validation.get("missing_sections"):
            active_logger.warning(
                "Dashboard data generated with missing sections: %s",
                ", ".join(validation["missing_sections"]),
            )
        return {
            "html_path": str(output_html),
            "data_path": str(self.paths.resolve(self.config.output_data_json)),
            "output_files": output_files,
            "validation": validation,
            "metadata": payload.get("metadata", {}),
            "weekly_delta": payload.get("sections", {}).get("weekly_delta", {}),
        }

    def status(self) -> str:
        html_output = self.paths.resolve(self.config.output_html)
        data_output = self.paths.resolve(self.config.output_data_json)
        return (
            "Dashboard service is ready. Official entrypoint: "
            f"{html_output}. Canonical data contract: {data_output}."
        )
