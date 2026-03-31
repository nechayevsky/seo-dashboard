from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ValidationResult:
    errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.errors


@dataclass(slots=True)
class InitReport:
    created_directories: list[str] = field(default_factory=list)
    initialized_files: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SmokeTestResult:
    access_ok: bool = False
    error_message: str | None = None


@dataclass(slots=True)
class GSCSmokeTestResult(SmokeTestResult):
    target_site: str = ""
    target_site_found: bool = False
    total_accessible_sites: int = 0
    accessible_sites: list[str] = field(default_factory=list)
    sitemaps_found: int = 0
    sitemaps: list[str] = field(default_factory=list)


@dataclass(slots=True)
class GA4SmokeTestResult(SmokeTestResult):
    property_resource: str = ""
    rows_returned: int = 0
    sample_rows: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class DateRange:
    start_date: str
    end_date: str

    def to_dict(self) -> dict[str, str]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


@dataclass(slots=True)
class GSCRow:
    clicks: float
    impressions: float
    ctr: float
    position: float
    date: str | None = None
    query: str | None = None
    page: str | None = None
    country: str | None = None
    device: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}

        if self.date is not None:
            payload["date"] = self.date
        if self.page is not None:
            payload["page"] = self.page
        if self.query is not None:
            payload["query"] = self.query
        if self.country is not None:
            payload["country"] = self.country
        if self.device is not None:
            payload["device"] = self.device

        payload["clicks"] = self.clicks
        payload["impressions"] = self.impressions
        payload["ctr"] = self.ctr
        payload["position"] = self.position
        return payload


@dataclass(slots=True)
class GSCFetchSummary:
    report_name: str
    window_name: str
    start_date: str
    end_date: str
    row_count: int
    output_path: str | None = None
    skipped_export: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_name": self.report_name,
            "window_name": self.window_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "output_path": self.output_path,
            "skipped_export": self.skipped_export,
        }


@dataclass(slots=True)
class GSCReportBundle:
    site_url: str
    generated_at: str
    date_windows: dict[str, DateRange] = field(default_factory=dict)
    sitewide_trends: dict[str, list[GSCRow]] = field(default_factory=dict)
    query_reports: dict[str, list[GSCRow]] = field(default_factory=dict)
    page_reports: dict[str, list[GSCRow]] = field(default_factory=dict)
    page_query_reports: dict[str, list[GSCRow]] = field(default_factory=dict)
    country_reports: dict[str, list[GSCRow]] = field(default_factory=dict)
    device_reports: dict[str, list[GSCRow]] = field(default_factory=dict)
    query_report: list[GSCRow] = field(default_factory=list)
    page_report: list[GSCRow] = field(default_factory=list)
    page_query_report: list[GSCRow] = field(default_factory=list)
    summaries: dict[str, GSCFetchSummary] = field(default_factory=dict)
    output_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "site_url": self.site_url,
            "generated_at": self.generated_at,
            "date_windows": {
                name: date_range.to_dict()
                for name, date_range in self.date_windows.items()
            },
            "sitewide_trends": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.sitewide_trends.items()
            },
            "query_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.query_reports.items()
            },
            "page_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.page_reports.items()
            },
            "page_query_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.page_query_reports.items()
            },
            "country_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.country_reports.items()
            },
            "device_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.device_reports.items()
            },
            "query_report": [row.to_dict() for row in self.query_report],
            "page_report": [row.to_dict() for row in self.page_report],
            "page_query_report": [row.to_dict() for row in self.page_query_report],
            "summaries": {
                name: summary.to_dict()
                for name, summary in self.summaries.items()
            },
            "output_files": self.output_files,
        }


@dataclass(slots=True)
class GA4Row:
    date_range: str
    landing_page_plus_query_string: str
    landing_page_plus_query_string_original: str
    normalized_page_url: str
    normalized_page_path: str
    sessions: float
    engaged_sessions: float
    conversions: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "date_range": self.date_range,
            "landing_page_plus_query_string": self.landing_page_plus_query_string,
            "landing_page_plus_query_string_original": self.landing_page_plus_query_string_original,
            "normalized_page_url": self.normalized_page_url,
            "normalized_page_path": self.normalized_page_path,
            "sessions": self.sessions,
            "engaged_sessions": self.engaged_sessions,
            "conversions": self.conversions,
        }


@dataclass(slots=True)
class GA4FetchSummary:
    report_name: str
    window_name: str
    start_date: str
    end_date: str
    row_count: int
    output_path: str | None = None
    skipped_export: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_name": self.report_name,
            "window_name": self.window_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "output_path": self.output_path,
            "skipped_export": self.skipped_export,
        }


@dataclass(slots=True)
class GA4ReportBundle:
    property_resource: str
    site_url: str
    generated_at: str
    date_windows: dict[str, DateRange] = field(default_factory=dict)
    landing_page_reports: dict[str, list[GA4Row]] = field(default_factory=dict)
    summaries: dict[str, GA4FetchSummary] = field(default_factory=dict)
    output_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "property_resource": self.property_resource,
            "site_url": self.site_url,
            "generated_at": self.generated_at,
            "date_windows": {
                name: date_range.to_dict()
                for name, date_range in self.date_windows.items()
            },
            "landing_page_reports": {
                name: [row.to_dict() for row in rows]
                for name, rows in self.landing_page_reports.items()
            },
            "summaries": {
                name: summary.to_dict()
                for name, summary in self.summaries.items()
            },
            "output_files": self.output_files,
        }


@dataclass(slots=True)
class UnifiedPageRow:
    page_original_gsc: str
    page_original_ga4: str
    normalized_page_url: str
    normalized_page_path: str
    gsc_clicks: float
    gsc_impressions: float
    gsc_ctr: float
    gsc_position: float
    ga4_sessions: float
    ga4_engaged_sessions: float
    ga4_conversions: float
    data_source_match_type: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "page_original_gsc": self.page_original_gsc,
            "page_original_ga4": self.page_original_ga4,
            "normalized_page_url": self.normalized_page_url,
            "normalized_page_path": self.normalized_page_path,
            "gsc_clicks": self.gsc_clicks,
            "gsc_impressions": self.gsc_impressions,
            "gsc_ctr": self.gsc_ctr,
            "gsc_position": self.gsc_position,
            "ga4_sessions": self.ga4_sessions,
            "ga4_engaged_sessions": self.ga4_engaged_sessions,
            "ga4_conversions": self.ga4_conversions,
            "data_source_match_type": self.data_source_match_type,
        }


@dataclass(slots=True)
class MergeSummary:
    gsc_rows_loaded: int
    ga4_rows_loaded: int
    merged_rows: int
    path_matches: int
    url_matches: int
    gsc_only_rows: int
    ga4_only_rows: int
    output_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "gsc_rows_loaded": self.gsc_rows_loaded,
            "ga4_rows_loaded": self.ga4_rows_loaded,
            "merged_rows": self.merged_rows,
            "path_matches": self.path_matches,
            "url_matches": self.url_matches,
            "gsc_only_rows": self.gsc_only_rows,
            "ga4_only_rows": self.ga4_only_rows,
            "output_files": self.output_files,
        }


@dataclass(slots=True)
class QuickWinQueueRow:
    normalized_page_path: str
    impact_score: float
    effort_score: float
    quick_win_score: float
    reason_code: str
    gsc_impressions: float
    gsc_position: float
    ga4_sessions: float
    ga4_conversions: float
    recommended_action: str
    recommended_action_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_page_path": self.normalized_page_path,
            "impact_score": self.impact_score,
            "effort_score": self.effort_score,
            "quick_win_score": self.quick_win_score,
            "reason_code": self.reason_code,
            "gsc_impressions": self.gsc_impressions,
            "gsc_position": self.gsc_position,
            "ga4_sessions": self.ga4_sessions,
            "ga4_conversions": self.ga4_conversions,
            "recommended_action": self.recommended_action,
            "recommended_action_text": self.recommended_action_text,
        }


@dataclass(slots=True)
class PageScoreRow:
    normalized_page_path: str
    normalized_page_url: str
    page_original_gsc: str
    page_original_ga4: str
    gsc_clicks: float
    gsc_impressions: float
    gsc_ctr: float
    gsc_position: float
    ga4_sessions: float
    ga4_engaged_sessions: float
    ga4_conversions: float
    ga4_bounce_rate_proxy: float
    impact_score: float
    effort_score: float
    quick_win_score: float
    reason_code: str
    recommended_action: str
    recommended_action_text: str
    data_source_match_type: str
    inspection_effort_multiplier: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_page_path": self.normalized_page_path,
            "normalized_page_url": self.normalized_page_url,
            "page_original_gsc": self.page_original_gsc,
            "page_original_ga4": self.page_original_ga4,
            "gsc_clicks": self.gsc_clicks,
            "gsc_impressions": self.gsc_impressions,
            "gsc_ctr": self.gsc_ctr,
            "gsc_position": self.gsc_position,
            "ga4_sessions": self.ga4_sessions,
            "ga4_engaged_sessions": self.ga4_engaged_sessions,
            "ga4_conversions": self.ga4_conversions,
            "ga4_bounce_rate_proxy": self.ga4_bounce_rate_proxy,
            "impact_score": self.impact_score,
            "effort_score": self.effort_score,
            "quick_win_score": self.quick_win_score,
            "reason_code": self.reason_code,
            "recommended_action": self.recommended_action,
            "recommended_action_text": self.recommended_action_text,
            "data_source_match_type": self.data_source_match_type,
            "inspection_effort_multiplier": self.inspection_effort_multiplier,
        }

    def to_queue_row(self) -> QuickWinQueueRow:
        return QuickWinQueueRow(
            normalized_page_path=self.normalized_page_path,
            impact_score=self.impact_score,
            effort_score=self.effort_score,
            quick_win_score=self.quick_win_score,
            reason_code=self.reason_code,
            gsc_impressions=self.gsc_impressions,
            gsc_position=self.gsc_position,
            ga4_sessions=self.ga4_sessions,
            ga4_conversions=self.ga4_conversions,
            recommended_action=self.recommended_action,
            recommended_action_text=self.recommended_action_text,
        )


@dataclass(slots=True)
class ScoringSummary:
    unified_rows_loaded: int
    scored_rows: int
    queue_rows: int
    reason_code_counts: dict[str, int] = field(default_factory=dict)
    output_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "unified_rows_loaded": self.unified_rows_loaded,
            "scored_rows": self.scored_rows,
            "queue_rows": self.queue_rows,
            "reason_code_counts": self.reason_code_counts,
            "output_files": self.output_files,
        }


@dataclass(slots=True)
class InspectionResult:
    inspected_url: str
    verdict: str = ""
    coverage_state: str = ""
    indexing_state: str = ""
    last_crawl_time: str = ""
    page_fetch_state: str = ""
    robots_txt_state: str = ""
    google_canonical: str = ""
    user_canonical: str = ""
    error_message: str = ""
    inspected_at: str = ""
    source_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "inspected_url": self.inspected_url,
            "verdict": self.verdict,
            "coverage_state": self.coverage_state,
            "indexing_state": self.indexing_state,
            "last_crawl_time": self.last_crawl_time,
            "page_fetch_state": self.page_fetch_state,
            "robots_txt_state": self.robots_txt_state,
            "google_canonical": self.google_canonical,
            "user_canonical": self.user_canonical,
            "error_message": self.error_message,
            "inspected_at": self.inspected_at,
            "source_type": self.source_type,
        }


@dataclass(slots=True)
class UnifiedPageWithInspection:
    page_original_gsc: str
    page_original_ga4: str
    normalized_page_url: str
    normalized_page_path: str
    gsc_clicks: float
    gsc_impressions: float
    gsc_ctr: float
    gsc_position: float
    ga4_sessions: float
    ga4_engaged_sessions: float
    ga4_conversions: float
    data_source_match_type: str
    inspection_verdict: str
    inspection_coverage_state: str
    inspection_indexing_state: str
    inspection_last_crawl_time: str
    inspection_page_fetch_state: str
    inspection_robots_txt_state: str
    inspection_google_canonical: str
    inspection_user_canonical: str
    inspection_error_message: str
    ga4_bounce_rate_proxy: float
    inspection_effort_multiplier: float
    impact_score: float
    effort_score: float
    quick_win_score: float
    reason_code: str
    recommended_action: str
    recommended_action_text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "page_original_gsc": self.page_original_gsc,
            "page_original_ga4": self.page_original_ga4,
            "normalized_page_url": self.normalized_page_url,
            "normalized_page_path": self.normalized_page_path,
            "gsc_clicks": self.gsc_clicks,
            "gsc_impressions": self.gsc_impressions,
            "gsc_ctr": self.gsc_ctr,
            "gsc_position": self.gsc_position,
            "ga4_sessions": self.ga4_sessions,
            "ga4_engaged_sessions": self.ga4_engaged_sessions,
            "ga4_conversions": self.ga4_conversions,
            "data_source_match_type": self.data_source_match_type,
            "inspection_verdict": self.inspection_verdict,
            "inspection_coverage_state": self.inspection_coverage_state,
            "inspection_indexing_state": self.inspection_indexing_state,
            "inspection_last_crawl_time": self.inspection_last_crawl_time,
            "inspection_page_fetch_state": self.inspection_page_fetch_state,
            "inspection_robots_txt_state": self.inspection_robots_txt_state,
            "inspection_google_canonical": self.inspection_google_canonical,
            "inspection_user_canonical": self.inspection_user_canonical,
            "inspection_error_message": self.inspection_error_message,
            "ga4_bounce_rate_proxy": self.ga4_bounce_rate_proxy,
            "inspection_effort_multiplier": self.inspection_effort_multiplier,
            "impact_score": self.impact_score,
            "effort_score": self.effort_score,
            "quick_win_score": self.quick_win_score,
            "reason_code": self.reason_code,
            "recommended_action": self.recommended_action,
            "recommended_action_text": self.recommended_action_text,
        }


@dataclass(slots=True)
class InspectionSummary:
    queue_rows_loaded: int
    urls_selected: int
    inspection_results_count: int
    successful_inspections: int
    quota_capped: bool = False
    reused_cached_results: int = 0
    fresh_requests_made: int = 0
    output_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_rows_loaded": self.queue_rows_loaded,
            "urls_selected": self.urls_selected,
            "inspection_results_count": self.inspection_results_count,
            "successful_inspections": self.successful_inspections,
            "quota_capped": self.quota_capped,
            "reused_cached_results": self.reused_cached_results,
            "fresh_requests_made": self.fresh_requests_made,
            "output_files": self.output_files,
        }


@dataclass(slots=True)
class PageMoverRow:
    normalized_page_path: str
    normalized_page_url: str
    current_gsc_clicks: float
    previous_gsc_clicks: float
    current_gsc_impressions: float
    previous_gsc_impressions: float
    current_ga4_sessions: float
    previous_ga4_sessions: float
    current_ga4_conversions: float
    previous_ga4_conversions: float
    click_delta: float
    impression_delta: float
    sessions_delta: float
    conversions_delta: float
    mover_score: float
    movement_direction: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_page_path": self.normalized_page_path,
            "normalized_page_url": self.normalized_page_url,
            "current_gsc_clicks": self.current_gsc_clicks,
            "previous_gsc_clicks": self.previous_gsc_clicks,
            "current_gsc_impressions": self.current_gsc_impressions,
            "previous_gsc_impressions": self.previous_gsc_impressions,
            "current_ga4_sessions": self.current_ga4_sessions,
            "previous_ga4_sessions": self.previous_ga4_sessions,
            "current_ga4_conversions": self.current_ga4_conversions,
            "previous_ga4_conversions": self.previous_ga4_conversions,
            "click_delta": self.click_delta,
            "impression_delta": self.impression_delta,
            "sessions_delta": self.sessions_delta,
            "conversions_delta": self.conversions_delta,
            "mover_score": self.mover_score,
            "movement_direction": self.movement_direction,
        }


@dataclass(slots=True)
class IndexingReviewRow:
    normalized_page_path: str
    normalized_page_url: str
    inspection_verdict: str
    inspection_coverage_state: str
    inspection_indexing_state: str
    inspection_page_fetch_state: str
    inspection_robots_txt_state: str
    inspection_google_canonical: str
    inspection_user_canonical: str
    inspection_last_crawl_time: str
    inspection_error_message: str
    has_issue: bool
    issue_types: list[str] = field(default_factory=list)
    recommended_action: str = ""
    recommended_action_text: str = ""
    source_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_page_path": self.normalized_page_path,
            "normalized_page_url": self.normalized_page_url,
            "inspection_verdict": self.inspection_verdict,
            "inspection_coverage_state": self.inspection_coverage_state,
            "inspection_indexing_state": self.inspection_indexing_state,
            "inspection_page_fetch_state": self.inspection_page_fetch_state,
            "inspection_robots_txt_state": self.inspection_robots_txt_state,
            "inspection_google_canonical": self.inspection_google_canonical,
            "inspection_user_canonical": self.inspection_user_canonical,
            "inspection_last_crawl_time": self.inspection_last_crawl_time,
            "inspection_error_message": self.inspection_error_message,
            "has_issue": self.has_issue,
            "issue_types": self.issue_types,
            "recommended_action": self.recommended_action,
            "recommended_action_text": self.recommended_action_text,
            "source_type": self.source_type,
        }
