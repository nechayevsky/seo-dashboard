from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse, urlunparse

from ..config import AppConfig
from ..models import PageScoreRow, QuickWinQueueRow, ScoringSummary
from ..paths import ProjectPaths
from ..utils.io_utils import read_csv_file, write_csv_file, write_json_file

QUEUE_FIELDNAMES = [
    "normalized_page_path",
    "impact_score",
    "effort_score",
    "quick_win_score",
    "reason_code",
    "gsc_impressions",
    "gsc_position",
    "ga4_sessions",
    "ga4_conversions",
    "recommended_action",
    "recommended_action_text",
]

RECOMMENDED_ACTIONS = {
    "noindex_detected": ("request_indexing", "Resolve noindex directives and request indexing after the fix."),
    "canonical_mismatch": ("merge", "Resolve canonical conflicts or consolidate duplicate-intent pages."),
    "robots_txt_blocked": ("request_indexing", "Unblock the URL in robots.txt and request indexing."),
    "not_indexed_crawlable": ("request_indexing", "Investigate indexability and request indexing once fixed."),
    "high_impressions_low_ctr": ("rewrite", "Rewrite title and meta description to improve CTR."),
    "deep_serp_opportunity": ("expand", "Expand the page and strengthen internal links for ranking gains."),
    "traffic_without_conversions": ("rewrite", "Rewrite the page to align traffic intent with conversion goals."),
    "new_page_traffic": ("expand", "Expand supporting content and reinforce internal linking."),
    "underperforming_content": ("expand", "Expand the content to better satisfy search intent."),
    "no_data": ("no_action", "No action until more reliable data is available."),
}


class ScoringServiceError(Exception):
    """Raised when page scoring or queue generation cannot be completed."""


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_path_identifier(row: dict[str, Any]) -> str:
    normalized_page_path = str(row.get("normalized_page_path", "")).strip()
    if normalized_page_path:
        return normalized_page_path

    normalized_page_url = str(row.get("normalized_page_url", "")).strip()
    if normalized_page_url:
        return normalized_page_url

    return str(row.get("page_original_gsc") or row.get("page_original_ga4") or "").strip()


def _calculate_bounce_rate_proxy(row: dict[str, Any]) -> float:
    sessions = _to_float(row.get("ga4_sessions"))
    engaged_sessions = _to_float(row.get("ga4_engaged_sessions"))

    if sessions <= 0:
        return 0.0

    engagement_rate = max(0.0, min(engaged_sessions / sessions, 1.0))
    return max(0.0, min(1.0 - engagement_rate, 1.0))


def _normalize_comparable_url(url: str) -> str:
    raw_url = str(url or "").strip()
    if not raw_url:
        return ""

    parsed = urlparse(raw_url)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    comparable = parsed._replace(
        scheme=(parsed.scheme or "https").lower(),
        netloc=parsed.netloc.lower(),
        path=path or "/",
        params="",
        query="",
        fragment="",
    )
    return urlunparse(comparable)


def _is_robots_blocked(row: dict[str, Any]) -> bool:
    robots_state = str(row.get("inspection_robots_txt_state", "")).strip().lower()
    page_fetch_state = str(row.get("inspection_page_fetch_state", "")).strip().lower()
    return any(
        token in robots_state or token in page_fetch_state
        for token in ("blocked", "disallowed", "robots.txt")
    )


def _is_noindex_detected(row: dict[str, Any]) -> bool:
    inspection_text = " ".join(
        str(row.get(field, "")).strip().lower()
        for field in (
            "inspection_verdict",
            "inspection_coverage_state",
            "inspection_indexing_state",
        )
    )
    return "noindex" in inspection_text


def _is_canonical_mismatch(row: dict[str, Any]) -> bool:
    google_canonical = _normalize_comparable_url(str(row.get("inspection_google_canonical", "")))
    user_canonical = _normalize_comparable_url(str(row.get("inspection_user_canonical", "")))
    return bool(google_canonical and user_canonical and google_canonical != user_canonical)


def _is_not_indexed_crawlable(row: dict[str, Any]) -> bool:
    indexing_state = str(row.get("inspection_indexing_state", "")).strip().lower()
    verdict = str(row.get("inspection_verdict", "")).strip().lower()
    page_fetch_state = str(row.get("inspection_page_fetch_state", "")).strip().lower()
    coverage_state = str(row.get("inspection_coverage_state", "")).strip().lower()

    crawlable = any(token in page_fetch_state for token in ("successful", "success"))
    not_indexed = "not indexed" in indexing_state or (
        verdict in {"fail", "neutral"} and "indexed" not in coverage_state
    )
    return crawlable and not _is_robots_blocked(row) and not_indexed


def calculate_inspection_effort_multiplier(row: dict[str, Any]) -> float:
    multiplier = 1.0

    if _is_noindex_detected(row):
        multiplier = max(multiplier, 2.0)
    if _is_canonical_mismatch(row):
        multiplier = max(multiplier, 1.4)
    if _is_robots_blocked(row):
        multiplier = max(multiplier, 2.5)
    if _is_not_indexed_crawlable(row):
        multiplier = max(multiplier, 1.8)

    return multiplier


def calculate_impact_score(row: dict[str, Any]) -> float:
    gsc_impressions = _to_float(row.get("gsc_impressions"))
    gsc_clicks = _to_float(row.get("gsc_clicks"))
    ga4_sessions = _to_float(row.get("ga4_sessions"))
    ga4_conversions = _to_float(row.get("ga4_conversions"))
    gsc_position = _to_float(row.get("gsc_position"))

    position_component = (1 / gsc_position * 1000) if gsc_position > 0 else 0.0

    return (
        (gsc_impressions * 0.3)
        + (gsc_clicks * 0.2)
        + (ga4_sessions * 0.3)
        + (ga4_conversions * 0.2)
        + position_component
    )


def calculate_effort_score(row: dict[str, Any]) -> float:
    gsc_ctr = _to_float(row.get("gsc_ctr"))
    gsc_position = _to_float(row.get("gsc_position"))
    bounce_rate_proxy = _calculate_bounce_rate_proxy(row)
    inspection_effort_multiplier = calculate_inspection_effort_multiplier(row)

    effort_score = 1.0

    if gsc_ctr < 0.02:
        effort_score += 2.0
    if gsc_position > 20:
        effort_score += 3.0
    if bounce_rate_proxy > 0.70:
        effort_score += 1.0

    return min(effort_score * inspection_effort_multiplier, 10.0)


def calculate_quick_win_score(row: dict[str, Any]) -> float:
    impact_score = calculate_impact_score(row)
    effort_score = calculate_effort_score(row)
    return impact_score / effort_score if effort_score > 0 else 0.0


def assign_reason_code(row: dict[str, Any]) -> str:
    gsc_impressions = _to_float(row.get("gsc_impressions"))
    gsc_clicks = _to_float(row.get("gsc_clicks"))
    gsc_ctr = _to_float(row.get("gsc_ctr"))
    gsc_position = _to_float(row.get("gsc_position"))
    ga4_sessions = _to_float(row.get("ga4_sessions"))
    ga4_conversions = _to_float(row.get("ga4_conversions"))

    if _is_noindex_detected(row):
        return "noindex_detected"
    if _is_canonical_mismatch(row):
        return "canonical_mismatch"
    if _is_robots_blocked(row):
        return "robots_txt_blocked"
    if _is_not_indexed_crawlable(row):
        return "not_indexed_crawlable"
    if gsc_impressions > 1000 and gsc_ctr < 0.02:
        return "high_impressions_low_ctr"
    if gsc_position > 15 and gsc_impressions > 500:
        return "deep_serp_opportunity"
    if ga4_sessions > 100 and ga4_conversions == 0:
        return "traffic_without_conversions"
    if ga4_sessions > 50 and gsc_impressions < 100:
        return "new_page_traffic"
    if gsc_clicks > 10 and gsc_position > 10:
        return "underperforming_content"
    return "no_data"


def score_page_row(row: dict[str, Any]) -> PageScoreRow:
    normalized_page_path = _safe_path_identifier(row)
    impact_score = calculate_impact_score(row)
    effort_score = calculate_effort_score(row)
    quick_win_score = impact_score / effort_score if effort_score > 0 else 0.0
    reason_code = assign_reason_code(row)
    inspection_effort_multiplier = calculate_inspection_effort_multiplier(row)

    return PageScoreRow(
        normalized_page_path=normalized_page_path,
        normalized_page_url=str(row.get("normalized_page_url", "")).strip(),
        page_original_gsc=str(row.get("page_original_gsc", "")).strip(),
        page_original_ga4=str(row.get("page_original_ga4", "")).strip(),
        gsc_clicks=_to_float(row.get("gsc_clicks")),
        gsc_impressions=_to_float(row.get("gsc_impressions")),
        gsc_ctr=_to_float(row.get("gsc_ctr")),
        gsc_position=_to_float(row.get("gsc_position")),
        ga4_sessions=_to_float(row.get("ga4_sessions")),
        ga4_engaged_sessions=_to_float(row.get("ga4_engaged_sessions")),
        ga4_conversions=_to_float(row.get("ga4_conversions")),
        ga4_bounce_rate_proxy=_calculate_bounce_rate_proxy(row),
        impact_score=impact_score,
        effort_score=effort_score,
        quick_win_score=quick_win_score,
        reason_code=reason_code,
        recommended_action=RECOMMENDED_ACTIONS.get(reason_code, RECOMMENDED_ACTIONS["no_data"])[0],
        recommended_action_text=RECOMMENDED_ACTIONS.get(reason_code, RECOMMENDED_ACTIONS["no_data"])[1],
        data_source_match_type=str(row.get("data_source_match_type", "")).strip(),
        inspection_effort_multiplier=inspection_effort_multiplier,
    )


def _score_rows(unified_rows: Iterable[dict[str, Any]]) -> list[PageScoreRow]:
    return [score_page_row(row) for row in unified_rows]


def generate_page_queue(
    unified_df: Iterable[dict[str, Any]],
    top_n: int = 100,
) -> list[QuickWinQueueRow]:
    scored_rows = _score_rows(unified_df)
    return _queue_from_scored_rows(scored_rows, top_n=top_n)


def _queue_from_scored_rows(
    scored_rows: Iterable[PageScoreRow],
    top_n: int = 100,
) -> list[QuickWinQueueRow]:
    ranked_rows = sorted(
        scored_rows,
        key=lambda row: (
            -row.quick_win_score,
            -row.ga4_conversions,
            -row.gsc_impressions,
            row.normalized_page_path,
        ),
    )
    return [row.to_queue_row() for row in ranked_rows[:top_n]]


def load_unified_pages_csv(path: str | Path) -> list[dict[str, Any]]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise ScoringServiceError(f"Unified pages CSV not found: {resolved_path}")
    return read_csv_file(resolved_path)


def build_page_queue_dataset(
    app_config: AppConfig,
    logger: Any,
    window_name: str = "last_28_days",
    overwrite: bool = True,
    top_n: int = 100,
) -> tuple[list[PageScoreRow], list[QuickWinQueueRow], ScoringSummary]:
    unified_csv_path = app_config.resolve_path(f"data/processed/unified_pages_{window_name}.csv")
    if window_name == "last_28_days":
        queue_csv_path = app_config.resolve_path(f"data/processed/page_queue_top_{top_n}.csv")
        queue_json_path = app_config.resolve_path(f"data/processed/page_queue_top_{top_n}.json")
    else:
        queue_csv_path = app_config.resolve_path(f"data/processed/page_queue_{window_name}_top_{top_n}.csv")
        queue_json_path = app_config.resolve_path(f"data/processed/page_queue_{window_name}_top_{top_n}.json")

    logger.info("Loading unified pages dataset from %s", unified_csv_path)
    unified_rows = load_unified_pages_csv(unified_csv_path)
    scored_rows = _score_rows(unified_rows)
    queue_rows = _queue_from_scored_rows(scored_rows, top_n=top_n)

    reason_code_counts = dict(Counter(row.reason_code for row in scored_rows))
    summary = ScoringSummary(
        unified_rows_loaded=len(unified_rows),
        scored_rows=len(scored_rows),
        queue_rows=len(queue_rows),
        reason_code_counts=reason_code_counts,
    )

    output_files: list[str] = []
    if queue_csv_path.exists() and not overwrite:
        logger.warning("Skipping queue CSV export because file exists: %s", queue_csv_path)
    else:
        written_csv = write_csv_file(
            queue_csv_path,
            [row.to_dict() for row in queue_rows],
            QUEUE_FIELDNAMES,
            overwrite=overwrite,
        )
        if written_csv:
            output_files.append(str(written_csv))

    json_will_be_written = not (queue_json_path.exists() and not overwrite)
    prospective_output_files = list(output_files)
    if json_will_be_written:
        prospective_output_files.append(str(queue_json_path))
    summary.output_files = prospective_output_files

    json_payload = {
        "summary": summary.to_dict(),
        "rows": [row.to_dict() for row in queue_rows],
    }
    if queue_json_path.exists() and not overwrite:
        logger.warning("Skipping queue JSON export because file exists: %s", queue_json_path)
    else:
        written_json = write_json_file(
            queue_json_path,
            json_payload,
            overwrite=overwrite,
        )
        if written_json:
            output_files.append(str(written_json))

    summary.output_files = output_files
    logger.info(
        "Page queue built for %s from %s unified rows. Queue size: %s.",
        window_name,
        summary.unified_rows_loaded,
        summary.queue_rows,
    )
    return scored_rows, queue_rows, summary


@dataclass(slots=True)
class ScoringService:
    app_config: AppConfig
    paths: ProjectPaths
    logger: Any

    def status(self) -> str:
        return (
            "Scoring service is prepared to rank unified page data into a quick-win queue "
            f"under {self.paths.data_processed_dir}."
        )

    def build_page_queue(
        self,
        window_name: str = "last_28_days",
        top_n: int = 100,
        overwrite: bool = True,
    ) -> tuple[list[PageScoreRow], list[QuickWinQueueRow], ScoringSummary]:
        return build_page_queue_dataset(
            self.app_config,
            self.logger,
            window_name=window_name,
            overwrite=overwrite,
            top_n=top_n,
        )
