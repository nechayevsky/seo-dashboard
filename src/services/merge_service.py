from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import ParseResult, urlparse, urlunparse

from ..config import AppConfig
from ..models import MergeSummary, UnifiedPageRow
from ..paths import ProjectPaths
from ..utils.io_utils import read_csv_file, write_csv_file, write_json_file

UNIFIED_PAGE_FIELDNAMES = [
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
]

WINDOW_TO_SUFFIX = {
    "last_28_days": "last_28_days",
    "previous_28_days": "previous_28_days",
    "last_90_days": "last_90_days",
    "last_365_days": "last_365_days",
}


class MergeServiceError(Exception):
    """Raised when unified page dataset cannot be built."""


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_path(path: str) -> str:
    if not path:
        return ""
    if path != "/" and path.endswith("/"):
        return path.rstrip("/")
    return path


def _normalize_url_like_value(page_url: str, site_url: str) -> ParseResult:
    site_parts = urlparse(site_url)
    parsed = urlparse((page_url or "").strip())

    if parsed.scheme and parsed.netloc:
        effective = parsed
    else:
        path_part = (page_url or "").strip() or "/"
        if not path_part.startswith("/"):
            path_part = f"/{path_part}"
        effective = ParseResult(
            scheme=site_parts.scheme,
            netloc=site_parts.netloc,
            path=path_part,
            params="",
            query="",
            fragment="",
        )

    normalized_path = _normalize_path(effective.path or "/")
    return ParseResult(
        scheme=effective.scheme or site_parts.scheme,
        netloc=effective.netloc or site_parts.netloc,
        path=normalized_path or "/",
        params="",
        query="",
        fragment="",
    )


def normalize_gsc_page_url(page_url: str, site_url: str) -> str:
    normalized = _normalize_url_like_value(page_url, site_url)
    return urlunparse(
        (
            normalized.scheme,
            normalized.netloc,
            normalized.path,
            "",
            "",
            "",
        )
    )


def normalize_gsc_page_path(page_url: str, site_url: str) -> str:
    normalized = _normalize_url_like_value(page_url, site_url)
    return normalized.path or "/"


def load_gsc_pages_csv(path: str | Path, site_url: str) -> list[dict[str, Any]]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise MergeServiceError(f"GSC pages CSV not found: {resolved_path}")

    rows = read_csv_file(resolved_path)
    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        original_page = (row.get("page") or "").strip()
        normalized_rows.append(
            {
                "page_original_gsc": original_page,
                "normalized_page_url": normalize_gsc_page_url(original_page, site_url),
                "normalized_page_path": normalize_gsc_page_path(original_page, site_url),
                "gsc_clicks": _to_float(row.get("clicks")),
                "gsc_impressions": _to_float(row.get("impressions")),
                "gsc_ctr": _to_float(row.get("ctr")),
                "gsc_position": _to_float(row.get("position")),
            }
        )

    return normalized_rows


def load_ga4_landing_csv(path: str | Path) -> list[dict[str, Any]]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise MergeServiceError(f"GA4 landing CSV not found: {resolved_path}")

    rows = read_csv_file(resolved_path)
    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        normalized_rows.append(
            {
                "page_original_ga4": (row.get("landing_page_plus_query_string_original") or "").strip(),
                "normalized_page_url": (row.get("normalized_page_url") or "").strip(),
                "normalized_page_path": (row.get("normalized_page_path") or "").strip(),
                "ga4_sessions": _to_float(row.get("sessions")),
                "ga4_engaged_sessions": _to_float(row.get("engaged_sessions")),
                "ga4_conversions": _to_float(row.get("conversions")),
            }
        )

    return normalized_rows


def _representative_original(current_value: str, candidate_value: str) -> str:
    if not current_value:
        return candidate_value
    if "?" in current_value and "?" not in candidate_value:
        return candidate_value
    return current_value


def _aggregate_ga4_rows(ga4_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}

    for row in ga4_rows:
        normalized_path = row.get("normalized_page_path", "")
        normalized_url = row.get("normalized_page_url", "")
        aggregate_key = f"path:{normalized_path}" if normalized_path else f"url:{normalized_url}"

        if aggregate_key not in aggregates:
            aggregates[aggregate_key] = {
                "aggregate_key": aggregate_key,
                "page_original_ga4": row.get("page_original_ga4", ""),
                "normalized_page_url": normalized_url,
                "normalized_page_path": normalized_path,
                "ga4_sessions": 0.0,
                "ga4_engaged_sessions": 0.0,
                "ga4_conversions": 0.0,
            }

        aggregate = aggregates[aggregate_key]
        aggregate["page_original_ga4"] = _representative_original(
            aggregate.get("page_original_ga4", ""),
            row.get("page_original_ga4", ""),
        )
        aggregate["ga4_sessions"] += _to_float(row.get("ga4_sessions"))
        aggregate["ga4_engaged_sessions"] += _to_float(row.get("ga4_engaged_sessions"))
        aggregate["ga4_conversions"] += _to_float(row.get("ga4_conversions"))

    return aggregates


def merge_gsc_ga4_pages(
    gsc_rows: list[dict[str, Any]],
    ga4_rows: list[dict[str, Any]],
) -> tuple[list[UnifiedPageRow], MergeSummary]:
    aggregated_ga4 = _aggregate_ga4_rows(ga4_rows)
    ga4_by_path = {
        row["normalized_page_path"]: row
        for row in aggregated_ga4.values()
        if row.get("normalized_page_path")
    }
    ga4_by_url = {
        row["normalized_page_url"]: row
        for row in aggregated_ga4.values()
        if row.get("normalized_page_url")
    }

    matched_ga4_keys: set[str] = set()
    unified_rows: list[UnifiedPageRow] = []
    path_matches = 0
    url_matches = 0
    gsc_only_rows = 0

    for gsc_row in gsc_rows:
        normalized_path = gsc_row.get("normalized_page_path", "")
        normalized_url = gsc_row.get("normalized_page_url", "")
        matched_ga4_row: dict[str, Any] | None = None
        match_type = "gsc_only"

        if normalized_path and normalized_path in ga4_by_path:
            matched_ga4_row = ga4_by_path[normalized_path]
            matched_ga4_keys.add(matched_ga4_row["aggregate_key"])
            path_matches += 1
            match_type = "path_match"
        elif normalized_url and normalized_url in ga4_by_url:
            matched_ga4_row = ga4_by_url[normalized_url]
            matched_ga4_keys.add(matched_ga4_row["aggregate_key"])
            url_matches += 1
            match_type = "url_match"
        else:
            gsc_only_rows += 1

        unified_rows.append(
            UnifiedPageRow(
                page_original_gsc=gsc_row.get("page_original_gsc", ""),
                page_original_ga4=(matched_ga4_row or {}).get("page_original_ga4", ""),
                normalized_page_url=normalized_url or (matched_ga4_row or {}).get("normalized_page_url", ""),
                normalized_page_path=normalized_path or (matched_ga4_row or {}).get("normalized_page_path", ""),
                gsc_clicks=_to_float(gsc_row.get("gsc_clicks")),
                gsc_impressions=_to_float(gsc_row.get("gsc_impressions")),
                gsc_ctr=_to_float(gsc_row.get("gsc_ctr")),
                gsc_position=_to_float(gsc_row.get("gsc_position")),
                ga4_sessions=_to_float((matched_ga4_row or {}).get("ga4_sessions")),
                ga4_engaged_sessions=_to_float((matched_ga4_row or {}).get("ga4_engaged_sessions")),
                ga4_conversions=_to_float((matched_ga4_row or {}).get("ga4_conversions")),
                data_source_match_type=match_type,
            )
        )

    ga4_only_rows = 0
    for ga4_key, ga4_row in aggregated_ga4.items():
        if ga4_key in matched_ga4_keys:
            continue

        ga4_only_rows += 1
        unified_rows.append(
            UnifiedPageRow(
                page_original_gsc="",
                page_original_ga4=ga4_row.get("page_original_ga4", ""),
                normalized_page_url=ga4_row.get("normalized_page_url", ""),
                normalized_page_path=ga4_row.get("normalized_page_path", ""),
                gsc_clicks=0.0,
                gsc_impressions=0.0,
                gsc_ctr=0.0,
                gsc_position=0.0,
                ga4_sessions=_to_float(ga4_row.get("ga4_sessions")),
                ga4_engaged_sessions=_to_float(ga4_row.get("ga4_engaged_sessions")),
                ga4_conversions=_to_float(ga4_row.get("ga4_conversions")),
                data_source_match_type="ga4_only",
            )
        )

    summary = MergeSummary(
        gsc_rows_loaded=len(gsc_rows),
        ga4_rows_loaded=len(ga4_rows),
        merged_rows=len(unified_rows),
        path_matches=path_matches,
        url_matches=url_matches,
        gsc_only_rows=gsc_only_rows,
        ga4_only_rows=ga4_only_rows,
    )

    return unified_rows, summary


def build_unified_pages_dataset(
    app_config: AppConfig,
    logger: Any,
    window_name: str = "last_28_days",
    overwrite: bool = True,
) -> tuple[list[UnifiedPageRow], MergeSummary]:
    if window_name not in WINDOW_TO_SUFFIX:
        raise MergeServiceError(f"Unsupported window_name: {window_name}")

    suffix = WINDOW_TO_SUFFIX[window_name]
    gsc_csv_path = app_config.resolve_path(f"data/raw/gsc_pages_{suffix}.csv")
    ga4_csv_path = app_config.resolve_path(f"data/raw/ga4_landing_{suffix}.csv")
    unified_csv_path = app_config.resolve_path(f"data/processed/unified_pages_{suffix}.csv")
    unified_json_path = app_config.resolve_path(f"data/processed/unified_pages_{suffix}.json")

    logger.info("Loading GSC pages CSV from %s", gsc_csv_path)
    gsc_rows = load_gsc_pages_csv(gsc_csv_path, app_config.site_url)

    logger.info("Loading GA4 landing pages CSV from %s", ga4_csv_path)
    ga4_rows = load_ga4_landing_csv(ga4_csv_path)

    unified_rows, summary = merge_gsc_ga4_pages(gsc_rows, ga4_rows)

    output_files: list[str] = []
    if unified_csv_path.exists() and not overwrite:
        logger.warning("Skipping unified CSV export because file exists: %s", unified_csv_path)
    else:
        written_csv = write_csv_file(
            unified_csv_path,
            [row.to_dict() for row in unified_rows],
            UNIFIED_PAGE_FIELDNAMES,
            overwrite=overwrite,
        )
        if written_csv:
            output_files.append(str(written_csv))

    json_will_be_written = not (unified_json_path.exists() and not overwrite)
    prospective_output_files = list(output_files)
    if json_will_be_written:
        prospective_output_files.append(str(unified_json_path))
    summary.output_files = prospective_output_files

    json_payload = {
        "summary": summary.to_dict(),
        "rows": [row.to_dict() for row in unified_rows],
    }
    if unified_json_path.exists() and not overwrite:
        logger.warning("Skipping unified JSON export because file exists: %s", unified_json_path)
    else:
        written_json = write_json_file(
            unified_json_path,
            json_payload,
            overwrite=overwrite,
        )
        if written_json:
            output_files.append(str(written_json))

    summary.output_files = output_files
    logger.info(
        "Unified pages dataset built for %s with %s merged rows (%s path matches, %s url matches).",
        window_name,
        summary.merged_rows,
        summary.path_matches,
        summary.url_matches,
    )
    return unified_rows, summary


@dataclass(slots=True)
class MergeService:
    app_config: AppConfig
    paths: ProjectPaths
    logger: Any

    def status(self) -> str:
        return (
            "Merge service is prepared to combine GSC page data and GA4 landing pages into "
            f"a unified dataset under {self.paths.data_processed_dir}."
        )

    def build_unified_pages_dataset(
        self,
        window_name: str = "last_28_days",
        overwrite: bool = True,
    ) -> tuple[list[UnifiedPageRow], MergeSummary]:
        return build_unified_pages_dataset(
            self.app_config,
            self.logger,
            window_name=window_name,
            overwrite=overwrite,
        )
