from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import ParseResult, urlparse, urlunparse

from ..models import DateRange, GA4Row, GA4SmokeTestResult


class GA4ClientError(Exception):
    """Raised when GA4 operations cannot be executed."""


def _import_ga4_dependencies() -> tuple[Any, Any, Any, Any, Any, Any]:
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange as GA4DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        from google.api_core.exceptions import GoogleAPICallError
        from google.api_core.exceptions import NotFound
        from google.api_core.exceptions import PermissionDenied
        from google.api_core.exceptions import ResourceExhausted
    except ModuleNotFoundError as exc:
        raise GA4ClientError(
            "Google Analytics dependencies are not installed. "
            "Run: pip install -r requirements.txt"
        ) from exc

    return (
        BetaAnalyticsDataClient,
        GA4DateRange,
        Dimension,
        Metric,
        RunReportRequest,
        (GoogleAPICallError, NotFound, PermissionDenied, ResourceExhausted),
    )


def _coerce_property_resource(property_id: str) -> str:
    return property_id if property_id.startswith("properties/") else f"properties/{property_id}"


def _to_float(value: str | None) -> float:
    if value is None:
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


def _normalize_landing_page(raw_value: str, site_url: str) -> dict[str, str]:
    original = (raw_value or "").strip()

    if not original:
        return {
            "landing_page_plus_query_string_original": "",
            "landing_page_plus_query_string": "",
            "normalized_page_url": "",
            "normalized_page_path": "",
        }

    if original in {"(not set)", "(other)"}:
        return {
            "landing_page_plus_query_string_original": original,
            "landing_page_plus_query_string": original,
            "normalized_page_url": "",
            "normalized_page_path": "",
        }

    site_parts = urlparse(site_url)
    parsed = urlparse(original)

    if parsed.scheme and parsed.netloc:
        effective = parsed
    else:
        path_part, _, query_part = original.partition("?")
        normalized_path = path_part if path_part.startswith("/") else f"/{path_part}"
        effective = ParseResult(
            scheme=site_parts.scheme,
            netloc=site_parts.netloc,
            path=normalized_path,
            params="",
            query=query_part,
            fragment="",
        )

    normalized_path = _normalize_path(effective.path or "/")
    normalized_with_query = urlunparse(
        (
            effective.scheme,
            effective.netloc,
            normalized_path or "/",
            "",
            effective.query,
            "",
        )
    )
    normalized_without_query = urlunparse(
        (
            effective.scheme,
            effective.netloc,
            normalized_path or "/",
            "",
            "",
            "",
        )
    )

    return {
        "landing_page_plus_query_string_original": original,
        "landing_page_plus_query_string": normalized_with_query,
        "normalized_page_url": normalized_without_query,
        "normalized_page_path": normalized_path or "/",
    }


def _normalize_landing_rows(
    raw_rows: list[dict[str, str]],
    site_url: str,
    window_name: str,
) -> list[GA4Row]:
    normalized_rows: list[GA4Row] = []

    for raw_row in raw_rows:
        landing_raw = raw_row.get("landingPagePlusQueryString", "")
        normalized_page = _normalize_landing_page(landing_raw, site_url)

        normalized_rows.append(
            GA4Row(
                date_range=window_name,
                landing_page_plus_query_string=normalized_page["landing_page_plus_query_string"],
                landing_page_plus_query_string_original=normalized_page[
                    "landing_page_plus_query_string_original"
                ],
                normalized_page_url=normalized_page["normalized_page_url"],
                normalized_page_path=normalized_page["normalized_page_path"],
                sessions=_to_float(raw_row.get("sessions")),
                engaged_sessions=_to_float(raw_row.get("engagedSessions")),
                conversions=_to_float(raw_row.get("conversions")),
            )
        )

    return normalized_rows


def build_ga4_client(credentials: Any) -> Any:
    BetaAnalyticsDataClient, _, _, _, _, _ = _import_ga4_dependencies()
    return BetaAnalyticsDataClient(credentials=credentials)


def run_report(
    client: Any,
    property_id: str,
    date_range: DateRange,
    dimensions: list[str],
    metrics: list[str],
    limit: int = 10_000,
    offset: int = 0,
) -> list[dict[str, str]]:
    _, GA4DateRange, Dimension, Metric, RunReportRequest, handled_errors = (
        _import_ga4_dependencies()
    )
    property_resource = _coerce_property_resource(property_id)

    try:
        request = RunReportRequest(
            property=property_resource,
            date_ranges=[
                GA4DateRange(
                    start_date=date_range.start_date,
                    end_date=date_range.end_date,
                )
            ],
            dimensions=[Dimension(name=name) for name in dimensions],
            metrics=[Metric(name=name) for name in metrics],
            limit=limit,
            offset=offset,
        )
        response = client.run_report(request=request)
    except handled_errors as exc:
        raise GA4ClientError(f"GA4 Data API request failed: {exc}") from exc
    except Exception as exc:
        raise GA4ClientError(f"GA4 report request failed: {exc}") from exc

    if not response.rows:
        return []

    dimension_names = list(dimensions)
    metric_names = list(metrics)
    rows: list[dict[str, str]] = []

    for row in response.rows:
        row_payload: dict[str, str] = {}

        for index, name in enumerate(dimension_names):
            row_payload[name] = (
                row.dimension_values[index].value
                if index < len(row.dimension_values)
                else ""
            )

        for index, name in enumerate(metric_names):
            row_payload[name] = (
                row.metric_values[index].value
                if index < len(row.metric_values)
                else ""
            )

        rows.append(row_payload)

    return rows


def paginate_report(
    client: Any,
    property_id: str,
    date_range: DateRange,
    dimensions: list[str],
    metrics: list[str],
    limit: int = 10_000,
) -> list[dict[str, str]]:
    from ..logger import APP_LOGGER_NAME
    import logging

    logger = logging.getLogger(APP_LOGGER_NAME)
    all_rows: list[dict[str, str]] = []
    offset = 0
    property_resource = _coerce_property_resource(property_id)

    while True:
        rows = run_report(
            client,
            property_resource,
            date_range,
            dimensions,
            metrics,
            limit=limit,
            offset=offset,
        )

        if not rows:
            logger.info(
                "GA4 pagination finished for property=%s dimensions=%s at offset=%s with no more rows.",
                property_resource,
                dimensions,
                offset,
            )
            break

        all_rows.extend(rows)
        logger.info(
            "Fetched %s GA4 rows for property=%s dimensions=%s at offset=%s.",
            len(rows),
            property_resource,
            dimensions,
            offset,
        )

        if len(rows) < limit:
            break

        offset += limit

    return all_rows


def check_ga4_access(client: Any, property_id: str) -> GA4SmokeTestResult:
    property_resource = _coerce_property_resource(property_id)
    result = GA4SmokeTestResult(property_resource=property_resource)

    try:
        rows = run_report(
            client,
            property_resource,
            DateRange(start_date="7daysAgo", end_date="yesterday"),
            dimensions=["date"],
            metrics=["sessions"],
            limit=5,
            offset=0,
        )
        result.rows_returned = len(rows)
        result.sample_rows = rows[:5]
        result.access_ok = True
        return result
    except GA4ClientError as exc:
        result.error_message = str(exc)
        return result
    except Exception as exc:
        result.error_message = f"GA4 smoke test failed: {exc}"
        return result


def fetch_landing_page_report(
    client: Any,
    property_id: str,
    site_url: str,
    date_range: DateRange,
    window_name: str,
    limit: int = 10_000,
) -> list[GA4Row]:
    raw_rows = paginate_report(
        client,
        property_id,
        date_range,
        dimensions=["landingPagePlusQueryString"],
        metrics=["sessions", "engagedSessions", "conversions"],
        limit=limit,
    )
    return _normalize_landing_rows(raw_rows, site_url, window_name)


def run_ga4_smoke_test(credentials: Any, property_id: str, logger: Any) -> GA4SmokeTestResult:
    try:
        client = build_ga4_client(credentials)
        result = check_ga4_access(client, property_id)
    except Exception as exc:
        result = GA4SmokeTestResult(
            property_resource=_coerce_property_resource(property_id),
            error_message=f"GA4 client initialization failed: {exc}",
        )

    if result.access_ok:
        logger.info(
            "GA4 smoke test succeeded for %s with %s row(s).",
            result.property_resource,
            result.rows_returned,
        )
    else:
        logger.error(result.error_message or "GA4 smoke test failed.")

    return result


@dataclass(slots=True)
class GA4Client:
    credentials: Any

    def build_service(self) -> Any:
        return build_ga4_client(self.credentials)

    def smoke_test(self, property_id: str, logger: Any) -> GA4SmokeTestResult:
        return run_ga4_smoke_test(self.credentials, property_id, logger)

    def fetch_landing_page_report(
        self,
        property_id: str,
        site_url: str,
        date_range: DateRange,
        window_name: str,
        limit: int = 10_000,
    ) -> list[GA4Row]:
        return fetch_landing_page_report(
            self.build_service(),
            property_id,
            site_url,
            date_range,
            window_name,
            limit=limit,
        )
