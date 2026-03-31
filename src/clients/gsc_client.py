from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ..logger import APP_LOGGER_NAME
from ..models import DateRange, GSCRow, GSCSmokeTestResult
from ..utils.date_utils import get_date_ranges


class GSCClientError(Exception):
    """Raised when Search Console operations cannot be executed."""


def _import_gsc_dependencies() -> tuple[Any, Any]:
    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ModuleNotFoundError as exc:
        raise GSCClientError(
            "Google API client dependencies are not installed. "
            "Run: pip install -r requirements.txt"
        ) from exc

    return build, HttpError


def _normalize_site_url(site_url: str) -> str:
    if site_url.startswith("sc-domain:"):
        return site_url
    return site_url.rstrip("/") + "/"


def _build_metric_value(raw_row: dict[str, Any], key: str) -> float:
    value = raw_row.get(key, 0.0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_search_analytics_rows(
    raw_rows: list[dict[str, Any]],
    dimensions: list[str],
) -> list[GSCRow]:
    normalized_rows: list[GSCRow] = []

    for raw_row in raw_rows:
        keys = raw_row.get("keys", [])
        dimension_map = {
            dimension: keys[index] if index < len(keys) else None
            for index, dimension in enumerate(dimensions)
        }

        normalized_rows.append(
            GSCRow(
                date=dimension_map.get("date"),
                page=dimension_map.get("page"),
                query=dimension_map.get("query"),
                country=dimension_map.get("country"),
                device=dimension_map.get("device"),
                clicks=_build_metric_value(raw_row, "clicks"),
                impressions=_build_metric_value(raw_row, "impressions"),
                ctr=_build_metric_value(raw_row, "ctr"),
                position=_build_metric_value(raw_row, "position"),
            )
        )

    return normalized_rows


def _format_http_error(exc: Exception) -> str:
    status = getattr(getattr(exc, "resp", None), "status", "unknown")
    content = getattr(exc, "content", b"")
    detail = ""

    if isinstance(content, bytes) and content:
        try:
            payload = json.loads(content.decode("utf-8"))
            error_block = payload.get("error", {})
            detail = error_block.get("message", "")
        except (ValueError, UnicodeDecodeError):
            detail = ""

    return f"HTTP {status}: {detail}".strip().rstrip(":")


def build_gsc_service(credentials: Any) -> Any:
    build, _ = _import_gsc_dependencies()
    return build(
        "searchconsole",
        "v1",
        credentials=credentials,
        cache_discovery=False,
    )


def list_sites(service: Any) -> list[dict[str, str]]:
    response = service.sites().list().execute()
    site_entries = response.get("siteEntry", [])

    return [
        {
            "site_url": entry.get("siteUrl", ""),
            "permission_level": entry.get("permissionLevel", ""),
        }
        for entry in site_entries
        if entry.get("siteUrl")
    ]


def verify_site_access(service: Any, site_url: str) -> bool:
    normalized_target = _normalize_site_url(site_url)
    return any(
        _normalize_site_url(item["site_url"]) == normalized_target
        for item in list_sites(service)
    )


def query_search_analytics(service: Any, site_url: str, request_body: dict[str, Any]) -> dict[str, Any]:
    effective_body = dict(request_body)
    effective_body.setdefault("type", "web")
    response = service.searchanalytics().query(siteUrl=site_url, body=effective_body).execute()
    return response if isinstance(response, dict) else {}


def paginate_search_analytics(
    service: Any,
    site_url: str,
    request_body: dict[str, Any],
    row_limit: int = 25_000,
) -> list[dict[str, Any]]:
    logger = logging.getLogger(APP_LOGGER_NAME)
    all_rows: list[dict[str, Any]] = []
    start_row = 0

    while True:
        paginated_body = dict(request_body)
        paginated_body["rowLimit"] = row_limit
        paginated_body["startRow"] = start_row
        response = query_search_analytics(service, site_url, paginated_body)
        rows = response.get("rows", [])

        if not rows:
            logger.info(
                "GSC pagination finished for dimensions=%s at startRow=%s with no more rows.",
                paginated_body.get("dimensions", []),
                start_row,
            )
            break

        all_rows.extend(rows)
        logger.info(
            "Fetched %s GSC rows for dimensions=%s at startRow=%s.",
            len(rows),
            paginated_body.get("dimensions", []),
            start_row,
        )

        if len(rows) < row_limit:
            break

        start_row += row_limit

    return all_rows


def list_sitemaps(service: Any, site_url: str) -> list[str]:
    response = service.sitemaps().list(siteUrl=site_url).execute()
    sitemap_entries = response.get("sitemap", [])
    return [entry.get("path", "") for entry in sitemap_entries if entry.get("path")]


def fetch_sitewide_trends(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    response = query_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date"],
            "type": "web",
            "rowLimit": 25_000,
            "startRow": 0,
        },
    )
    return _normalize_search_analytics_rows(response.get("rows", []), ["date"])


def fetch_query_report(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    rows = paginate_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["query"],
            "type": "web",
        },
    )
    return _normalize_search_analytics_rows(rows, ["query"])


def fetch_page_report(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    rows = paginate_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["page"],
            "type": "web",
        },
    )
    return _normalize_search_analytics_rows(rows, ["page"])


def fetch_page_query_report(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    rows = paginate_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["page", "query"],
            "type": "web",
        },
    )
    return _normalize_search_analytics_rows(rows, ["page", "query"])


def fetch_country_report(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    rows = paginate_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["country"],
            "type": "web",
        },
    )
    return _normalize_search_analytics_rows(rows, ["country"])


def fetch_device_report(
    service: Any,
    site_url: str,
    start_date: str,
    end_date: str,
) -> list[GSCRow]:
    rows = paginate_search_analytics(
        service,
        site_url,
        {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["device"],
            "type": "web",
        },
    )
    return _normalize_search_analytics_rows(rows, ["device"])


def build_date_windows(
    app_config: Any,
    today: date | datetime | None = None,
) -> dict[str, DateRange]:
    return get_date_ranges(app_config, today=today)


def run_gsc_smoke_test(credentials: Any, site_url: str, logger: Any) -> GSCSmokeTestResult:
    _, HttpError = _import_gsc_dependencies()
    result = GSCSmokeTestResult(target_site=site_url)

    try:
        service = build_gsc_service(credentials)
        accessible_sites = list_sites(service)
        result.total_accessible_sites = len(accessible_sites)
        result.accessible_sites = [item["site_url"] for item in accessible_sites]
        result.target_site_found = any(
            _normalize_site_url(item["site_url"]) == _normalize_site_url(site_url)
            for item in accessible_sites
        )

        if not result.target_site_found:
            result.error_message = (
                f"Configured site was not found among accessible Search Console properties: {site_url}"
            )
            logger.error(result.error_message)
            return result

        sitemaps = list_sitemaps(service, site_url)
        result.sitemaps = sitemaps
        result.sitemaps_found = len(sitemaps)
        result.access_ok = True
        logger.info(
            "Search Console smoke test succeeded for %s with %s sitemap(s).",
            site_url,
            result.sitemaps_found,
        )
        return result
    except HttpError as exc:
        result.error_message = f"Search Console API request failed: {_format_http_error(exc)}"
        logger.error(result.error_message)
        return result
    except Exception as exc:
        result.error_message = f"Search Console smoke test failed: {exc}"
        logger.error(result.error_message)
        return result


@dataclass(slots=True)
class GSCClient:
    credentials: Any

    def build_service(self) -> Any:
        return build_gsc_service(self.credentials)

    def smoke_test(self, site_url: str, logger: Any) -> GSCSmokeTestResult:
        return run_gsc_smoke_test(self.credentials, site_url, logger)

    def fetch_sitewide_trends(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
    ) -> list[GSCRow]:
        return fetch_sitewide_trends(self.build_service(), site_url, start_date, end_date)

    def fetch_query_report(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
    ) -> list[GSCRow]:
        return fetch_query_report(self.build_service(), site_url, start_date, end_date)

    def fetch_page_report(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
    ) -> list[GSCRow]:
        return fetch_page_report(self.build_service(), site_url, start_date, end_date)

    def fetch_page_query_report(
        self,
        site_url: str,
        start_date: str,
        end_date: str,
    ) -> list[GSCRow]:
        return fetch_page_query_report(self.build_service(), site_url, start_date, end_date)
