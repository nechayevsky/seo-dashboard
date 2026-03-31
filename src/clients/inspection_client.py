from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable

from ..config import AppConfig
from ..logger import APP_LOGGER_NAME
from ..models import InspectionResult

INSPECTION_DAILY_QUOTA = 2_000
INSPECTION_PER_MINUTE_QUOTA = 600
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_DELAY_SECONDS = 10


class InspectionClientError(Exception):
    """Raised when URL Inspection API operations cannot be completed."""


class InspectionQuotaError(InspectionClientError):
    """Raised when URL Inspection API quota is exceeded."""


def _import_inspection_dependencies() -> tuple[Any, Any, Any, Any]:
    try:
        import httplib2
        from google_auth_httplib2 import AuthorizedHttp
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ModuleNotFoundError as exc:
        raise InspectionClientError(
            "Google API client dependencies are not installed. "
            "Run: pip install -r requirements.txt"
        ) from exc

    return build, HttpError, httplib2, AuthorizedHttp


def _format_http_error(exc: Exception) -> str:
    status = getattr(getattr(exc, "resp", None), "status", "unknown")
    content = getattr(exc, "content", b"")
    detail = ""

    if isinstance(content, bytes) and content:
        try:
            payload = json.loads(content.decode("utf-8"))
            detail = payload.get("error", {}).get("message", "")
        except (ValueError, UnicodeDecodeError):
            detail = ""

    return f"HTTP {status}: {detail}".strip().rstrip(":")


def _is_quota_error(exc: Exception) -> bool:
    status = getattr(getattr(exc, "resp", None), "status", None)
    message = _format_http_error(exc).lower()
    return status in {429, 403} and any(
        token in message
        for token in (
            "quota",
            "rate limit",
            "resource exhausted",
            "too many requests",
        )
    )


def build_inspection_service(credentials: Any) -> Any:
    build, _, httplib2, AuthorizedHttp = _import_inspection_dependencies()
    authorized_http = AuthorizedHttp(
        credentials,
        http=httplib2.Http(timeout=30),
    )
    return build(
        "searchconsole",
        "v1",
        http=authorized_http,
        cache_discovery=False,
    )


def inspect_url(service: Any, site_url: str, url: str) -> dict[str, Any]:
    _, HttpError, _, _ = _import_inspection_dependencies()
    request_body = {
        "inspectionUrl": url,
        "siteUrl": site_url,
    }

    try:
        response = service.urlInspection().index().inspect(body=request_body).execute(num_retries=0)
    except HttpError as exc:
        message = _format_http_error(exc)
        if _is_quota_error(exc):
            raise InspectionQuotaError(f"URL Inspection quota limit reached: {message}") from exc
        raise InspectionClientError(f"URL inspection failed for {url}: {message}") from exc
    except Exception as exc:
        raise InspectionClientError(f"URL inspection failed for {url}: {exc}") from exc

    if not isinstance(response, dict):
        return {"_inspected_url": url}

    response["_inspected_url"] = url
    return response


def parse_inspection_result(result: dict[str, Any]) -> InspectionResult:
    inspection_url = str(result.get("_inspected_url", "")).strip()
    inspection_block = result.get("inspectionResult", {})
    if not isinstance(inspection_block, dict):
        inspection_block = {}

    index_status_result = inspection_block.get("indexStatusResult", {})
    if not isinstance(index_status_result, dict):
        index_status_result = {}

    return InspectionResult(
        inspected_url=inspection_url,
        verdict=str(index_status_result.get("verdict", "")).strip(),
        coverage_state=str(index_status_result.get("coverageState", "")).strip(),
        indexing_state=str(index_status_result.get("indexingState", "")).strip(),
        last_crawl_time=str(index_status_result.get("lastCrawlTime", "")).strip(),
        page_fetch_state=str(index_status_result.get("pageFetchState", "")).strip(),
        robots_txt_state=str(index_status_result.get("robotsTxtState", "")).strip(),
        google_canonical=str(index_status_result.get("googleCanonical", "")).strip(),
        user_canonical=str(index_status_result.get("userCanonical", "")).strip(),
    )


def batch_inspect_urls(
    service: Any,
    site_url: str,
    urls: Iterable[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[InspectionResult]:
    logger = logging.getLogger(APP_LOGGER_NAME)
    deduped_urls: list[str] = []
    seen_urls: set[str] = set()

    for raw_url in urls:
        candidate = str(raw_url or "").strip()
        if not candidate or candidate in seen_urls:
            continue
        seen_urls.add(candidate)
        deduped_urls.append(candidate)

    if len(deduped_urls) > INSPECTION_DAILY_QUOTA:
        logger.warning(
            "Requested %s inspection URLs, but daily quota is %s. Truncating the list.",
            len(deduped_urls),
            INSPECTION_DAILY_QUOTA,
        )
        deduped_urls = deduped_urls[:INSPECTION_DAILY_QUOTA]

    results: list[InspectionResult] = []
    effective_batch_size = min(batch_size, DEFAULT_BATCH_SIZE)

    for batch_index, batch_start in enumerate(range(0, len(deduped_urls), effective_batch_size), start=1):
        batch_urls = deduped_urls[batch_start: batch_start + effective_batch_size]
        logger.info(
            "Starting inspection batch %s with %s URL(s).",
            batch_index,
            len(batch_urls),
        )

        for url in batch_urls:
            if len(results) % 10 == 0:
                logger.info(
                    "Inspection progress: %s/%s URL(s) processed.",
                    len(results),
                    len(deduped_urls),
                )
            try:
                raw_result = inspect_url(service, site_url, url)
                results.append(parse_inspection_result(raw_result))
            except InspectionQuotaError as exc:
                logger.error("%s Further URL inspection requests will be stopped.", exc)
                results.append(InspectionResult(inspected_url=url, error_message=str(exc)))
                return results
            except InspectionClientError as exc:
                logger.warning("Inspection skipped for %s: %s", url, exc)
                results.append(InspectionResult(inspected_url=url, error_message=str(exc)))

        more_batches_remaining = batch_start + effective_batch_size < len(deduped_urls)
        if more_batches_remaining:
            logger.info(
                "Inspection batch %s finished. Sleeping for %s seconds to respect %s/min quota.",
                batch_index,
                DEFAULT_BATCH_DELAY_SECONDS,
                INSPECTION_PER_MINUTE_QUOTA,
            )
            time.sleep(DEFAULT_BATCH_DELAY_SECONDS)

    return results


@dataclass(slots=True)
class InspectionClient:
    config: AppConfig

    def status(self) -> str:
        return (
            "URL Inspection client is configured for Search Console URL Inspection API with "
            f"daily quota {INSPECTION_DAILY_QUOTA} and minute quota {INSPECTION_PER_MINUTE_QUOTA}."
        )
