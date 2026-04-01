from __future__ import annotations

import gzip
import logging
import re
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from ..config import AppConfig
from ..logger import APP_LOGGER_NAME
from ..models import SitemapInventoryRow, SitemapOpportunityReviewRow
from ..paths import ProjectPaths
from ..utils.io_utils import read_csv_file, read_json_file, write_csv_file, write_json_file
from .interpretation_service import InterpretationService

try:  # pragma: no cover - optional dependency at runtime
    import certifi
except Exception:  # pragma: no cover - defensive guard
    certifi = None

USER_AGENT = "seo-dashboard/1.0 (+local sitemap review)"
REQUEST_TIMEOUT_SECONDS = 8
MAX_SITEMAP_DEPTH = 6
MAX_TITLE_FETCHES = 20
MAX_TITLE_BYTES = 200_000
WINDOWS = ("last_28_days", "last_90_days", "last_365_days")
GENERIC_PATH_TOKENS = {
    "blog",
    "page",
    "pages",
    "category",
    "tag",
    "tags",
    "amp",
    "html",
    "php",
    "www",
}
SITEMAP_REVIEW_FIELDNAMES = [
    "url",
    "normalized_page_url",
    "normalized_page_path",
    "source_in_sitemap",
    "source_in_gsc",
    "source_in_ga4",
    "source_in_inspection",
    "page_segment",
    "page_directory_group",
    "title",
    "title_source",
    "basic_fetch_status",
    "basic_fetch_state",
    "sitemap_lastmod",
    "gsc_clicks",
    "gsc_impressions",
    "gsc_position",
    "ga4_sessions",
    "ga4_conversions",
    "inspection_verdict",
    "inspection_coverage_state",
    "inspection_indexing_state",
    "inspection_page_fetch_state",
    "inspection_robots_txt_state",
    "inspection_google_canonical",
    "inspection_user_canonical",
    "indexed_status",
    "crawlable_not_indexed",
    "canonical_mismatch",
    "robots_issue",
    "visibility_status",
    "session_status",
    "thin_content_risk",
    "thin_content_signals",
    "duplicate_slug_cluster_size",
    "slug_potential_score",
    "merge_candidate_url",
    "merge_candidate_path",
    "merge_candidate_similarity",
    "merge_candidate_confidence",
    "stronger_candidate_reason",
    "opportunity_score",
    "opportunity_bucket",
    "recommended_action",
    "recommended_action_text",
]


class SitemapServiceError(Exception):
    """Raised when sitemap inventory or review generation cannot be completed."""


def _default_logger(logger: Any | None = None) -> Any:
    return logger or logging.getLogger(APP_LOGGER_NAME)


def _string(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _normalize_page_url(url: str) -> str:
    raw_url = _string(url)
    if not raw_url:
        return ""
    parsed = urlparse(raw_url)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    normalized = parsed._replace(
        scheme=(parsed.scheme or "https").lower(),
        netloc=parsed.netloc.lower(),
        path=path or "/",
        params="",
        query="",
        fragment="",
    )
    return urlunparse(normalized)


def _normalize_page_path(value: str) -> str:
    raw_value = _string(value)
    if not raw_value:
        return ""
    if raw_value.startswith(("http://", "https://")):
        raw_value = urlparse(raw_value).path or "/"
    if not raw_value.startswith("/"):
        raw_value = f"/{raw_value}"
    if raw_value != "/" and raw_value.endswith("/"):
        raw_value = raw_value.rstrip("/")
    return raw_value or "/"


def _is_same_host(url: str, site_url: str) -> bool:
    parsed_url = urlparse(_string(url))
    parsed_site = urlparse(_string(site_url))
    return bool(parsed_url.netloc and parsed_url.netloc.lower() == parsed_site.netloc.lower())


def _safe_request(url: str) -> Request:
    return Request(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})


def _ssl_context() -> ssl.SSLContext:
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


def _xml_root(bytes_payload: bytes) -> ET.Element:
    raw_payload = bytes_payload
    if raw_payload[:2] == b"\x1f\x8b":
        raw_payload = gzip.decompress(raw_payload)
    return ET.fromstring(raw_payload)


def _tag_name(tag: str) -> str:
    return tag.split("}", 1)[-1].lower()


def _title_from_html(html: str) -> str:
    class _TitleParser(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.in_title = False
            self.parts: list[str] = []

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag.lower() == "title":
                self.in_title = True

        def handle_endtag(self, tag: str) -> None:
            if tag.lower() == "title":
                self.in_title = False

        def handle_data(self, data: str) -> None:
            if self.in_title:
                self.parts.append(data)

    parser = _TitleParser()
    parser.feed(html)
    return " ".join(part.strip() for part in parser.parts if part.strip()).strip()


def _path_tokens(path: str) -> list[str]:
    normalized_path = _normalize_page_path(path)
    tokens = [
        token.lower()
        for token in re.split(r"[^a-zA-Z0-9]+", normalized_path)
        if token and token.lower() not in GENERIC_PATH_TOKENS and not token.isdigit()
    ]
    return [token for token in tokens if len(token) > 1]


def _slug_signature(path: str) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(_path_tokens(path))))


def _jaccard_similarity(left_tokens: Iterable[str], right_tokens: Iterable[str]) -> float:
    left_set = set(left_tokens)
    right_set = set(right_tokens)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _inspection_text(row: dict[str, Any], *fields: str) -> str:
    return " ".join(_string(row.get(field)).lower() for field in fields if _string(row.get(field)))


def _is_robots_issue(row: dict[str, Any]) -> bool:
    robots_state = _inspection_text(row, "inspection_robots_txt_state")
    fetch_state = _inspection_text(row, "inspection_page_fetch_state")
    return any(token in robots_state or token in fetch_state for token in ("block", "disallow", "robots"))


def _is_canonical_mismatch(row: dict[str, Any]) -> bool:
    google_canonical = _normalize_page_url(_string(row.get("inspection_google_canonical")))
    user_canonical = _normalize_page_url(_string(row.get("inspection_user_canonical")))
    return bool(google_canonical and user_canonical and google_canonical != user_canonical)


def _indexed_status(row: dict[str, Any]) -> str:
    coverage = _inspection_text(row, "inspection_coverage_state")
    indexing = _inspection_text(row, "inspection_indexing_state")
    verdict = _inspection_text(row, "inspection_verdict")
    error_message = _inspection_text(row, "inspection_error_message")

    combined = " ".join(part for part in (coverage, indexing, verdict, error_message) if part)
    if not combined:
        return "unknown"
    if "not indexed" in combined:
        return "not_indexed"
    if "excluded" in combined or "noindex" in combined:
        return "excluded"
    if "indexed" in combined or "indexing_allowed" in combined or "submitted and indexed" in combined:
        return "indexed"
    return "unknown"


def _crawlable_not_indexed(row: dict[str, Any]) -> bool:
    fetch_state = _inspection_text(row, "inspection_page_fetch_state")
    return (
        _indexed_status(row) == "not_indexed"
        and not _is_robots_issue(row)
        and any(token in fetch_state for token in ("successful", "success"))
    )


def _visibility_status(gsc_impressions: float) -> str:
    if gsc_impressions <= 0:
        return "zero_visibility"
    if gsc_impressions < 25:
        return "low_visibility"
    return "visible"


def _session_status(ga4_sessions: float) -> str:
    if ga4_sessions <= 0:
        return "zero_sessions"
    if ga4_sessions < 10:
        return "low_sessions"
    return "active_sessions"


def _slug_potential(path: str, row: dict[str, Any]) -> float:
    tokens = _path_tokens(path)
    segment = _string(row.get("page_segment"))
    title = _string(row.get("title"))
    impressions = _to_float(row.get("gsc_impressions"))
    sessions = _to_float(row.get("ga4_sessions"))

    score = 0.0
    if segment in {"blog", "commercial"}:
        score += 2.0
    if 2 <= len(tokens) <= 6:
        score += 2.0
    elif len(tokens) == 1:
        score += 1.0
    if title:
        score += 1.0
    if impressions > 0:
        score += 1.5
    if sessions > 0:
        score += 1.0
    if any(len(token) > 4 for token in tokens):
        score += 1.0
    return min(score, 10.0)


def _thin_content_signals(row: dict[str, Any]) -> tuple[str, list[str]]:
    signals: list[str] = []

    if _to_float(row.get("gsc_impressions")) <= 0:
        signals.append("zero_impressions")
    elif _to_float(row.get("gsc_impressions")) < 25:
        signals.append("low_impressions")

    if _to_float(row.get("ga4_sessions")) <= 0:
        signals.append("zero_sessions")
    elif _to_float(row.get("ga4_sessions")) < 10:
        signals.append("low_sessions")

    if _to_float(row.get("ga4_conversions")) <= 0:
        signals.append("zero_conversions")

    title = _string(row.get("title"))
    if not title:
        signals.append("missing_title")
    elif len(title) < 12:
        signals.append("short_title")

    if _crawlable_not_indexed(row):
        signals.append("crawlable_not_indexed")
    if _is_canonical_mismatch(row):
        signals.append("canonical_mismatch")
    if _is_robots_issue(row):
        signals.append("robots_issue")

    duplicate_slug_cluster_size = int(row.get("duplicate_slug_cluster_size") or 0)
    if duplicate_slug_cluster_size > 1:
        signals.append("duplicate_slug_cluster")

    if len(signals) >= 5 or {"zero_impressions", "zero_sessions", "missing_title"} <= set(signals):
        return "high", signals
    if len(signals) >= 3:
        return "medium", signals
    if signals:
        return "low", signals
    return "low", []


def _opportunity_bucket(score: float) -> str:
    if score >= 70:
        return "high_priority"
    if score >= 40:
        return "review"
    return "low_priority"


def _recommended_action(row: dict[str, Any]) -> tuple[str, str]:
    indexed_status = _string(row.get("indexed_status"))
    thin_risk = _string(row.get("thin_content_risk"))
    merge_candidate = _string(row.get("merge_candidate_path"))
    merge_confidence = _string(row.get("merge_candidate_confidence"))
    slug_potential = _to_float(row.get("slug_potential_score"))
    visibility_status = _string(row.get("visibility_status"))
    session_status = _string(row.get("session_status"))

    if _is_robots_issue(row) or _is_canonical_mismatch(row):
        return (
            "manual_review",
            "Review robots/canonical setup before changing the page or redirecting it.",
        )

    if merge_candidate and merge_confidence in {"high", "medium"} and thin_risk in {"high", "medium"}:
        if slug_potential <= 3 and visibility_status == "zero_visibility" and session_status == "zero_sessions":
            return (
                "redirect_301",
                "The page looks weak and a stronger related URL already exists. Validate intent, then consider a 301 redirect.",
            )
        return (
            "merge_into_stronger_page",
            "A stronger related URL exists. Review whether this page should be merged into that page instead of expanded.",
        )

    if indexed_status in {"not_indexed", "unknown"} and slug_potential >= 5:
        return (
            "expand_content",
            "The slug/topic still looks promising. Improve the page and re-evaluate indexability and content depth.",
        )

    if thin_risk == "high" and slug_potential <= 3:
        return (
            "manual_review",
            "Signals suggest low-value or weak content, but confidence is not high enough for an automatic redirect decision.",
        )

    return (
        "keep_and_monitor",
        "Keep the URL in inventory and monitor indexing, visibility, and engagement before making a stronger action.",
    )


@dataclass(slots=True)
class SitemapService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any | None = None

    @property
    def inventory_json_path(self) -> Path:
        return self.paths.data_raw_dir / "sitemap_inventory.json"

    @property
    def review_json_path(self) -> Path:
        return self.paths.data_processed_dir / "sitemap_opportunity_review.json"

    @property
    def review_csv_path(self) -> Path:
        return self.paths.data_processed_dir / "sitemap_opportunity_review.csv"

    def status(self) -> str:
        return (
            "Sitemap opportunity review service is ready. "
            f"Inventory: {self.inventory_json_path}. Review: {self.review_json_path}."
        )

    def _fetch_url_bytes(self, url: str) -> bytes:
        with urlopen(_safe_request(url), timeout=REQUEST_TIMEOUT_SECONDS, context=_ssl_context()) as response:
            payload = response.read()
        return payload

    def _fetch_title_and_status(self, url: str) -> dict[str, str]:
        try:
            with urlopen(_safe_request(url), timeout=REQUEST_TIMEOUT_SECONDS, context=_ssl_context()) as response:
                status = str(getattr(response, "status", "") or "")
                content_type = response.headers.get("Content-Type", "")
                raw_bytes = response.read(MAX_TITLE_BYTES)
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            return {
                "title": "",
                "title_source": "",
                "basic_fetch_status": "",
                "basic_fetch_state": "fetch_error",
                "fetch_error": str(exc),
            }

        if "text/html" in content_type.lower():
            try:
                html = raw_bytes.decode("utf-8", errors="replace")
            except Exception:  # pragma: no cover - defensive guard
                html = ""
            title = _title_from_html(html)
            return {
                "title": title,
                "title_source": "light_fetch_title" if title else "",
                "basic_fetch_status": status,
                "basic_fetch_state": "fetched",
                "fetch_error": "",
            }

        return {
            "title": "",
            "title_source": "",
            "basic_fetch_status": status,
            "basic_fetch_state": "fetched",
            "fetch_error": "",
        }

    def _parse_sitemap(
        self,
        sitemap_url: str,
        *,
        seen_sitemaps: set[str],
        depth: int,
    ) -> list[dict[str, str]]:
        normalized_sitemap_url = _string(sitemap_url)
        if not normalized_sitemap_url or normalized_sitemap_url in seen_sitemaps or depth > MAX_SITEMAP_DEPTH:
            return []

        seen_sitemaps.add(normalized_sitemap_url)
        root = _xml_root(self._fetch_url_bytes(normalized_sitemap_url))
        root_tag = _tag_name(root.tag)
        entries: list[dict[str, str]] = []

        if root_tag == "sitemapindex":
            for child in root:
                if _tag_name(child.tag) != "sitemap":
                    continue
                loc = ""
                for item in child:
                    if _tag_name(item.tag) == "loc":
                        loc = _string(item.text)
                        break
                if loc:
                    entries.extend(self._parse_sitemap(loc, seen_sitemaps=seen_sitemaps, depth=depth + 1))
            return entries

        if root_tag != "urlset":
            raise SitemapServiceError(f"Unsupported sitemap root tag in {normalized_sitemap_url}: {root.tag}")

        for child in root:
            if _tag_name(child.tag) != "url":
                continue
            loc = ""
            lastmod = ""
            for item in child:
                child_tag = _tag_name(item.tag)
                if child_tag == "loc":
                    loc = _string(item.text)
                elif child_tag == "lastmod":
                    lastmod = _string(item.text)
            if loc:
                entries.append({"loc": loc, "lastmod": lastmod, "source_sitemap_url": normalized_sitemap_url})
        return entries

    def fetch_all_urls(self, sitemap_url: str) -> list[str]:
        return [
            entry["loc"]
            for entry in self._parse_sitemap(sitemap_url, seen_sitemaps=set(), depth=0)
            if _string(entry.get("loc"))
        ]

    def _collect_inventory_from_network(
        self,
        *,
        overwrite: bool,
    ) -> tuple[list[SitemapInventoryRow], list[str], dict[str, Any]]:
        active_logger = _default_logger(self.logger)
        configured_sitemaps = list(self.config.sitemap_urls or ())
        if not configured_sitemaps:
            raise SitemapServiceError("No sitemap_url or sitemap_urls configured.")

        grouped: dict[str, dict[str, Any]] = {}
        output_files: list[str] = []
        warnings: list[str] = []

        for sitemap_url in configured_sitemaps:
            entries = self._parse_sitemap(sitemap_url, seen_sitemaps=set(), depth=0)
            for entry in entries:
                normalized_url = _normalize_page_url(entry.get("loc", ""))
                if not normalized_url or not _is_same_host(normalized_url, self.config.site_url):
                    continue
                normalized_path = _normalize_page_path(normalized_url)
                payload = grouped.setdefault(
                    normalized_url,
                    {
                        "url": normalized_url,
                        "normalized_page_url": normalized_url,
                        "normalized_page_path": normalized_path,
                        "sitemap_source_urls": [],
                        "sitemap_lastmod": "",
                        "title": "",
                        "title_source": "",
                        "basic_fetch_status": "",
                        "basic_fetch_state": "",
                        "fetch_error": "",
                    },
                )
                source_sitemap = _string(entry.get("source_sitemap_url"))
                if source_sitemap and source_sitemap not in payload["sitemap_source_urls"]:
                    payload["sitemap_source_urls"].append(source_sitemap)
                if _string(entry.get("lastmod")) and not payload["sitemap_lastmod"]:
                    payload["sitemap_lastmod"] = _string(entry.get("lastmod"))

        inventory_rows = [
            SitemapInventoryRow(
                url=row["url"],
                normalized_page_url=row["normalized_page_url"],
                normalized_page_path=row["normalized_page_path"],
                sitemap_source_urls=row["sitemap_source_urls"],
                sitemap_lastmod=row["sitemap_lastmod"],
                title=row["title"],
                title_source=row["title_source"],
                basic_fetch_status=row["basic_fetch_status"],
                basic_fetch_state=row["basic_fetch_state"],
                fetch_error=row["fetch_error"],
            )
            for row in sorted(grouped.values(), key=lambda row: row["normalized_page_path"])
        ]

        title_fetches = 0
        for row in inventory_rows:
            if title_fetches >= MAX_TITLE_FETCHES:
                row.basic_fetch_state = row.basic_fetch_state or "not_fetched_limit"
                continue
            fetch_meta = self._fetch_title_and_status(row.url)
            row.title = fetch_meta["title"]
            row.title_source = fetch_meta["title_source"]
            row.basic_fetch_status = fetch_meta["basic_fetch_status"]
            row.basic_fetch_state = fetch_meta["basic_fetch_state"]
            row.fetch_error = fetch_meta["fetch_error"]
            title_fetches += 1
            if row.fetch_error:
                warnings.append(f"title fetch failed for {row.normalized_page_path}: {row.fetch_error}")

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "site_url": self.config.site_url,
            "configured_sitemap_urls": configured_sitemaps,
            "title_fetch_limit": MAX_TITLE_FETCHES,
            "rows": [row.to_dict() for row in inventory_rows],
        }
        written_inventory = write_json_file(self.inventory_json_path, payload, overwrite=overwrite)
        if written_inventory:
            output_files.append(str(written_inventory))

        active_logger.info("Collected %s URL(s) from sitemap inventory.", len(inventory_rows))
        return inventory_rows, output_files, {"warnings": warnings, "configured_sitemaps": configured_sitemaps}

    def _load_inventory_rows(self) -> list[SitemapInventoryRow]:
        payload = read_json_file(self.inventory_json_path)
        rows: list[SitemapInventoryRow] = []
        for raw_row in payload.get("rows", []):
            if not isinstance(raw_row, dict):
                continue
            rows.append(
                SitemapInventoryRow(
                    url=_string(raw_row.get("url")),
                    normalized_page_url=_normalize_page_url(_string(raw_row.get("normalized_page_url") or raw_row.get("url"))),
                    normalized_page_path=_normalize_page_path(_string(raw_row.get("normalized_page_path") or raw_row.get("url"))),
                    sitemap_source_urls=[
                        _string(value)
                        for value in raw_row.get("sitemap_source_urls", [])
                        if _string(value)
                    ],
                    sitemap_lastmod=_string(raw_row.get("sitemap_lastmod")),
                    title=_string(raw_row.get("title")),
                    title_source=_string(raw_row.get("title_source")),
                    basic_fetch_status=_string(raw_row.get("basic_fetch_status")),
                    basic_fetch_state=_string(raw_row.get("basic_fetch_state")),
                    fetch_error=_string(raw_row.get("fetch_error")),
                )
            )
        return rows

    def _load_unified_rows_by_window(self) -> dict[str, list[dict[str, Any]]]:
        rows_by_window: dict[str, list[dict[str, Any]]] = {}
        for window_name in WINDOWS + ("previous_28_days",):
            json_path = self.paths.data_processed_dir / f"unified_pages_{window_name}.json"
            csv_path = self.paths.data_processed_dir / f"unified_pages_{window_name}.csv"
            if json_path.exists():
                payload = read_json_file(json_path)
                candidate_rows = payload.get("rows", [])
                rows_by_window[window_name] = candidate_rows if isinstance(candidate_rows, list) else []
                continue
            if csv_path.exists():
                rows_by_window[window_name] = read_csv_file(csv_path)
                continue
            rows_by_window[window_name] = []
        return rows_by_window

    def _load_inspection_rows(self) -> list[dict[str, Any]]:
        inspection_path = self.paths.data_raw_dir / "gsc_inspection_top_500.json"
        if not inspection_path.exists():
            return []
        payload = read_json_file(inspection_path)
        rows: list[dict[str, Any]] = []
        for raw_row in payload.get("results", []):
            if not isinstance(raw_row, dict):
                continue
            inspected_url = _normalize_page_url(_string(raw_row.get("inspected_url")))
            rows.append(
                {
                    "normalized_page_url": inspected_url,
                    "normalized_page_path": _normalize_page_path(inspected_url),
                    "inspection_verdict": _string(raw_row.get("verdict")),
                    "inspection_coverage_state": _string(raw_row.get("coverage_state")),
                    "inspection_indexing_state": _string(raw_row.get("indexing_state")),
                    "inspection_page_fetch_state": _string(raw_row.get("page_fetch_state")),
                    "inspection_robots_txt_state": _string(raw_row.get("robots_txt_state")),
                    "inspection_google_canonical": _string(raw_row.get("google_canonical")),
                    "inspection_user_canonical": _string(raw_row.get("user_canonical")),
                    "inspection_error_message": _string(raw_row.get("error_message")),
                }
            )
        return rows

    def _union_inventory(
        self,
        sitemap_rows: list[SitemapInventoryRow],
        rows_by_window: dict[str, list[dict[str, Any]]],
        inspection_rows: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        union_rows: dict[str, dict[str, Any]] = {}

        def ensure(url: str, path: str) -> dict[str, Any]:
            normalized_url = _normalize_page_url(url)
            normalized_path = _normalize_page_path(path or normalized_url)
            key = normalized_path or normalized_url
            row = union_rows.setdefault(
                key,
                {
                    "url": normalized_url or "",
                    "normalized_page_url": normalized_url or "",
                    "normalized_page_path": normalized_path,
                    "source_in_sitemap": False,
                    "source_in_gsc": False,
                    "source_in_ga4": False,
                    "source_in_inspection": False,
                    "title": "",
                    "title_source": "",
                    "basic_fetch_status": "",
                    "basic_fetch_state": "",
                    "sitemap_lastmod": "",
                    "inspection_verdict": "",
                    "inspection_coverage_state": "",
                    "inspection_indexing_state": "",
                    "inspection_page_fetch_state": "",
                    "inspection_robots_txt_state": "",
                    "inspection_google_canonical": "",
                    "inspection_user_canonical": "",
                    "inspection_error_message": "",
                },
            )
            if normalized_url and not row["normalized_page_url"]:
                row["normalized_page_url"] = normalized_url
                row["url"] = normalized_url
            return row

        for sitemap_row in sitemap_rows:
            row = ensure(sitemap_row.normalized_page_url or sitemap_row.url, sitemap_row.normalized_page_path)
            row["source_in_sitemap"] = True
            row["title"] = sitemap_row.title
            row["title_source"] = sitemap_row.title_source
            row["basic_fetch_status"] = sitemap_row.basic_fetch_status
            row["basic_fetch_state"] = sitemap_row.basic_fetch_state
            row["sitemap_lastmod"] = sitemap_row.sitemap_lastmod

        for window_rows in rows_by_window.values():
            for raw_row in window_rows:
                normalized_path = _normalize_page_path(_string(raw_row.get("normalized_page_path")))
                normalized_url = _normalize_page_url(_string(raw_row.get("normalized_page_url")))
                if not normalized_path and not normalized_url:
                    continue
                row = ensure(normalized_url, normalized_path)
                if _string(raw_row.get("page_original_gsc")):
                    row["source_in_gsc"] = True
                if _string(raw_row.get("page_original_ga4")):
                    row["source_in_ga4"] = True

        for inspection_row in inspection_rows:
            row = ensure(
                _string(inspection_row.get("normalized_page_url")),
                _string(inspection_row.get("normalized_page_path")),
            )
            row["source_in_inspection"] = True
            for key in (
                "inspection_verdict",
                "inspection_coverage_state",
                "inspection_indexing_state",
                "inspection_page_fetch_state",
                "inspection_robots_txt_state",
                "inspection_google_canonical",
                "inspection_user_canonical",
                "inspection_error_message",
            ):
                row[key] = _string(inspection_row.get(key))

        return union_rows

    def _window_metric_map(
        self,
        rows_by_window: dict[str, list[dict[str, Any]]],
        window_name: str,
    ) -> dict[str, dict[str, Any]]:
        metric_map: dict[str, dict[str, Any]] = {}
        for row in rows_by_window.get(window_name, []):
            path = _normalize_page_path(_string(row.get("normalized_page_path")))
            url = _normalize_page_url(_string(row.get("normalized_page_url")))
            key = path or url
            if not key:
                continue
            metric_map[key] = dict(row)
        return metric_map

    def _merge_candidate_map(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        candidates: dict[str, dict[str, Any]] = {}
        grouped_by_directory: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped_by_directory.setdefault(_string(row.get("page_directory_group")) or "root", []).append(row)

        for group_rows in grouped_by_directory.values():
            for row in group_rows:
                row_tokens = _path_tokens(_string(row.get("normalized_page_path")))
                if len(row_tokens) < 2:
                    continue
                best_candidate: dict[str, Any] | None = None
                best_similarity = 0.0
                for candidate_row in group_rows:
                    if candidate_row is row:
                        continue
                    candidate_tokens = _path_tokens(_string(candidate_row.get("normalized_page_path")))
                    similarity = _jaccard_similarity(row_tokens, candidate_tokens)
                    if similarity < 0.4:
                        continue
                    row_strength = (
                        (_to_float(row.get("gsc_impressions")) * 0.1)
                        + (_to_float(row.get("ga4_sessions")) * 0.2)
                        + (_to_float(row.get("ga4_conversions")) * 4.0)
                        + (10 if _string(row.get("indexed_status")) == "indexed" else 0)
                    )
                    candidate_strength = (
                        (_to_float(candidate_row.get("gsc_impressions")) * 0.1)
                        + (_to_float(candidate_row.get("ga4_sessions")) * 0.2)
                        + (_to_float(candidate_row.get("ga4_conversions")) * 4.0)
                        + (10 if _string(candidate_row.get("indexed_status")) == "indexed" else 0)
                    )
                    if candidate_strength <= row_strength:
                        continue
                    if similarity > best_similarity or (
                        similarity == best_similarity
                        and best_candidate
                        and candidate_strength
                        > (
                            (_to_float(best_candidate.get("gsc_impressions")) * 0.1)
                            + (_to_float(best_candidate.get("ga4_sessions")) * 0.2)
                            + (_to_float(best_candidate.get("ga4_conversions")) * 4.0)
                            + (10 if _string(best_candidate.get("indexed_status")) == "indexed" else 0)
                        )
                    ):
                        best_similarity = similarity
                        best_candidate = candidate_row

                if not best_candidate:
                    continue

                if best_similarity >= 0.8:
                    confidence = "high"
                elif best_similarity >= 0.55:
                    confidence = "medium"
                else:
                    confidence = "low"

                candidates[_string(row.get("normalized_page_path"))] = {
                    "merge_candidate_url": _string(best_candidate.get("normalized_page_url")),
                    "merge_candidate_path": _string(best_candidate.get("normalized_page_path")),
                    "merge_candidate_similarity": best_similarity,
                    "merge_candidate_confidence": confidence,
                    "stronger_candidate_reason": "Higher-signal page with overlapping slug/topic tokens.",
                }
        return candidates

    def _build_window_rows(
        self,
        union_rows: dict[str, dict[str, Any]],
        metric_map: dict[str, dict[str, Any]],
        interpretation_service: InterpretationService,
    ) -> list[dict[str, Any]]:
        raw_rows: list[dict[str, Any]] = []

        for key, base_row in union_rows.items():
            metric_row = metric_map.get(key, {})
            normalized_path = _string(base_row.get("normalized_page_path"))
            normalized_url = _string(base_row.get("normalized_page_url") or base_row.get("url"))
            page_attrs = interpretation_service.classify_page_segment(normalized_url or normalized_path)
            row = {
                "url": _string(base_row.get("url") or normalized_url),
                "normalized_page_url": normalized_url,
                "normalized_page_path": normalized_path,
                "source_in_sitemap": _yes_no(bool(base_row.get("source_in_sitemap"))),
                "source_in_gsc": _yes_no(bool(base_row.get("source_in_gsc"))),
                "source_in_ga4": _yes_no(bool(base_row.get("source_in_ga4"))),
                "source_in_inspection": _yes_no(bool(base_row.get("source_in_inspection"))),
                "page_segment": _string(page_attrs.get("page_segment")) or "other",
                "page_directory_group": _string(page_attrs.get("page_directory_group")) or "root",
                "title": _string(base_row.get("title")),
                "title_source": _string(base_row.get("title_source")),
                "basic_fetch_status": _string(base_row.get("basic_fetch_status")),
                "basic_fetch_state": _string(base_row.get("basic_fetch_state")) or "unavailable",
                "sitemap_lastmod": _string(base_row.get("sitemap_lastmod")),
                "gsc_clicks": _to_float(metric_row.get("gsc_clicks")),
                "gsc_impressions": _to_float(metric_row.get("gsc_impressions")),
                "gsc_position": _to_float(metric_row.get("gsc_position")),
                "ga4_sessions": _to_float(metric_row.get("ga4_sessions")),
                "ga4_conversions": _to_float(metric_row.get("ga4_conversions")),
                "inspection_verdict": _string(base_row.get("inspection_verdict")),
                "inspection_coverage_state": _string(base_row.get("inspection_coverage_state")),
                "inspection_indexing_state": _string(base_row.get("inspection_indexing_state")),
                "inspection_page_fetch_state": _string(base_row.get("inspection_page_fetch_state")),
                "inspection_robots_txt_state": _string(base_row.get("inspection_robots_txt_state")),
                "inspection_google_canonical": _string(base_row.get("inspection_google_canonical")),
                "inspection_user_canonical": _string(base_row.get("inspection_user_canonical")),
            }
            row["indexed_status"] = _indexed_status(row)
            row["crawlable_not_indexed"] = _crawlable_not_indexed(row)
            row["canonical_mismatch"] = _is_canonical_mismatch(row)
            row["robots_issue"] = _is_robots_issue(row)
            row["visibility_status"] = _visibility_status(_to_float(row.get("gsc_impressions")))
            row["session_status"] = _session_status(_to_float(row.get("ga4_sessions")))
            row["slug_potential_score"] = _slug_potential(normalized_path, row)
            raw_rows.append(row)

        signature_counts: dict[tuple[str, ...], int] = {}
        for row in raw_rows:
            signature = _slug_signature(_string(row.get("normalized_page_path")))
            if signature:
                signature_counts[signature] = signature_counts.get(signature, 0) + 1

        for row in raw_rows:
            signature = _slug_signature(_string(row.get("normalized_page_path")))
            row["duplicate_slug_cluster_size"] = signature_counts.get(signature, 0)
            thin_risk, thin_signals = _thin_content_signals(row)
            row["thin_content_risk"] = thin_risk
            row["thin_content_signals"] = thin_signals

        merge_candidate_map = self._merge_candidate_map(raw_rows)
        review_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            merge_meta = merge_candidate_map.get(_string(row.get("normalized_page_path")), {})
            row.update(merge_meta)

            opportunity_score = (
                (_to_float(row.get("slug_potential_score")) * 7.0)
                + (18.0 if row.get("crawlable_not_indexed") else 0.0)
                + (10.0 if _string(row.get("visibility_status")) == "zero_visibility" else 4.0 if _string(row.get("visibility_status")) == "low_visibility" else 0.0)
                + (10.0 if _string(row.get("session_status")) == "zero_sessions" else 4.0 if _string(row.get("session_status")) == "low_sessions" else 0.0)
                + (6.0 if _string(row.get("page_segment")) in {"blog", "commercial"} else 0.0)
                - (12.0 if row.get("robots_issue") else 0.0)
                - (8.0 if row.get("canonical_mismatch") else 0.0)
                + (6.0 if _string(row.get("merge_candidate_confidence")) == "high" else 3.0 if _string(row.get("merge_candidate_confidence")) == "medium" else 0.0)
            )
            row["opportunity_score"] = round(max(opportunity_score, 0.0), 2)
            row["opportunity_bucket"] = _opportunity_bucket(row["opportunity_score"])
            action, action_text = _recommended_action(row)
            row["recommended_action"] = action
            row["recommended_action_text"] = action_text
            review_rows.append(SitemapOpportunityReviewRow(**row).to_dict())

        return sorted(
            review_rows,
            key=lambda review_row: (
                -_to_float(review_row.get("opportunity_score")),
                review_row.get("normalized_page_path", ""),
            ),
        )

    def build_opportunity_review(
        self,
        *,
        overwrite: bool = True,
        allow_network: bool = False,
    ) -> tuple[dict[str, list[dict[str, Any]]], list[str], dict[str, Any]]:
        output_files: list[str] = []
        warnings: list[str] = []

        if allow_network and (overwrite or not self.inventory_json_path.exists()):
            _, inventory_output_files, inventory_meta = self._collect_inventory_from_network(overwrite=overwrite)
            output_files.extend(inventory_output_files)
            warnings.extend(inventory_meta.get("warnings", []))

        if not self.inventory_json_path.exists():
            return {}, output_files, {
                "status": "missing_inventory",
                "warnings": ["Run enrich-with-sitemap to fetch sitemap inventory before building review."],
            }

        sitemap_rows = self._load_inventory_rows()
        rows_by_window = self._load_unified_rows_by_window()
        inspection_rows = self._load_inspection_rows()
        interpretation_service = InterpretationService(self.config, self.paths, _default_logger(self.logger))
        union_rows = self._union_inventory(sitemap_rows, rows_by_window, inspection_rows)

        review_by_window: dict[str, list[dict[str, Any]]] = {}
        for window_name in WINDOWS:
            metric_map = self._window_metric_map(rows_by_window, window_name)
            review_by_window[window_name] = self._build_window_rows(union_rows, metric_map, interpretation_service)

        review_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "site_url": self.config.site_url,
            "inventory_source": str(self.inventory_json_path),
            "windows": review_by_window,
            "warnings": warnings,
        }
        written_json = write_json_file(self.review_json_path, review_payload, overwrite=overwrite)
        if written_json:
            output_files.append(str(written_json))

        written_csv = write_csv_file(
            self.review_csv_path,
            review_by_window.get("last_28_days", []),
            SITEMAP_REVIEW_FIELDNAMES,
            overwrite=overwrite,
        )
        if written_csv:
            output_files.append(str(written_csv))

        return review_by_window, output_files, {
            "status": "ok",
            "inventory_rows": len(sitemap_rows),
            "review_counts": {window: len(rows) for window, rows in review_by_window.items()},
            "warnings": warnings,
        }

    def enrich_data(self, overwrite: bool = True) -> dict[str, Any]:
        inventory_rows, output_files, inventory_meta = self._collect_inventory_from_network(overwrite=overwrite)
        review_by_window, review_output_files, review_meta = self.build_opportunity_review(
            overwrite=overwrite,
            allow_network=False,
        )
        output_files.extend(review_output_files)
        return {
            "status": "ok",
            "inventory_rows": len(inventory_rows),
            "review_counts": {window: len(rows) for window, rows in review_by_window.items()},
            "warnings": [*inventory_meta.get("warnings", []), *review_meta.get("warnings", [])],
            "output_files": output_files,
        }
