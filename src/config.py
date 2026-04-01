from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import ValidationResult
from .utils.io_utils import read_json_file

SUPPORTED_LANGUAGES = {"ru", "en"}

POSITIVE_INT_FIELDS = (
    "default_period_days",
    "comparison_period_days",
    "secondary_period_days",
    "long_term_period_days",
    "inspection_daily_limit",
    "crawl_frequency_days",
)

PATH_FIELDS = (
    "output_html",
    "output_data_json",
    "log_app_file",
    "log_error_file",
    "google_oauth_credentials_file",
    "google_oauth_token_file",
)


class ConfigError(Exception):
    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


@dataclass(frozen=True, slots=True)
class AppConfig:
    project_name: str
    site_url: str
    sitemap_url: str
    sitemap_urls: tuple[str, ...]
    ga4_property_id: str
    default_language: str
    default_period_days: int
    comparison_period_days: int
    secondary_period_days: int
    long_term_period_days: int
    inspection_daily_limit: int
    crawl_frequency_days: int
    inspection_scope_prefix: str
    output_html: str
    output_data_json: str
    log_app_file: str
    log_error_file: str
    google_oauth_credentials_file: str
    google_oauth_token_file: str
    source_config_path: Path
    project_root: Path

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        source_path: str | Path | None = None,
    ) -> "AppConfig":
        resolved_source_path = (
            Path(source_path).expanduser().resolve()
            if source_path is not None
            else Path.cwd().resolve()
        )
        project_root = (
            resolved_source_path.parent
            if resolved_source_path.suffix
            else resolved_source_path
        )

        return cls(
            project_name=str(data["project_name"]).strip(),
            site_url=str(data["site_url"]).strip(),
            sitemap_url=_primary_sitemap_url(data),
            sitemap_urls=_configured_sitemap_urls(data),
            ga4_property_id=str(data["ga4_property_id"]).strip(),
            default_language=str(data["default_language"]).strip(),
            default_period_days=int(data["default_period_days"]),
            comparison_period_days=int(data["comparison_period_days"]),
            secondary_period_days=int(data["secondary_period_days"]),
            long_term_period_days=int(data["long_term_period_days"]),
            inspection_daily_limit=int(data["inspection_daily_limit"]),
            crawl_frequency_days=int(data["crawl_frequency_days"]),
            inspection_scope_prefix=str(data["inspection_scope_prefix"]).strip(),
            output_html=str(data["output_html"]).strip(),
            output_data_json=str(data["output_data_json"]).strip(),
            log_app_file=str(data["log_app_file"]).strip(),
            log_error_file=str(data["log_error_file"]).strip(),
            google_oauth_credentials_file=str(data["google_oauth_credentials_file"]).strip(),
            google_oauth_token_file=str(data["google_oauth_token_file"]).strip(),
            source_config_path=resolved_source_path,
            project_root=project_root,
        )

    def resolve_path(self, path_value: str | Path) -> Path:
        raw_path = Path(path_value).expanduser()
        return raw_path if raw_path.is_absolute() else self.project_root / raw_path

    @property
    def ga4_property_resource(self) -> str:
        return (
            self.ga4_property_id
            if self.ga4_property_id.startswith("properties/")
            else f"properties/{self.ga4_property_id}"
        )


def _configured_sitemap_urls(data: dict[str, Any]) -> tuple[str, ...]:
    configured: list[str] = []

    sitemap_url = str(data.get("sitemap_url", "") or "").strip()
    if sitemap_url:
        configured.append(sitemap_url)

    raw_sitemap_urls = data.get("sitemap_urls")
    if isinstance(raw_sitemap_urls, list):
        for item in raw_sitemap_urls:
            candidate = str(item or "").strip()
            if candidate:
                configured.append(candidate)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in configured:
        if candidate in seen:
            continue
        deduped.append(candidate)
        seen.add(candidate)

    return tuple(deduped)


def _primary_sitemap_url(data: dict[str, Any]) -> str:
    configured = _configured_sitemap_urls(data)
    if configured:
        return configured[0]
    return str(data.get("sitemap_url", "") or "").strip()


def read_config_data(config_path: str | Path) -> dict[str, Any]:
    resolved_path = Path(config_path).expanduser()

    try:
        return read_json_file(resolved_path)
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {resolved_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON in config file {resolved_path}: {exc}") from exc
    except ValueError as exc:
        raise ConfigError(f"Invalid config content in {resolved_path}: {exc}") from exc


def validate_config_data(data: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    project_name = data.get("project_name")
    if not isinstance(project_name, str) or not project_name.strip():
        errors.append("project_name: must be a non-empty string")

    site_url = data.get("site_url")
    sitemap_url = data.get("sitemap_url")
    sitemap_urls = data.get("sitemap_urls")
    if not isinstance(site_url, str) or not site_url.strip():
        errors.append("site_url: must be a non-empty string")
    elif not (site_url.startswith("http://") or site_url.startswith("https://")):
        errors.append("site_url: must start with http:// or https://")
    elif not site_url.endswith("/"):
        errors.append("site_url: must end with /")

    configured_sitemaps = _configured_sitemap_urls(data)
    if not configured_sitemaps:
        errors.append("sitemap_url or sitemap_urls: must provide at least one sitemap URL")
    else:
        for sitemap_candidate in configured_sitemaps:
            if not (
                sitemap_candidate.startswith("http://")
                or sitemap_candidate.startswith("https://")
            ):
                errors.append(
                    f"sitemap URL must start with http:// or https://: {sitemap_candidate}"
                )

    if sitemap_urls is not None and not isinstance(sitemap_urls, list):
        errors.append("sitemap_urls: must be a list of sitemap URLs when provided")

    ga4_property_id = data.get("ga4_property_id")
    if not isinstance(ga4_property_id, str) or not ga4_property_id.strip():
        errors.append("ga4_property_id: must be a non-empty numeric string")
    elif not ga4_property_id.isdigit():
        errors.append("ga4_property_id: must contain only digits")

    default_language = data.get("default_language")
    if not isinstance(default_language, str) or default_language not in SUPPORTED_LANGUAGES:
        errors.append("default_language: must be ru or en")

    for field_name in POSITIVE_INT_FIELDS:
        value = data.get(field_name)

        if isinstance(value, bool) or not isinstance(value, int):
            errors.append(f"{field_name}: must be a positive integer")
            continue

        if value <= 0:
            errors.append(f"{field_name}: must be a positive integer")

    inspection_scope_prefix = data.get("inspection_scope_prefix")
    if not isinstance(inspection_scope_prefix, str) or not inspection_scope_prefix.strip():
        errors.append("inspection_scope_prefix: must be a non-empty string")
    elif not inspection_scope_prefix.startswith("/"):
        errors.append("inspection_scope_prefix: must start with /")

    for field_name in PATH_FIELDS:
        value = data.get(field_name)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field_name}: must be a non-empty string")

    return ValidationResult(errors=errors)


def load_config(config_path: str | Path) -> AppConfig:
    resolved_path = Path(config_path).expanduser().resolve()
    payload = read_config_data(resolved_path)
    validation_result = validate_config_data(payload)

    if not validation_result.is_valid:
        raise ConfigError("Config validation failed.", errors=validation_result.errors)

    return AppConfig.from_dict(payload, resolved_path)
