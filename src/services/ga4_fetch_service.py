from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..clients.ga4_client import build_ga4_client, fetch_landing_page_report
from ..config import AppConfig
from ..models import DateRange, GA4FetchSummary, GA4ReportBundle, GA4Row
from ..paths import ProjectPaths
from ..services.oauth_service import get_google_credentials_from_config
from ..utils.date_utils import get_date_ranges, utc_now_iso
from ..utils.io_utils import write_csv_file, write_json_file

GA4_LANDING_FIELDNAMES = [
    "date_range",
    "landing_page_plus_query_string",
    "landing_page_plus_query_string_original",
    "normalized_page_url",
    "normalized_page_path",
    "sessions",
    "engaged_sessions",
    "conversions",
]


@dataclass(slots=True)
class GA4FetchService:
    app_config: AppConfig
    project_paths: ProjectPaths
    logger: Any
    _client: Any | None = field(default=None, init=False, repr=False)

    def _get_client(self) -> Any:
        if self._client is None:
            credentials = get_google_credentials_from_config(self.app_config, self.logger)
            self._client = build_ga4_client(credentials)
        return self._client

    def _build_empty_bundle(self, date_windows: dict[str, DateRange]) -> GA4ReportBundle:
        return GA4ReportBundle(
            property_resource=self.app_config.ga4_property_resource,
            site_url=self.app_config.site_url,
            generated_at=utc_now_iso(),
            date_windows=date_windows,
        )

    def _save_rows_to_csv(
        self,
        rows: list[GA4Row],
        relative_path: str,
        overwrite: bool,
    ) -> tuple[str | None, bool]:
        output_path = self.project_paths.resolve(relative_path)

        if output_path.exists() and not overwrite:
            self.logger.warning("Skipping CSV export because file exists: %s", output_path)
            return str(output_path), True

        written_path = write_csv_file(
            output_path,
            [row.to_dict() for row in rows],
            GA4_LANDING_FIELDNAMES,
            overwrite=overwrite,
        )
        return (str(written_path), False) if written_path else (None, False)

    def _save_bundle_json(
        self,
        bundle: GA4ReportBundle,
        relative_path: str,
        overwrite: bool,
    ) -> tuple[str | None, bool]:
        output_path = self.project_paths.resolve(relative_path)

        if output_path.exists() and not overwrite:
            self.logger.warning("Skipping JSON export because file exists: %s", output_path)
            return str(output_path), True

        written_path = write_json_file(output_path, bundle.to_dict(), overwrite=overwrite)
        return (str(written_path), False) if written_path else (None, False)

    def _add_output_file(self, bundle: GA4ReportBundle, output_path: str | None, skipped: bool) -> None:
        if output_path and not skipped and output_path not in bundle.output_files:
            bundle.output_files.append(output_path)

    def _record_summary(
        self,
        bundle: GA4ReportBundle,
        summary_key: str,
        window_name: str,
        date_range: DateRange,
        row_count: int,
        output_path: str | None,
        skipped_export: bool,
    ) -> None:
        bundle.summaries[summary_key] = GA4FetchSummary(
            report_name="landing_page_report",
            window_name=window_name,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            row_count=row_count,
            output_path=output_path,
            skipped_export=skipped_export,
        )

    def _fetch_landing_window(
        self,
        bundle: GA4ReportBundle,
        window_name: str,
        date_range: DateRange,
        relative_path: str,
        overwrite: bool,
    ) -> None:
        client = self._get_client()
        self.logger.info(
            "Fetching GA4 landing page report for %s: %s -> %s",
            window_name,
            date_range.start_date,
            date_range.end_date,
        )

        rows = fetch_landing_page_report(
            client,
            self.app_config.ga4_property_resource,
            self.app_config.site_url,
            date_range,
            window_name,
        )
        bundle.landing_page_reports[window_name] = rows

        output_path, skipped = self._save_rows_to_csv(rows, relative_path, overwrite)
        self._add_output_file(bundle, output_path, skipped)
        self._record_summary(
            bundle=bundle,
            summary_key=f"landing_{window_name}",
            window_name=window_name,
            date_range=date_range,
            row_count=len(rows),
            output_path=output_path,
            skipped_export=skipped,
        )

    def fetch_landing_bundle(self, overwrite: bool = True) -> GA4ReportBundle:
        date_windows = get_date_ranges(self.app_config)
        bundle = self._build_empty_bundle(date_windows)

        self._fetch_landing_window(
            bundle,
            "last_28_days",
            date_windows["last_28_days"],
            "data/raw/ga4_landing_last_28_days.csv",
            overwrite,
        )

        return bundle

    def fetch_bundle(self, overwrite: bool = True) -> GA4ReportBundle:
        date_windows = get_date_ranges(self.app_config)
        bundle = self._build_empty_bundle(date_windows)

        window_to_file = {
            "last_28_days": "data/raw/ga4_landing_last_28_days.csv",
            "previous_28_days": "data/raw/ga4_landing_previous_28_days.csv",
            "last_90_days": "data/raw/ga4_landing_last_90_days.csv",
            "last_365_days": "data/raw/ga4_landing_last_365_days.csv",
        }

        for window_name, relative_path in window_to_file.items():
            self._fetch_landing_window(
                bundle,
                window_name,
                date_windows[window_name],
                relative_path,
                overwrite,
            )

        output_path, skipped = self._save_bundle_json(
            bundle,
            "data/raw/ga4_bundle.json",
            overwrite,
        )
        self._add_output_file(bundle, output_path, skipped)
        bundle.summaries["bundle_json"] = GA4FetchSummary(
            report_name="bundle_json",
            window_name="all",
            start_date="",
            end_date="",
            row_count=0,
            output_path=output_path,
            skipped_export=skipped,
        )
        return bundle
