from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ..clients.gsc_client import (
    build_date_windows,
    build_gsc_service,
    fetch_country_report,
    fetch_device_report,
    fetch_page_query_report,
    fetch_page_report,
    fetch_query_report,
    fetch_sitewide_trends,
)
from ..config import AppConfig
from ..models import DateRange, GSCFetchSummary, GSCReportBundle, GSCRow
from ..paths import ProjectPaths
from ..services.oauth_service import get_google_credentials_from_config
from ..utils.date_utils import utc_now_iso
from ..utils.io_utils import write_csv_file, write_json_file

TREND_FIELDNAMES = ["date", "clicks", "impressions", "ctr", "position"]
QUERY_FIELDNAMES = ["query", "clicks", "impressions", "ctr", "position"]
PAGE_FIELDNAMES = ["page", "clicks", "impressions", "ctr", "position"]
PAGE_QUERY_FIELDNAMES = ["page", "query", "clicks", "impressions", "ctr", "position"]
COUNTRY_FIELDNAMES = ["country", "clicks", "impressions", "ctr", "position"]
DEVICE_FIELDNAMES = ["device", "clicks", "impressions", "ctr", "position"]

WINDOW_TO_SUFFIX = {
    "last_28_days": "last_28_days",
    "previous_28_days": "previous_28_days",
    "last_90_days": "last_90_days",
    "last_365_days": "last_365_days",
}


@dataclass(slots=True)
class GSCFetchService:
    app_config: AppConfig
    project_paths: ProjectPaths
    logger: Any
    _service: Any | None = field(default=None, init=False, repr=False)

    def _get_service(self) -> Any:
        if self._service is None:
            credentials = get_google_credentials_from_config(self.app_config, self.logger)
            self._service = build_gsc_service(credentials)
        return self._service

    def _build_empty_bundle(self, date_windows: dict[str, DateRange]) -> GSCReportBundle:
        return GSCReportBundle(
            site_url=self.app_config.site_url,
            generated_at=utc_now_iso(),
            date_windows=date_windows,
        )

    def _save_rows_to_csv(
        self,
        rows: list[GSCRow],
        relative_path: str,
        fieldnames: list[str],
        overwrite: bool,
    ) -> tuple[str | None, bool]:
        output_path = self.project_paths.resolve(relative_path)

        if output_path.exists() and not overwrite:
            self.logger.warning("Skipping CSV export because file exists: %s", output_path)
            return str(output_path), True

        written_path = write_csv_file(
            output_path,
            [row.to_dict() for row in rows],
            fieldnames=fieldnames,
            overwrite=overwrite,
        )
        return (str(written_path), False) if written_path else (None, False)

    def _save_bundle_json(
        self,
        bundle: GSCReportBundle,
        relative_path: str,
        overwrite: bool,
    ) -> tuple[str | None, bool]:
        output_path = self.project_paths.resolve(relative_path)

        if output_path.exists() and not overwrite:
            self.logger.warning("Skipping JSON export because file exists: %s", output_path)
            return str(output_path), True

        written_path = write_json_file(output_path, bundle.to_dict(), overwrite=overwrite)
        return (str(written_path), False) if written_path else (None, False)

    def _add_output_file(self, bundle: GSCReportBundle, output_path: str | None, skipped: bool) -> None:
        if output_path and not skipped and output_path not in bundle.output_files:
            bundle.output_files.append(output_path)

    def _record_summary(
        self,
        bundle: GSCReportBundle,
        summary_key: str,
        report_name: str,
        window_name: str,
        date_range: DateRange,
        row_count: int,
        output_path: str | None,
        skipped_export: bool,
    ) -> None:
        bundle.summaries[summary_key] = GSCFetchSummary(
            report_name=report_name,
            window_name=window_name,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            row_count=row_count,
            output_path=output_path,
            skipped_export=skipped_export,
        )

    def _windowed_relative_path(self, prefix: str, window_name: str) -> str:
        canonical_prefix = {
            "sitewide": "gsc_sitewide",
            "queries": "gsc_queries",
            "pages": "gsc_pages",
            "countries": "gsc_countries",
            "devices": "gsc_devices",
        }.get(prefix, prefix)
        return f"data/raw/{canonical_prefix}_{WINDOW_TO_SUFFIX[window_name]}.csv"

    def _fetch_windowed_report(
        self,
        bundle: GSCReportBundle,
        report_name: str,
        fieldnames: list[str],
        fetcher: Callable[[Any, str, str, str], list[GSCRow]],
        target_mapping: dict[str, list[GSCRow]],
        target_last_28_attr: str | None,
        overwrite: bool,
    ) -> None:
        service = self._get_service()

        for window_name, date_range in bundle.date_windows.items():
            self.logger.info(
                "Fetching GSC %s for %s: %s -> %s",
                report_name,
                window_name,
                date_range.start_date,
                date_range.end_date,
            )
            rows = fetcher(
                service,
                self.app_config.site_url,
                date_range.start_date,
                date_range.end_date,
            )
            target_mapping[window_name] = rows
            if window_name == "last_28_days" and target_last_28_attr:
                setattr(bundle, target_last_28_attr, rows)

            output_path, skipped = self._save_rows_to_csv(
                rows,
                self._windowed_relative_path(report_name, window_name),
                fieldnames,
                overwrite,
            )
            self._add_output_file(bundle, output_path, skipped)
            self._record_summary(
                bundle=bundle,
                summary_key=f"{report_name}_{window_name}",
                report_name=report_name,
                window_name=window_name,
                date_range=date_range,
                row_count=len(rows),
                output_path=output_path,
                skipped_export=skipped,
            )

    def fetch_trends_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        self._fetch_windowed_report(
            bundle=bundle,
            report_name="sitewide",
            fieldnames=TREND_FIELDNAMES,
            fetcher=fetch_sitewide_trends,
            target_mapping=bundle.sitewide_trends,
            target_last_28_attr=None,
            overwrite=overwrite,
        )
        return bundle

    def fetch_queries_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        self._fetch_windowed_report(
            bundle=bundle,
            report_name="queries",
            fieldnames=QUERY_FIELDNAMES,
            fetcher=fetch_query_report,
            target_mapping=bundle.query_reports,
            target_last_28_attr="query_report",
            overwrite=overwrite,
        )
        return bundle

    def fetch_pages_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        self._fetch_windowed_report(
            bundle=bundle,
            report_name="pages",
            fieldnames=PAGE_FIELDNAMES,
            fetcher=fetch_page_report,
            target_mapping=bundle.page_reports,
            target_last_28_attr="page_report",
            overwrite=overwrite,
        )
        return bundle

    def fetch_country_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        self._fetch_windowed_report(
            bundle=bundle,
            report_name="countries",
            fieldnames=COUNTRY_FIELDNAMES,
            fetcher=fetch_country_report,
            target_mapping=bundle.country_reports,
            target_last_28_attr=None,
            overwrite=overwrite,
        )
        return bundle

    def fetch_device_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        self._fetch_windowed_report(
            bundle=bundle,
            report_name="devices",
            fieldnames=DEVICE_FIELDNAMES,
            fetcher=fetch_device_report,
            target_mapping=bundle.device_reports,
            target_last_28_attr=None,
            overwrite=overwrite,
        )
        return bundle

    def fetch_page_query_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        service = self._get_service()
        date_windows = build_date_windows(self.app_config)
        bundle = self._build_empty_bundle(date_windows)
        date_range = date_windows["last_28_days"]

        self.logger.info(
            "Fetching GSC page_query for last_28_days: %s -> %s",
            date_range.start_date,
            date_range.end_date,
        )
        rows = fetch_page_query_report(
            service,
            self.app_config.site_url,
            date_range.start_date,
            date_range.end_date,
        )
        bundle.page_query_report = rows
        bundle.page_query_reports["last_28_days"] = rows
        output_path, skipped = self._save_rows_to_csv(
            rows,
            "data/raw/gsc_page_query_last_28_days.csv",
            PAGE_QUERY_FIELDNAMES,
            overwrite,
        )
        self._add_output_file(bundle, output_path, skipped)
        self._record_summary(
            bundle=bundle,
            summary_key="page_query_last_28_days",
            report_name="page_query",
            window_name="last_28_days",
            date_range=date_range,
            row_count=len(rows),
            output_path=output_path,
            skipped_export=skipped,
        )
        return bundle

    def fetch_bundle(self, overwrite: bool = True) -> GSCReportBundle:
        trends_bundle = self.fetch_trends_bundle(overwrite=overwrite)
        queries_bundle = self.fetch_queries_bundle(overwrite=overwrite)
        pages_bundle = self.fetch_pages_bundle(overwrite=overwrite)
        countries_bundle = self.fetch_country_bundle(overwrite=overwrite)
        devices_bundle = self.fetch_device_bundle(overwrite=overwrite)
        page_query_bundle = self.fetch_page_query_bundle(overwrite=overwrite)

        bundle = self._build_empty_bundle(trends_bundle.date_windows)
        bundle.sitewide_trends = trends_bundle.sitewide_trends
        bundle.query_reports = queries_bundle.query_reports
        bundle.page_reports = pages_bundle.page_reports
        bundle.country_reports = countries_bundle.country_reports
        bundle.device_reports = devices_bundle.device_reports
        bundle.page_query_reports = page_query_bundle.page_query_reports
        bundle.query_report = queries_bundle.query_report
        bundle.page_report = pages_bundle.page_report
        bundle.page_query_report = page_query_bundle.page_query_report
        bundle.summaries = {
            **trends_bundle.summaries,
            **queries_bundle.summaries,
            **pages_bundle.summaries,
            **countries_bundle.summaries,
            **devices_bundle.summaries,
            **page_query_bundle.summaries,
        }
        bundle.output_files = [
            *trends_bundle.output_files,
            *queries_bundle.output_files,
            *pages_bundle.output_files,
            *countries_bundle.output_files,
            *devices_bundle.output_files,
            *page_query_bundle.output_files,
        ]

        output_path, skipped = self._save_bundle_json(bundle, "data/raw/gsc_bundle.json", overwrite)
        self._add_output_file(bundle, output_path, skipped)
        bundle.summaries["bundle_json"] = GSCFetchSummary(
            report_name="bundle_json",
            window_name="all",
            start_date="",
            end_date="",
            row_count=0,
            output_path=output_path,
            skipped_export=skipped,
        )
        return bundle
