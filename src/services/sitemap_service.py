import csv
import logging
import ssl
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from ..config import AppConfig
from ..logger import APP_LOGGER_NAME
from ..paths import ProjectPaths
from ..utils.io_utils import ensure_parent_dir


def _default_logger(logger: Any | None = None) -> Any:
    return logger or logging.getLogger(APP_LOGGER_NAME)


@dataclass(slots=True)
class SitemapService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any | None = None

    def enrich_data(self, overwrite: bool = True) -> str:
        active_logger = _default_logger(self.logger)
        active_logger.info("Enriching data with sitemap: %s", self.config.sitemap_url)

        try:
            sitemap_urls = self.fetch_all_urls(self.config.sitemap_url)
            active_logger.info("Found %d URLs in sitemap.", len(sitemap_urls))

            merged_data_path = self.paths.data_processed_dir / "unified_pages_last_28_days.csv"
            if not merged_data_path.exists():
                active_logger.error("Merged data not found at %s. Run merge-pages first.", merged_data_path)
                return f"Error: Merged data not found at {merged_data_path}"

            processed_urls = set()
            with open(merged_data_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("normalized_page_url")
                    if url:
                        processed_urls.add(url.rstrip("/"))

            gaps = []
            for url in sitemap_urls:
                if url.rstrip("/") not in processed_urls:
                    gaps.append({"url": url, "reason": "Missing from GSC/GA4"})

            output_path = self.paths.data_processed_dir / "sitemap_gaps.csv"
            ensure_parent_dir(output_path)

            with open(output_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["url", "reason"])
                writer.writeheader()
                writer.writerows(gaps)

            active_logger.info("Identified %d indexing gaps. Saved to %s", len(gaps), output_path)
            return str(output_path)

        except Exception as exc:
            active_logger.error("Sitemap enrichment failed: %s", exc)
            return f"Error: {exc}"

    def fetch_all_urls(self, url: str) -> list[str]:
        req = Request(url, headers={"User-Agent": "SEO-Dashboard-Bot/1.0"})
        # macOS Python often has outdated CA certs, so we use an unverified context for this tool
        context = ssl._create_unverified_context()
        with urlopen(req, context=context) as response:
            content = response.read()

        root = ET.fromstring(content)
        # Handle namespaces
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        urls = []
        # Check if it's a sitemap index
        if root.tag.endswith("sitemapindex"):
            for sitemap in root.findall("ns:sitemap", ns):
                loc = sitemap.find("ns:loc", ns)
                if loc is not None and loc.text:
                    urls.extend(self.fetch_all_urls(loc.text))
        else:
            for url_entry in root.findall("ns:url", ns):
                loc = url_entry.find("ns:loc", ns)
                if loc is not None and loc.text:
                    urls.append(loc.text)

        return list(set(urls))

    def status(self) -> str:
        return (
            f"Sitemap service is ready for {self.config.site_url}. "
            f"Target sitemap: {self.config.sitemap_url}"
        )
