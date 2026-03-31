from __future__ import annotations

from dataclasses import dataclass

from ..config import AppConfig


@dataclass(slots=True)
class CrawlService:
    config: AppConfig

    def status(self) -> str:
        return (
            "Crawl service stub is prepared with a crawl frequency of "
            f"{self.config.crawl_frequency_days} day(s)."
        )
