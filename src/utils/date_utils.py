from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from ..config import AppConfig
from ..models import DateRange


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_iso() -> str:
    return date.today().isoformat()


def days_ago_iso(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _coerce_today(today: date | datetime | None = None) -> date:
    if isinstance(today, datetime):
        return today.date()
    if isinstance(today, date):
        return today
    return date.today()


def _build_inclusive_date_range(end_date: date, days: int) -> DateRange:
    start_date = end_date - timedelta(days=days - 1)
    return DateRange(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )


def get_date_ranges(
    app_config: AppConfig,
    today: date | datetime | None = None,
) -> dict[str, DateRange]:
    resolved_today = _coerce_today(today)

    last_28_days = _build_inclusive_date_range(
        resolved_today,
        app_config.default_period_days,
    )
    previous_window_end = date.fromisoformat(last_28_days.start_date) - timedelta(days=1)
    previous_28_days = _build_inclusive_date_range(
        previous_window_end,
        app_config.comparison_period_days,
    )
    last_90_days = _build_inclusive_date_range(
        resolved_today,
        app_config.secondary_period_days,
    )
    last_365_days = _build_inclusive_date_range(
        resolved_today,
        app_config.long_term_period_days,
    )

    return {
        "last_28_days": last_28_days,
        "previous_28_days": previous_28_days,
        "last_90_days": last_90_days,
        "last_365_days": last_365_days,
    }
