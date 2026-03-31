"""Google API client helpers and smoke-test entrypoints."""

from .ga4_client import (
    GA4Client,
    build_ga4_client,
    check_ga4_access,
    fetch_landing_page_report,
    paginate_report,
    run_ga4_smoke_test,
    run_report,
)
from .gsc_client import (
    GSCClient,
    build_date_windows,
    build_gsc_service,
    fetch_page_query_report,
    fetch_page_report,
    fetch_query_report,
    fetch_sitewide_trends,
    list_sitemaps,
    list_sites,
    paginate_search_analytics,
    query_search_analytics,
    run_gsc_smoke_test,
    verify_site_access,
)
from .inspection_client import InspectionClient
from .inspection_client import (
    batch_inspect_urls,
    build_inspection_service,
    inspect_url,
    parse_inspection_result,
)

__all__ = [
    "GA4Client",
    "GSCClient",
    "InspectionClient",
    "build_inspection_service",
    "inspect_url",
    "batch_inspect_urls",
    "parse_inspection_result",
    "build_ga4_client",
    "check_ga4_access",
    "run_report",
    "paginate_report",
    "fetch_landing_page_report",
    "run_ga4_smoke_test",
    "build_date_windows",
    "build_gsc_service",
    "query_search_analytics",
    "paginate_search_analytics",
    "fetch_sitewide_trends",
    "fetch_query_report",
    "fetch_page_report",
    "fetch_page_query_report",
    "list_sites",
    "verify_site_access",
    "list_sitemaps",
    "run_gsc_smoke_test",
]
