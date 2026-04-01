from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..paths import ProjectPaths
from ..utils.io_utils import read_json_file, write_json_file

WINDOWS = ("last_28_days", "last_90_days", "last_365_days")
DELTA_TOP_N = 10


def _to_float(value: Any) -> float:
    if value in ("", None):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _string(value: Any) -> str:
    return str(value or "").strip()


def _path_key(row: dict[str, Any]) -> str:
    return _string(row.get("normalized_page_path")) or _string(row.get("normalized_page_url"))


def _safe_filename_token(value: str) -> str:
    return (
        value.replace(":", "-")
        .replace("/", "-")
        .replace(" ", "_")
        .replace("+", "Z")
    )


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _iso_week_key(timestamp: str) -> str:
    moment = _parse_iso_datetime(timestamp)
    iso_year, iso_week, _ = moment.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _copy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if isinstance(row, dict)]


def _sort_desc(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (-_to_float(row.get(key)), _path_key(row)))


def _sort_asc(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (_to_float(row.get(key)), _path_key(row)))


def _query_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "available": False,
            "total_rows": 0,
            "total_clicks": 0.0,
            "total_impressions": 0.0,
            "avg_ctr": 0.0,
            "avg_position": 0.0,
            "top_queries": [],
        }

    total_clicks = sum(_to_float(row.get("clicks")) for row in rows)
    total_impressions = sum(_to_float(row.get("impressions")) for row in rows)
    avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
    avg_position = (
        sum(_to_float(row.get("position")) for row in rows) / len(rows)
        if rows
        else 0.0
    )
    top_queries = [
        {
            "query": _string(row.get("query")),
            "clicks": _to_float(row.get("clicks")),
            "impressions": _to_float(row.get("impressions")),
            "ctr": _to_float(row.get("ctr")),
            "position": _to_float(row.get("position")),
        }
        for row in _sort_desc(rows, "clicks")[:10]
        if _string(row.get("query"))
    ]
    return {
        "available": True,
        "total_rows": len(rows),
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "avg_ctr": avg_ctr,
        "avg_position": avg_position,
        "top_queries": top_queries,
    }


def _rows_by_key(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        key: row
        for row in rows
        if isinstance(row, dict) and (key := _path_key(row))
    }


def _issue_set(row: dict[str, Any]) -> set[str]:
    raw = row.get("issue_types") or []
    if isinstance(raw, list):
        return {str(item).strip() for item in raw if str(item).strip()}
    return set()


def _support_message_key(window_name: str, area: str) -> str:
    if area == "quick_wins":
        return "messages.weeklyDeltaQuickWinsRange" if window_name != "last_28_days" else "messages.noDataGenerated"
    if area == "indexing_review":
        return "messages.weeklyDeltaIndexingRange" if window_name != "last_28_days" else "messages.noDataGenerated"
    if area == "pages":
        return "messages.noDataGenerated"
    if area == "queries":
        return "messages.weeklyDeltaQueriesRange"
    return "messages.noDataGenerated"


def _page_delta_rows(
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    current_by_key = _rows_by_key(current_rows)
    previous_by_key = _rows_by_key(previous_rows)
    all_keys = set(current_by_key) | set(previous_by_key)
    delta_rows: list[dict[str, Any]] = []

    for key in all_keys:
        current = current_by_key.get(key, {})
        previous = previous_by_key.get(key, {})

        current_position = _to_float(current.get("gsc_position"))
        previous_position = _to_float(previous.get("gsc_position"))
        position_gain = previous_position - current_position if previous_position and current_position else 0.0

        quick_win_score_delta = _to_float(current.get("quick_win_score")) - _to_float(previous.get("quick_win_score"))
        click_delta = _to_float(current.get("gsc_clicks")) - _to_float(previous.get("gsc_clicks"))
        impression_delta = _to_float(current.get("gsc_impressions")) - _to_float(previous.get("gsc_impressions"))
        sessions_delta = _to_float(current.get("ga4_sessions")) - _to_float(previous.get("ga4_sessions"))
        conversions_delta = _to_float(current.get("ga4_conversions")) - _to_float(previous.get("ga4_conversions"))

        # Keep the weekly movement logic transparent: score change is mostly driven by score delta,
        # then supported by traffic/conversion deltas and SERP position improvement.
        delta_score = (
            quick_win_score_delta
            + (click_delta * 0.75)
            + (impression_delta * 0.05)
            + (sessions_delta * 0.5)
            + (conversions_delta * 25.0)
            + (position_gain * 10.0)
        )

        current_reason_code = _string(current.get("reason_code"))
        previous_reason_code = _string(previous.get("reason_code"))
        delta_rows.append(
            {
                "normalized_page_path": _string(current.get("normalized_page_path") or previous.get("normalized_page_path")),
                "normalized_page_url": _string(current.get("normalized_page_url") or previous.get("normalized_page_url")),
                "current_quick_win_score": _to_float(current.get("quick_win_score")),
                "previous_quick_win_score": _to_float(previous.get("quick_win_score")),
                "quick_win_score_delta": quick_win_score_delta,
                "current_reason_code": current_reason_code,
                "previous_reason_code": previous_reason_code,
                "reason_code_changed": bool(current_reason_code and previous_reason_code and current_reason_code != previous_reason_code),
                "current_recommended_action": _string(current.get("recommended_action")),
                "previous_recommended_action": _string(previous.get("recommended_action")),
                "current_gsc_clicks": _to_float(current.get("gsc_clicks")),
                "previous_gsc_clicks": _to_float(previous.get("gsc_clicks")),
                "click_delta": click_delta,
                "current_gsc_impressions": _to_float(current.get("gsc_impressions")),
                "previous_gsc_impressions": _to_float(previous.get("gsc_impressions")),
                "impression_delta": impression_delta,
                "current_ga4_sessions": _to_float(current.get("ga4_sessions")),
                "previous_ga4_sessions": _to_float(previous.get("ga4_sessions")),
                "sessions_delta": sessions_delta,
                "current_ga4_conversions": _to_float(current.get("ga4_conversions")),
                "previous_ga4_conversions": _to_float(previous.get("ga4_conversions")),
                "conversions_delta": conversions_delta,
                "current_gsc_position": current_position,
                "previous_gsc_position": previous_position,
                "position_gain": position_gain,
                "delta_score": delta_score,
                "movement_direction": "up" if delta_score > 0 else "down" if delta_score < 0 else "flat",
            }
        )

    return delta_rows


def _delta_panel(rows: list[dict[str, Any]], supported: bool, message_key: str) -> dict[str, Any]:
    return {
        "supported": supported,
        "message_key": message_key,
        "rows": rows,
        "count": len(rows),
    }


@dataclass(slots=True)
class HistoryService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any

    def _snapshot_file_path(self, generated_at: str, week_key: str) -> Path:
        base_name = f"{_safe_filename_token(generated_at)}_{week_key}"
        candidate = self.paths.data_history_snapshots_dir / f"{base_name}.json"
        suffix = 1

        while candidate.exists():
            candidate = self.paths.data_history_snapshots_dir / f"{base_name}_{suffix}.json"
            suffix += 1

        return candidate

    def _list_snapshot_paths(self) -> list[Path]:
        return sorted(
            self.paths.data_history_snapshots_dir.glob("*.json"),
            key=lambda path: path.name,
        )

    def _load_snapshots(self) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        for path in self._list_snapshot_paths():
            try:
                payload = read_json_file(path)
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Skipping unreadable history snapshot %s: %s", path, exc)
                continue

            payload["_snapshot_path"] = str(path)
            snapshots.append(payload)

        snapshots.sort(key=lambda item: _string(item.get("generated_at")))
        return snapshots

    def _build_snapshot(self, dashboard_payload: dict[str, Any]) -> dict[str, Any]:
        generated_at = _string(dashboard_payload.get("metadata", {}).get("generated_at")) or datetime.now(UTC).isoformat()
        sections = dashboard_payload.get("sections", {})
        queries = sections.get("queries", {})
        snapshot = {
            "snapshot_id": _safe_filename_token(generated_at),
            "generated_at": generated_at,
            "week_key": _iso_week_key(generated_at),
            "metadata": {
                "project_name": _string(dashboard_payload.get("metadata", {}).get("project_name")),
                "site_url": _string(dashboard_payload.get("metadata", {}).get("site_url")),
                "contract_version": _string(dashboard_payload.get("metadata", {}).get("contract_version")),
            },
            "windows": dict(dashboard_payload.get("windows", {})),
            "sitewide_summary": {
                window_name: dict(dashboard_payload.get("kpis", {}).get(window_name, {}))
                for window_name in WINDOWS
            },
            "pages": {
                window_name: _copy_rows(sections.get("pages", {}).get(window_name, []))
                for window_name in WINDOWS
            },
            "quick_wins": {
                "last_28_days": _copy_rows(sections.get("quick_wins", {}).get("last_28_days", [])),
            },
            "indexing_review": {
                "last_28_days": _copy_rows(sections.get("indexing_review", {}).get("last_28_days", [])),
            },
            "top_page_movers": {
                "last_28_days": _copy_rows(sections.get("top_page_movers", {}).get("last_28_vs_previous_28", [])),
            },
            "queries_summary": {
                window_name: _query_summary(_copy_rows(queries.get(window_name, [])))
                for window_name in WINDOWS
            },
        }
        return snapshot

    def _previous_week_snapshot(
        self,
        snapshots: list[dict[str, Any]],
        current_snapshot: dict[str, Any],
    ) -> dict[str, Any] | None:
        current_week_key = _string(current_snapshot.get("week_key"))
        eligible = [
            snapshot
            for snapshot in snapshots
            if _string(snapshot.get("week_key")) and _string(snapshot.get("week_key")) != current_week_key
        ]
        if not eligible:
            return None
        eligible.sort(key=lambda item: _string(item.get("generated_at")))
        return eligible[-1]

    def _build_window_delta(
        self,
        window_name: str,
        current_snapshot: dict[str, Any],
        previous_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        current_pages = _copy_rows(current_snapshot.get("pages", {}).get(window_name, []))
        previous_pages = _copy_rows((previous_snapshot or {}).get("pages", {}).get(window_name, []))
        current_kpis = dict(current_snapshot.get("sitewide_summary", {}).get(window_name, {}))
        previous_kpis = dict((previous_snapshot or {}).get("sitewide_summary", {}).get(window_name, {}))

        if previous_snapshot is None:
            message_key = "messages.noPriorSnapshot"
            return {
                "available": False,
                "message_key": message_key,
                "current_snapshot": {
                    "snapshot_id": current_snapshot.get("snapshot_id"),
                    "generated_at": current_snapshot.get("generated_at"),
                    "week_key": current_snapshot.get("week_key"),
                },
                "previous_snapshot": None,
                "summary": {
                    "new_issues_count": 0,
                    "resolved_issues_count": 0,
                    "pages_improved_count": 0,
                    "pages_declined_count": 0,
                    "new_quick_wins_count": 0,
                    "removed_quick_wins_count": 0,
                    "reason_code_changes_count": 0,
                },
                "sitewide_summary_delta": {},
                "query_summary_delta": {},
                "panels": {
                    "new_issues": _delta_panel([], False, message_key),
                    "resolved_issues": _delta_panel([], False, message_key),
                    "new_quick_wins": _delta_panel([], False, message_key),
                    "removed_quick_wins": _delta_panel([], False, message_key),
                    "positive_movers": _delta_panel([], False, message_key),
                    "negative_movers": _delta_panel([], False, message_key),
                    "reason_code_changes": _delta_panel([], False, message_key),
                },
            }

        delta_rows = _page_delta_rows(current_pages, previous_pages)
        positive_movers = _sort_desc([row for row in delta_rows if _to_float(row.get("delta_score")) > 0], "delta_score")[:DELTA_TOP_N]
        negative_movers = _sort_asc([row for row in delta_rows if _to_float(row.get("delta_score")) < 0], "delta_score")[:DELTA_TOP_N]
        reason_code_changes = [
            row
            for row in delta_rows
            if row.get("reason_code_changed")
        ]

        quick_wins_supported = window_name == "last_28_days"
        indexing_supported = window_name == "last_28_days"

        current_quick_wins = _copy_rows(current_snapshot.get("quick_wins", {}).get("last_28_days", [])) if quick_wins_supported else []
        previous_quick_wins = _copy_rows(previous_snapshot.get("quick_wins", {}).get("last_28_days", [])) if quick_wins_supported else []
        current_quick_wins_by_key = _rows_by_key(current_quick_wins)
        previous_quick_wins_by_key = _rows_by_key(previous_quick_wins)

        new_quick_wins = [
            dict(current_quick_wins_by_key[key], quick_win_change="entered")
            for key in sorted(set(current_quick_wins_by_key) - set(previous_quick_wins_by_key))
        ]
        removed_quick_wins = [
            dict(previous_quick_wins_by_key[key], quick_win_change="removed")
            for key in sorted(set(previous_quick_wins_by_key) - set(current_quick_wins_by_key))
        ]

        current_indexing = _copy_rows(current_snapshot.get("indexing_review", {}).get("last_28_days", [])) if indexing_supported else []
        previous_indexing = _copy_rows(previous_snapshot.get("indexing_review", {}).get("last_28_days", [])) if indexing_supported else []
        current_indexing_by_key = _rows_by_key(current_indexing)
        previous_indexing_by_key = _rows_by_key(previous_indexing)

        new_issues: list[dict[str, Any]] = []
        resolved_issues: list[dict[str, Any]] = []
        for key in sorted(set(current_indexing_by_key) | set(previous_indexing_by_key)):
            current_row = current_indexing_by_key.get(key, {})
            previous_row = previous_indexing_by_key.get(key, {})
            current_issues = _issue_set(current_row)
            previous_issues = _issue_set(previous_row)

            newly_added = sorted(current_issues - previous_issues)
            newly_resolved = sorted(previous_issues - current_issues)

            if newly_added:
                new_issues.append(
                    {
                        "normalized_page_path": _string(current_row.get("normalized_page_path") or previous_row.get("normalized_page_path")),
                        "normalized_page_url": _string(current_row.get("normalized_page_url") or previous_row.get("normalized_page_url")),
                        "issue_types": newly_added,
                        "inspection_verdict": _string(current_row.get("inspection_verdict")),
                        "inspection_indexing_state": _string(current_row.get("inspection_indexing_state")),
                        "inspection_robots_txt_state": _string(current_row.get("inspection_robots_txt_state")),
                        "recommended_action": _string(current_row.get("recommended_action")) or "no_action",
                        "recommended_action_text": _string(current_row.get("recommended_action_text")),
                    }
                )

            if newly_resolved:
                resolved_issues.append(
                    {
                        "normalized_page_path": _string(previous_row.get("normalized_page_path") or current_row.get("normalized_page_path")),
                        "normalized_page_url": _string(previous_row.get("normalized_page_url") or current_row.get("normalized_page_url")),
                        "issue_types": newly_resolved,
                        "inspection_verdict": _string(previous_row.get("inspection_verdict")),
                        "inspection_indexing_state": _string(previous_row.get("inspection_indexing_state")),
                        "inspection_robots_txt_state": _string(previous_row.get("inspection_robots_txt_state")),
                        "recommended_action": _string(previous_row.get("recommended_action")) or "no_action",
                        "recommended_action_text": _string(previous_row.get("recommended_action_text")),
                    }
                )

        sitewide_summary_delta = {
            "gsc_clicks_delta": _to_float(current_kpis.get("gsc_clicks")) - _to_float(previous_kpis.get("gsc_clicks")),
            "gsc_impressions_delta": _to_float(current_kpis.get("gsc_impressions")) - _to_float(previous_kpis.get("gsc_impressions")),
            "avg_ctr_delta": _to_float(current_kpis.get("avg_ctr")) - _to_float(previous_kpis.get("avg_ctr")),
            "avg_position_delta": _to_float(current_kpis.get("avg_position")) - _to_float(previous_kpis.get("avg_position")),
            "ga4_sessions_delta": _to_float(current_kpis.get("ga4_sessions")) - _to_float(previous_kpis.get("ga4_sessions")),
            "ga4_conversions_delta": _to_float(current_kpis.get("ga4_conversions")) - _to_float(previous_kpis.get("ga4_conversions")),
        }

        current_query_summary = dict(current_snapshot.get("queries_summary", {}).get(window_name, {}))
        previous_query_summary = dict(previous_snapshot.get("queries_summary", {}).get(window_name, {}))
        query_summary_delta = {
            "available": bool(current_query_summary.get("available") and previous_query_summary.get("available")),
            "total_rows_delta": _to_float(current_query_summary.get("total_rows")) - _to_float(previous_query_summary.get("total_rows")),
            "total_clicks_delta": _to_float(current_query_summary.get("total_clicks")) - _to_float(previous_query_summary.get("total_clicks")),
            "total_impressions_delta": _to_float(current_query_summary.get("total_impressions")) - _to_float(previous_query_summary.get("total_impressions")),
            "avg_ctr_delta": _to_float(current_query_summary.get("avg_ctr")) - _to_float(previous_query_summary.get("avg_ctr")),
            "avg_position_delta": _to_float(current_query_summary.get("avg_position")) - _to_float(previous_query_summary.get("avg_position")),
        }

        return {
            "available": True,
            "message_key": "",
            "current_snapshot": {
                "snapshot_id": current_snapshot.get("snapshot_id"),
                "generated_at": current_snapshot.get("generated_at"),
                "week_key": current_snapshot.get("week_key"),
            },
            "previous_snapshot": {
                "snapshot_id": previous_snapshot.get("snapshot_id"),
                "generated_at": previous_snapshot.get("generated_at"),
                "week_key": previous_snapshot.get("week_key"),
            },
            "summary": {
                "new_issues_count": len(new_issues),
                "resolved_issues_count": len(resolved_issues),
                "pages_improved_count": len([row for row in delta_rows if _to_float(row.get("delta_score")) > 0]),
                "pages_declined_count": len([row for row in delta_rows if _to_float(row.get("delta_score")) < 0]),
                "new_quick_wins_count": len(new_quick_wins),
                "removed_quick_wins_count": len(removed_quick_wins),
                "reason_code_changes_count": len(reason_code_changes),
            },
            "sitewide_summary_delta": sitewide_summary_delta,
            "query_summary_delta": query_summary_delta,
            "panels": {
                "new_issues": _delta_panel(
                    new_issues[:DELTA_TOP_N],
                    indexing_supported,
                    "" if indexing_supported else _support_message_key(window_name, "indexing_review"),
                ),
                "resolved_issues": _delta_panel(
                    resolved_issues[:DELTA_TOP_N],
                    indexing_supported,
                    "" if indexing_supported else _support_message_key(window_name, "indexing_review"),
                ),
                "new_quick_wins": _delta_panel(
                    new_quick_wins[:DELTA_TOP_N],
                    quick_wins_supported,
                    "" if quick_wins_supported else _support_message_key(window_name, "quick_wins"),
                ),
                "removed_quick_wins": _delta_panel(
                    removed_quick_wins[:DELTA_TOP_N],
                    quick_wins_supported,
                    "" if quick_wins_supported else _support_message_key(window_name, "quick_wins"),
                ),
                "positive_movers": _delta_panel(
                    positive_movers,
                    bool(current_pages or previous_pages),
                    _support_message_key(window_name, "pages"),
                ),
                "negative_movers": _delta_panel(
                    negative_movers,
                    bool(current_pages or previous_pages),
                    _support_message_key(window_name, "pages"),
                ),
                "reason_code_changes": _delta_panel(
                    reason_code_changes[:DELTA_TOP_N],
                    bool(current_pages or previous_pages),
                    _support_message_key(window_name, "pages"),
                ),
            },
        }

    def build_weekly_delta(self, dashboard_payload: dict[str, Any], overwrite: bool = True) -> tuple[dict[str, Any], list[str]]:
        snapshots = self._load_snapshots()
        current_snapshot = self._build_snapshot(dashboard_payload)
        previous_snapshot = self._previous_week_snapshot(snapshots, current_snapshot)

        delta_payload = {
            "history": {
                "snapshot_count": len(snapshots) + 1,
                "current_snapshot_id": current_snapshot.get("snapshot_id"),
                "current_week_key": current_snapshot.get("week_key"),
                "previous_snapshot_id": _string((previous_snapshot or {}).get("snapshot_id")),
                "previous_week_key": _string((previous_snapshot or {}).get("week_key")),
            },
            "by_window": {
                window_name: self._build_window_delta(window_name, current_snapshot, previous_snapshot)
                for window_name in WINDOWS
            },
        }

        output_files: list[str] = []
        snapshot_path = self._snapshot_file_path(
            _string(current_snapshot.get("generated_at")),
            _string(current_snapshot.get("week_key")),
        )
        written_snapshot = write_json_file(snapshot_path, current_snapshot, overwrite=False)
        if written_snapshot:
            output_files.append(str(written_snapshot))
        else:  # pragma: no cover - unique path generation should prevent this
            self.logger.warning("History snapshot path already existed and was skipped: %s", snapshot_path)

        latest_snapshot_path = self.paths.data_history_latest_dir / "latest_snapshot.json"
        latest_delta_path = self.paths.data_history_latest_dir / "latest_weekly_delta.json"
        written_latest_snapshot = write_json_file(latest_snapshot_path, current_snapshot, overwrite=True)
        written_latest_delta = write_json_file(latest_delta_path, delta_payload, overwrite=True)
        if written_latest_snapshot:
            output_files.append(str(written_latest_snapshot))
        if written_latest_delta:
            output_files.append(str(written_latest_delta))

        return delta_payload, output_files
