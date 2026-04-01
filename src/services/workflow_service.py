from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..paths import ProjectPaths
from ..utils.io_utils import read_json_file, write_json_file

WORKFLOW_STATUSES = (
    "new",
    "planned",
    "in_progress",
    "done",
    "ignored",
    "recheck_later",
)

WORKFLOW_STATUSES_FILENAME = "workflow_statuses.json"
WORKFLOW_NOTES_FILENAME = "notes.json"
WORKFLOW_VERSION = 1


def _string(value: Any) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_issue_types(value: Any) -> list[str]:
    if isinstance(value, list):
        return sorted({_string(item) for item in value if _string(item)})
    if isinstance(value, str):
        return sorted({_string(item) for item in value.split("|") if _string(item)})
    return []


def make_page_record_key(normalized_page_path: str, normalized_page_url: str = "") -> str:
    path = _string(normalized_page_path)
    if path:
        return f"page::{path}"

    url = _string(normalized_page_url)
    if url:
        return f"page::{url}"

    return "page::unknown"


def make_issue_record_key(normalized_page_path: str, issue_types: list[str] | str | None = None) -> str:
    path = _string(normalized_page_path)
    issues = _ensure_issue_types(issue_types or [])

    if not path or not issues:
        return ""

    return f"issue::{path}::{'|'.join(issues)}"


def _empty_status_payload() -> dict[str, Any]:
    return {
        "version": WORKFLOW_VERSION,
        "updated_at": "",
        "records": {},
    }


def _empty_notes_payload() -> dict[str, Any]:
    return {
        "version": WORKFLOW_VERSION,
        "updated_at": "",
        "records": {},
    }


def _normalize_status_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    records = data.get("records")
    return {
        "version": int(data.get("version") or WORKFLOW_VERSION),
        "updated_at": _string(data.get("updated_at")),
        "records": records if isinstance(records, dict) else {},
    }


def _normalize_notes_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    records = data.get("records")
    return {
        "version": int(data.get("version") or WORKFLOW_VERSION),
        "updated_at": _string(data.get("updated_at")),
        "records": records if isinstance(records, dict) else {},
    }


@dataclass(slots=True)
class WorkflowService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any

    @property
    def statuses_path(self) -> Path:
        return self.paths.data_state_dir / WORKFLOW_STATUSES_FILENAME

    @property
    def notes_path(self) -> Path:
        return self.paths.data_state_dir / WORKFLOW_NOTES_FILENAME

    def ensure_state_files(self) -> list[str]:
        created: list[str] = []

        if not self.statuses_path.exists():
            written = write_json_file(self.statuses_path, _empty_status_payload(), overwrite=True)
            if written:
                created.append(str(written))

        if not self.notes_path.exists():
            written = write_json_file(self.notes_path, _empty_notes_payload(), overwrite=True)
            if written:
                created.append(str(written))

        return created

    def load_state(self) -> tuple[dict[str, Any], dict[str, Any]]:
        statuses = _empty_status_payload()
        notes = _empty_notes_payload()

        if self.statuses_path.exists():
            try:
                statuses = _normalize_status_payload(read_json_file(self.statuses_path))
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Failed to read workflow statuses from %s: %s", self.statuses_path, exc)

        if self.notes_path.exists():
            try:
                notes = _normalize_notes_payload(read_json_file(self.notes_path))
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Failed to read workflow notes from %s: %s", self.notes_path, exc)

        return statuses, notes

    def apply_to_rows(self, rows: list[dict[str, Any]], scope: str = "page") -> list[dict[str, Any]]:
        statuses_payload, notes_payload = self.load_state()
        status_records = statuses_payload.get("records", {})
        note_records = notes_payload.get("records", {})
        enriched: list[dict[str, Any]] = []

        for row in rows:
            page_key = make_page_record_key(
                _string(row.get("normalized_page_path")),
                _string(row.get("normalized_page_url")),
            )
            issue_key = (
                make_issue_record_key(
                    _string(row.get("normalized_page_path")),
                    row.get("issue_types"),
                )
                if scope == "issue"
                else ""
            )
            record_key = issue_key or page_key
            status_entry = (
                status_records.get(record_key)
                or status_records.get(page_key)
                or {}
            )
            note_entry = (
                note_records.get(record_key)
                or note_records.get(page_key)
                or {}
            )
            explicit_status = bool(status_records.get(record_key) or status_records.get(page_key))
            explicit_note = bool(note_records.get(record_key) or note_records.get(page_key))

            status = _string(status_entry.get("status"))
            if status not in WORKFLOW_STATUSES:
                status = "new"

            note = _string(note_entry.get("note"))
            status_source = "issue" if issue_key and status_records.get(issue_key) else ("page" if status_records.get(page_key) else "default")
            note_source = "issue" if issue_key and note_records.get(issue_key) else ("page" if note_records.get(page_key) else "default")

            enriched.append(
                {
                    **row,
                    "workflow_scope": "issue" if issue_key else "page",
                    "workflow_page_key": page_key,
                    "workflow_issue_key": issue_key,
                    "workflow_record_key": record_key,
                    "workflow_status": status,
                    "workflow_status_explicit": explicit_status,
                    "workflow_status_source": status_source,
                    "workflow_status_updated_at": _string(status_entry.get("updated_at")),
                    "workflow_note": note,
                    "workflow_note_present": bool(note),
                    "workflow_note_source": note_source,
                    "workflow_note_updated_at": _string(note_entry.get("updated_at")),
                    "workflow_issue_signature": "|".join(_ensure_issue_types(row.get("issue_types"))),
                }
            )

        return enriched

    def summary_for_rows(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        counts = {status: 0 for status in WORKFLOW_STATUSES}
        for row in rows:
            status = _string(row.get("workflow_status")) or "new"
            if status not in counts:
                counts[status] = 0
            counts[status] += 1
        return counts
