from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


def ensure_parent_dir(path: str | Path) -> Path:
    resolved_path = Path(path).expanduser()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_path.parent


def touch_file(path: str | Path) -> Path:
    resolved_path = Path(path).expanduser()
    ensure_parent_dir(resolved_path)
    resolved_path.touch(exist_ok=True)
    return resolved_path


def read_json_file(path: str | Path) -> dict[str, Any]:
    resolved_path = Path(path).expanduser()

    with resolved_path.open("r", encoding="utf-8") as file_handle:
        payload = json.load(file_handle)

    if not isinstance(payload, dict):
        raise ValueError("JSON root must be an object.")

    return payload


def read_csv_file(path: str | Path) -> list[dict[str, str]]:
    resolved_path = Path(path).expanduser()

    with resolved_path.open("r", encoding="utf-8", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        return [dict(row) for row in reader]


def write_text_file(path: str | Path, content: str) -> Path:
    resolved_path = Path(path).expanduser()
    ensure_parent_dir(resolved_path)
    resolved_path.write_text(content, encoding="utf-8")
    return resolved_path


def write_json_file(
    path: str | Path,
    payload: Any,
    overwrite: bool = True,
) -> Path | None:
    resolved_path = Path(path).expanduser()
    ensure_parent_dir(resolved_path)

    if resolved_path.exists() and not overwrite:
        return None

    with resolved_path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2, ensure_ascii=False)

    return resolved_path


def write_csv_file(
    path: str | Path,
    rows: Iterable[dict[str, Any]],
    fieldnames: list[str],
    overwrite: bool = True,
) -> Path | None:
    resolved_path = Path(path).expanduser()
    ensure_parent_dir(resolved_path)

    if resolved_path.exists() and not overwrite:
        return None

    materialized_rows = list(rows)

    with resolved_path.open("w", encoding="utf-8", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in materialized_rows:
            writer.writerow(row)

    return resolved_path
