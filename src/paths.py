from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    project_root: Path
    src_dir: Path
    config_dir: Path
    data_raw_dir: Path
    data_processed_dir: Path
    data_history_dir: Path
    data_history_snapshots_dir: Path
    data_history_latest_dir: Path
    logs_dir: Path
    output_dir: Path

    @classmethod
    def discover(cls) -> "ProjectPaths":
        project_root = Path(__file__).resolve().parent.parent
        return cls(
            project_root=project_root,
            src_dir=project_root / "src",
            config_dir=project_root / "config",
            data_raw_dir=project_root / "data" / "raw",
            data_processed_dir=project_root / "data" / "processed",
            data_history_dir=project_root / "data" / "history",
            data_history_snapshots_dir=project_root / "data" / "history" / "snapshots",
            data_history_latest_dir=project_root / "data" / "history" / "latest",
            logs_dir=project_root / "logs",
            output_dir=project_root / "output",
        )

    @property
    def base_directories(self) -> tuple[Path, ...]:
        return (
            self.config_dir,
            self.data_raw_dir,
            self.data_processed_dir,
            self.data_history_dir,
            self.data_history_snapshots_dir,
            self.data_history_latest_dir,
            self.logs_dir,
            self.output_dir,
        )

    def resolve(self, target: str | Path) -> Path:
        raw_path = Path(target).expanduser()
        return raw_path if raw_path.is_absolute() else self.project_root / raw_path

    def ensure_base_directories(self) -> list[Path]:
        created_directories: list[Path] = []

        for directory in self.base_directories:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                created_directories.append(directory)
            else:
                directory.mkdir(parents=True, exist_ok=True)

        return created_directories

    def ensure_parents_for_targets(self, targets: Iterable[str | Path]) -> list[Path]:
        created_directories: list[Path] = []
        seen_directories: set[Path] = set()

        for target in targets:
            parent_dir = self.resolve(target).parent

            if parent_dir in seen_directories:
                continue

            if not parent_dir.exists():
                parent_dir.mkdir(parents=True, exist_ok=True)
                created_directories.append(parent_dir)
            else:
                parent_dir.mkdir(parents=True, exist_ok=True)

            seen_directories.add(parent_dir)

        return created_directories

    def as_dict(self) -> dict[str, str]:
        return {
            "project_root": str(self.project_root),
            "src_dir": str(self.src_dir),
            "config_dir": str(self.config_dir),
            "data_raw_dir": str(self.data_raw_dir),
            "data_processed_dir": str(self.data_processed_dir),
            "data_history_dir": str(self.data_history_dir),
            "data_history_snapshots_dir": str(self.data_history_snapshots_dir),
            "data_history_latest_dir": str(self.data_history_latest_dir),
            "logs_dir": str(self.logs_dir),
            "output_dir": str(self.output_dir),
        }
