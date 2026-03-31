from __future__ import annotations

import logging
from pathlib import Path

from .config import AppConfig
from .paths import ProjectPaths
from .utils.io_utils import touch_file

APP_LOGGER_NAME = "seo_dashboard.app"
ERROR_LOGGER_NAME = "seo_dashboard.error"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _build_formatter() -> logging.Formatter:
    return logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)


def _build_stream_handler(level: int) -> logging.Handler:
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(_build_formatter())
    return handler


def _build_file_handler(file_path: Path, level: int) -> logging.Handler:
    handler = logging.FileHandler(file_path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(_build_formatter())
    return handler


def _reset_logger(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def _configure_logger(logger_name: str, level: int, file_path: Path) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    _reset_logger(logger)
    logger.setLevel(level)
    logger.propagate = False
    logger.addHandler(_build_stream_handler(level))
    logger.addHandler(_build_file_handler(file_path, level))
    return logger


def setup_logging(
    project_paths: ProjectPaths,
    config: AppConfig | None = None,
) -> tuple[logging.Logger, logging.Logger]:
    app_log_path = project_paths.resolve(config.log_app_file if config else "logs/app.log")
    error_log_path = project_paths.resolve(config.log_error_file if config else "logs/error.log")

    touch_file(app_log_path)
    touch_file(error_log_path)

    app_logger = _configure_logger(APP_LOGGER_NAME, logging.INFO, app_log_path)
    error_logger = _configure_logger(ERROR_LOGGER_NAME, logging.ERROR, error_log_path)
    return app_logger, error_logger


def get_app_logger() -> logging.Logger:
    return logging.getLogger(APP_LOGGER_NAME)


def get_error_logger() -> logging.Logger:
    return logging.getLogger(ERROR_LOGGER_NAME)
