from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from ..config import AppConfig
from ..paths import ProjectPaths


GOOGLE_READONLY_SCOPES = (
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/analytics.readonly",
)


class OAuthError(Exception):
    """Raised when Google OAuth authentication cannot be completed."""


def get_google_scopes() -> tuple[str, ...]:
    return GOOGLE_READONLY_SCOPES


def _import_google_auth_dependencies() -> tuple[Any, Any, Any, Any]:
    try:
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ModuleNotFoundError as exc:
        raise OAuthError(
            "Google authentication dependencies are not installed. "
            "Run: pip install -r requirements.txt"
        ) from exc

    return Request, RefreshError, Credentials, InstalledAppFlow


def load_token(token_path: str | Path, scopes: Sequence[str]) -> Any | None:
    _, _, Credentials, _ = _import_google_auth_dependencies()
    resolved_token_path = Path(token_path).expanduser().resolve()

    if not resolved_token_path.exists():
        return None

    try:
        return Credentials.from_authorized_user_file(
            str(resolved_token_path),
            scopes=list(scopes),
        )
    except ValueError as exc:
        raise OAuthError(
            f"Stored OAuth token file is invalid or unreadable: {resolved_token_path}"
        ) from exc


def save_token(credentials: Any, token_path: str | Path) -> Path:
    resolved_token_path = Path(token_path).expanduser().resolve()
    resolved_token_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_token_path.write_text(credentials.to_json(), encoding="utf-8")
    return resolved_token_path


def credentials_valid(credentials: Any) -> bool:
    return bool(credentials and credentials.valid)


def authenticate_google(
    credentials_file: str | Path,
    token_file: str | Path,
    scopes: Sequence[str],
    logger: Any,
) -> Any:
    Request, RefreshError, _, InstalledAppFlow = _import_google_auth_dependencies()

    resolved_credentials_file = Path(credentials_file).expanduser().resolve()
    resolved_token_file = Path(token_file).expanduser().resolve()

    logger.info("Preparing Google OAuth credentials.")
    credentials: Any | None = None

    if resolved_token_file.exists():
        logger.info("Existing OAuth token file detected.")
        try:
            credentials = load_token(resolved_token_file, scopes)
        except OAuthError as exc:
            logger.warning("%s Browser authentication will be started.", exc)
            credentials = None

    if credentials and not credentials.has_scopes(list(scopes)):
        logger.info(
            "Stored OAuth token does not include all required scopes. "
            "Browser authentication will be started."
        )
        credentials = None

    if credentials_valid(credentials):
        logger.info("Using existing valid Google OAuth token.")
        return credentials

    if credentials and credentials.expired and credentials.refresh_token:
        logger.info("Refreshing expired Google OAuth token.")
        try:
            credentials.refresh(Request())
            save_token(credentials, resolved_token_file)
            logger.info("Google OAuth token refreshed successfully.")
            return credentials
        except RefreshError as exc:
            logger.warning(
                "Google OAuth token refresh failed. Browser authentication is required: %s",
                exc,
            )
        except Exception as exc:
            logger.warning(
                "Unexpected error while refreshing Google OAuth token. "
                "Browser authentication is required: %s",
                exc,
            )

    logger.info("Launching browser-based Google OAuth flow.")
    if not resolved_credentials_file.exists():
        raise OAuthError(
            "OAuth client credentials file not found: "
            f"{resolved_credentials_file}"
        )

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(resolved_credentials_file),
            list(scopes),
        )
        credentials = flow.run_local_server(
            port=0,
            open_browser=True,
            access_type="offline",
            prompt="consent",
        )
    except ValueError as exc:
        raise OAuthError(
            f"OAuth client credentials file is invalid: {resolved_credentials_file}"
        ) from exc
    except Exception as exc:
        raise OAuthError(f"Google browser OAuth flow failed: {exc}") from exc

    if not credentials_valid(credentials):
        raise OAuthError("Google OAuth flow completed without valid credentials.")

    save_token(credentials, resolved_token_file)
    logger.info("Google OAuth token saved successfully.")
    return credentials


def get_google_credentials_from_config(app_config: AppConfig, logger: Any) -> Any:
    return authenticate_google(
        credentials_file=app_config.resolve_path(app_config.google_oauth_credentials_file),
        token_file=app_config.resolve_path(app_config.google_oauth_token_file),
        scopes=get_google_scopes(),
        logger=logger,
    )


@dataclass(slots=True)
class OAuthService:
    config: AppConfig
    paths: ProjectPaths

    def get_credentials(self, logger: Any) -> Any:
        return get_google_credentials_from_config(self.config, logger)

    def status(self) -> str:
        credentials_path = self.config.resolve_path(self.config.google_oauth_credentials_file)
        token_path = self.config.resolve_path(self.config.google_oauth_token_file)
        return (
            "OAuth service is configured for installed-app browser login with "
            f"credentials at {credentials_path} and token cache at {token_path}."
        )
