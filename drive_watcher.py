import json
import logging
import os
from datetime import datetime, timezone, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
STATE_FILE = "drive_state.json"


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as exc:
        logger.warning("Could not save drive state: %s", exc)


def _build_service():
    creds = service_account.Credentials.from_service_account_file(
        config.GOOGLE_CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_new_video_files() -> list[dict]:
    """
    List all subfolders of DRIVE_PARENT_FOLDER_ID.
    For each subfolder, find video files created after last_checked.
    Returns list of dicts: {file_id, file_name, vendor_name, folder_id}
    """
    state = _load_state()
    now_utc = datetime.now(timezone.utc)

    # Use last_checked from state; fall back to POLL_INTERVAL * 2 minutes ago
    if "last_checked" in state:
        try:
            last_checked = datetime.fromisoformat(state["last_checked"])
            if last_checked.tzinfo is None:
                last_checked = last_checked.replace(tzinfo=timezone.utc)
        except ValueError:
            last_checked = now_utc - timedelta(minutes=config.POLL_INTERVAL_MINUTES * 2)
    else:
        last_checked = now_utc - timedelta(minutes=config.POLL_INTERVAL_MINUTES * 2)

    created_after = last_checked.strftime("%Y-%m-%dT%H:%M:%S")
    logger.info("Scanning for new videos created after %s", created_after)

    try:
        service = _build_service()
    except Exception as exc:
        logger.error("Failed to build Drive service: %s", exc)
        return []

    # List subfolders of the parent folder
    try:
        folders_result = service.files().list(
            q=(
                f"'{config.DRIVE_PARENT_FOLDER_ID}' in parents "
                "and mimeType = 'application/vnd.google-apps.folder' "
                "and trashed = false"
            ),
            fields="files(id, name)",
            pageSize=200,
        ).execute()
    except Exception as exc:
        logger.error("Failed to list subfolders: %s", exc)
        return []

    folders = folders_result.get("files", [])
    logger.info("Found %d vendor subfolders", len(folders))

    new_files = []
    for folder in folders:
        vendor_name = folder["name"].strip()
        folder_id = folder["id"]
        try:
            videos_result = service.files().list(
                q=(
                    f"'{folder_id}' in parents "
                    "and mimeType contains 'video' "
                    f"and createdTime > '{created_after}' "
                    "and trashed = false"
                ),
                fields="files(id, name, createdTime)",
                orderBy="createdTime desc",
                pageSize=50,
            ).execute()
        except Exception as exc:
            logger.error("Failed to list videos in folder %s (%s): %s", vendor_name, folder_id, exc)
            continue

        for f in videos_result.get("files", []):
            logger.info("New video found: %s / %s", vendor_name, f["name"])
            new_files.append(
                {
                    "file_id": f["id"],
                    "file_name": f["name"],
                    "vendor_name": vendor_name,
                    "folder_id": folder_id,
                }
            )

    # Update last_checked to now
    state["last_checked"] = now_utc.isoformat()
    _save_state(state)

    logger.info("Total new video files detected: %d", len(new_files))
    return new_files


def download_file(file_id: str, dest_path: str):
    """Download a Drive file by ID to dest_path."""
    try:
        service = _build_service()
        request = service.files().get_media(fileId=file_id)
        with open(dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug("Download progress: %d%%", int(status.progress() * 100))
        logger.info("Downloaded file %s to %s", file_id, dest_path)
    except Exception as exc:
        logger.error("Failed to download file %s: %s", file_id, exc)
        raise
