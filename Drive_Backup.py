import os
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# -------------------- CONFIG --------------------
# OAuth files
CLIENT_SECRET_FILE = os.getenv("GOOGLE_OAUTH_CLIENT_FILE", "credentials.json")
TOKEN_FILE = os.getenv("GOOGLE_OAUTH_TOKEN_FILE", "token.json")

# Least privilege for personal uploads (can read/write files you create or open with the picker)
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Local folder with your videos:
LOCAL_FOLDER = "D:\\Projects\\Drive_Backup_Threading\\Source"  # <-- change this

# If you already have a Drive folder, put its ID here; else leave None and we’ll create/find by name:
PARENT_FOLDER_ID = None
DRIVE_FOLDER_NAME = "Testing_Drive"   # used if PARENT_FOLDER_ID is None

MAX_WORKERS = 4          # modest concurrency; increase carefully if you see 429s/403s
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB chunks for faster uploads
MAX_RETRIES = 8
# ------------------------------------------------

_thread_local = threading.local()
_base_creds = None  # main process/user creds loaded once; cloned per thread


def _load_user_creds() -> Credentials:
    """
    Load/refresh OAuth user credentials (token.json).
    If missing/expired, run the local browser flow to obtain new credentials.
    """
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Starts a local server and opens your browser to grant access
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Persist for next runs
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


def _make_service_from_creds(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _make_service_for_thread():
    """
    Each thread uses its own Drive service instance.
    We reuse the same user creds but construct a new service per thread.
    """
    svc = getattr(_thread_local, "drive_service", None)
    if svc is not None:
        return svc
    # Clone credentials for safety (google-auth Credentials are thread-safe for reads,
    # but we avoid sharing mutable state)
    thread_creds = Credentials(
        token=_base_creds.token,
        refresh_token=_base_creds.refresh_token,
        token_uri=_base_creds.token_uri,
        client_id=_base_creds.client_id,
        client_secret=_base_creds.client_secret,
        scopes=_base_creds.scopes,
    )
    _thread_local.drive_service = _make_service_from_creds(thread_creds)
    return _thread_local.drive_service


def _ensure_drive_folder(folder_name: str) -> str:
    """
    Create (or fetch) a *My Drive* folder by name and return its ID.
    Note: name lookup is not unique; we pick the first match.
    """
    service = _make_service_for_thread()

    # Escape single quotes just in case
    safe = folder_name.replace("'", "\\'")
    resp = service.files().list(
        q=f"mimeType='application/vnd.google-apps.folder' and name='{safe}' and trashed=false",
        spaces="drive",
        fields="files(id, name)",
        pageSize=10,
    ).execute()

    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    created = service.files().create(body=metadata, fields="id, name").execute()
    return created["id"]


def _upload_with_resumable(file_path: str, display_name: str, parent_id: str) -> dict:
    service = _make_service_for_thread()
    file_metadata = {"name": display_name, "parents": [parent_id]}

    media = MediaFileUpload(file_path, resumable=True, chunksize=CHUNK_SIZE)

    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name, parents, mimeType, size, webViewLink",
    )

    backoff = 1.0
    tries = 0

    while True:
        try:
            status, response = request.next_chunk()
            if response is not None:
                return response
            if status:
                print(f"[{threading.current_thread().name}] {display_name}: {int(status.progress() * 100)}%")
        except HttpError as e:
            code = getattr(e, "status_code", None) or getattr(e, "resp", getattr(e, "response", None))
            code = getattr(code, "status", None) or getattr(code, "status_code", None) or getattr(e, "resp", None) and e.resp.status
            code = code or 0  # best-effort

            if code in (403, 408, 429, 500, 502, 503, 504) and tries < MAX_RETRIES:
                tries += 1
                print(f"[{threading.current_thread().name}] {display_name}: transient {code}, sleep {backoff:.1f}s (retry {tries}/{MAX_RETRIES})")
                time.sleep(backoff)
                backoff = min(backoff * 2, 32)
                continue
            raise


def _gather_mp4s(folder: str) -> list[tuple[str, str]]:
    p = Path(folder)
    return [(str(f), f.name) for f in sorted(p.glob("*.mp4"))]


def upload_folder_of_videos(local_folder: str, parent_folder_id: str | None, drive_folder_name: str | None):
    # Resolve target folder in *your* My Drive
    if parent_folder_id:
        target_folder_id = parent_folder_id
    else:
        if not drive_folder_name:
            drive_folder_name = Path(local_folder).name
        target_folder_id = _ensure_drive_folder(drive_folder_name)

    files = _gather_mp4s(local_folder)
    if not files:
        print("No .mp4 files found.")
        return []

    print(f"Uploading {len(files)} video(s) to Drive folder ID: {target_folder_id}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="drive-up") as pool:
        futs = {
            pool.submit(_upload_with_resumable, path, name, target_folder_id): (path, name)
            for path, name in files
        }
        for fut in as_completed(futs):
            path, name = futs[fut]
            try:
                resp = fut.result()
                results.append(resp)
                link = resp.get("webViewLink", "(no link)")
                print(f"Uploaded: {name} -> fileId={resp['id']} | {link}")
            except Exception as exc:
                print(f"FAILED: {name} -> {exc}")
    return results


if __name__ == "__main__":
    # Load user creds once, then use per-thread services made from them
    _base_creds = _load_user_creds()
    upload_folder_of_videos(LOCAL_FOLDER, PARENT_FOLDER_ID, DRIVE_FOLDER_NAME)
