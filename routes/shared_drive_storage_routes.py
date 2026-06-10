"""
routes/shared_drive_storage_routes.py

POST /api/shared-drive/storage-sizes
─────────────────────────────────────
Accepts source→destination Shared Drive ID pairs and returns storage size
for both sides in one response row per pair:

    Source Drive ID         Source Size    Destination Drive ID     Dest Size
    0AMsjkUJDhC7EUk9PVA    4.50 GB        0AIlOaJWf3SCuUk9PVA      0.001 GB
    0AJyBEB19OuSEUk9PVA    2.10 GB        0AMq5a3IghgDrUk9PVA      0.000 GB

WHY NOT THE REPORTS API?
────────────────────────
The Admin Reports API reports per-USER quota. Shared Drive storage is NOT
attributed to any individual user, so Reports API cannot answer "how many
GB is in Drive X?". The Drive v3 files().list() with corpora=drive is the
correct tool — it returns every non-trashed file's `size` field.

AUTHENTICATION FLOW (per drive)
────────────────────────────────
1. Build a Drive v3 service impersonating SOURCE_ADMIN_EMAIL (or
   DEST_ADMIN_EMAIL) via service-account domain-wide delegation (DWD).
2. For each drive_id:
     a. Check whether the admin is already a member (useDomainAdminAccess=True
        so the check works without prior membership).
     b. If NOT a member → add temporary 'organizer' permission.
        (files().list requires actual drive membership — useDomainAdminAccess
        alone is not sufficient for file-level calls.)
     c. Paginate files().list() and sum the `size` field.
     d. Revoke the temporary permission in a `finally` block — always runs.

REQUEST BODY (JSON)
────────────────────
{
    "sessionId":  "<sid>",           // optional
    "drive_pairs": [
        {
            "source_drive_id": "0AMsjkUJDhC7EUk9PVA",
            "dest_drive_id":   "0AIlOaJWf3SCuUk9PVA"
        },
        {
            "source_drive_id": "0AJyBEB19OuSEUk9PVA",
            "dest_drive_id":   "0AMq5a3IghgDrUk9PVA"
        }
    ]
}

RESPONSE (200)
──────────────
{
    "rows": [
        {
            "source_drive_id":    "0AMsjkUJDhC7EUk9PVA",
            "source_drive_name":  "Finance Q1",
            "source_total_bytes": 4831838208,
            "source_total_gb":    4.5,
            "source_file_count":  1243,
            "source_error":       null,

            "dest_drive_id":      "0AIlOaJWf3SCuUk9PVA",
            "dest_drive_name":    "Finance Q1 (Dest)",
            "dest_total_bytes":   1024000,
            "dest_total_gb":      0.001,
            "dest_file_count":    3,
            "dest_error":         null
        }
    ]
}

RESPONSE (4xx / 5xx)
────────────────────
{ "error": "..." }

REGISTER IN app.py
──────────────────
from routes.shared_drive_storage_routes import shared_drive_storage_bp
app.register_blueprint(shared_drive_storage_bp, url_prefix="/api")
"""

import logging
import sys
import time
from pathlib import Path

from flask import Blueprint, jsonify, request
from routes.auth_routes import require_auth
import session_state as state

shared_drive_storage_bp = Blueprint("shared_drive_storage", __name__)
BACKEND_DIR             = Path.home() / "amey"
_FLASK_CRED_DIR         = Path.home() / "flask-backend" / "uploads" / "credential"

logger = logging.getLogger(__name__)

_BYTES_PER_GB = 1_073_741_824.0   # 1024 ** 3


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/storage-sizes
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_storage_bp.route("/shared-drive/storage-sizes", methods=["POST"])
@require_auth
def get_shared_drive_storage_sizes():
    """
    Fetch storage for source + destination drive pairs in a single call.
    Returns one row per pair containing both source and destination sizes.
    """
    body = request.get_json(silent=True) or {}

    sid = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    drive_pairs = body.get("drive_pairs") or []

    if not drive_pairs:
        return jsonify({"error": "drive_pairs is required and must not be empty"}), 400
    if not isinstance(drive_pairs, list):
        return jsonify({"error": "drive_pairs must be a JSON array"}), 400

    if not BACKEND_DIR.exists():
        return jsonify({"error": "Backend repo not found at ~/amey"}), 500

    _ensure_backend_on_path()

    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    try:
        from routes.storage_routes import fetch_and_store_dest_super_admins
        fetch_and_store_dest_super_admins()
        logger.info("[sd-storage] Admin pool refreshed.")
    except Exception as e:
        logger.warning(f"[sd-storage] Admin pool refresh skipped: {e}")

    # Build both Drive services up front — fail fast if credentials are missing
    try:
        src_svc, src_admin = _build_drive_service("source")
    except Exception as exc:
        logger.error(f"[sd-storage] Source Drive service failed: {exc}")
        return jsonify({"error": f"Source credentials error: {exc}"}), 500

    try:
        dst_svc, dst_admin = _build_drive_service("destination")
    except Exception as exc:
        logger.error(f"[sd-storage] Destination Drive service failed: {exc}")
        return jsonify({"error": f"Destination credentials error: {exc}"}), 500

    rows = []
    for pair in drive_pairs:
        src_id = (pair.get("source_drive_id") or "").strip()
        dst_id = (pair.get("dest_drive_id")   or "").strip()

        if not src_id and not dst_id:
            continue

        # Source and destination fetched sequentially to avoid quota spikes
        src_result = (
            _get_drive_storage(src_svc, src_admin, src_id)
            if src_id else _empty_result(src_id)
        )
        dst_result = (
            _get_drive_storage(dst_svc, dst_admin, dst_id)
            if dst_id else _empty_result(dst_id)
        )

        rows.append({
            "source_drive_id":    src_id,
            "source_drive_name":  src_result["drive_name"],
            "source_total_bytes": src_result["total_bytes"],
            "source_total_gb":    src_result["total_gb"],
            "source_file_count":  src_result["file_count"],
            "source_error":       src_result["error"],

            "dest_drive_id":      dst_id,
            "dest_drive_name":    dst_result["drive_name"],
            "dest_total_bytes":   dst_result["total_bytes"],
            "dest_total_gb":      dst_result["total_gb"],
            "dest_file_count":    dst_result["file_count"],
            "dest_error":         dst_result["error"],
        })

    logger.info(f"[sd-storage] Completed {len(rows)} drive pair(s)")
    return jsonify({"rows": rows})


# ─────────────────────────────────────────────────────────────────────────────
# Core: per-drive size with full temp-membership lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def _get_drive_storage(drive_svc, admin_email: str, drive_id: str) -> dict:
    """
    Full lifecycle for one drive:
      1. Resolve human-readable drive name (best-effort).
      2. Check/add admin membership — add temp 'organizer' if not a member.
      3. Sum all file sizes via files().list().
      4. Always revoke temp permission in finally block.

    Never raises — always returns a dict.
    """
    drive_name   = drive_id   # fallback until Step 1 succeeds
    was_temp     = False
    temp_perm_id = None

    # ── Step 1: resolve drive name ────────────────────────────────────────────
    try:
        info = drive_svc.drives().get(driveId=drive_id,fields="name",useDomainAdminAccess=True,).execute()
        drive_name = info.get("name", drive_id)
    except Exception as exc:
        logger.warning(f"[sd-storage] Could not fetch name for {drive_id}: {exc}")

    # ── Step 2: ensure admin membership ──────────────────────────────────────
    try:
        was_temp, temp_perm_id = _ensure_admin_member(
            drive_svc, admin_email, drive_id, drive_name
        )
    except Exception as exc:
        logger.error(
            f"[sd-storage] Membership check failed for '{drive_name}' ({drive_id}): {exc}"
        )
        return _drive_error(drive_id, drive_name, str(exc))

    # ── Step 3: crawl files and sum sizes ─────────────────────────────────────
    try:
        total_bytes, file_count = _sum_drive_files(drive_svc, drive_id, drive_name)
        total_gb = round(total_bytes / _BYTES_PER_GB, 3)

        logger.info(
            f"[sd-storage] '{drive_name}' ({drive_id}) | "
            f"files={file_count} bytes={total_bytes} gb={total_gb} "
            f"was_temp_member={was_temp}"
        )
        return {
            "drive_id":        drive_id,
            "drive_name":      drive_name,
            "total_bytes":     total_bytes,
            "total_gb":        total_gb,
            "file_count":      file_count,
            "was_temp_member": was_temp,
            "error":           None,
        }

    except Exception as exc:
        logger.error(
            f"[sd-storage] File scan failed for '{drive_name}' ({drive_id}): {exc}"
        )
        return _drive_error(drive_id, drive_name, str(exc), was_temp)

    finally:
        # ── Step 4: always revoke temp membership ─────────────────────────────
        if was_temp and temp_perm_id:
            _revoke_admin_member(drive_svc, drive_id, drive_name, temp_perm_id)


def _ensure_admin_member(
    drive_svc,
    admin_email: str,
    drive_id: str,
    drive_name: str,
) -> tuple:
    """
    Check if admin_email is already a Shared Drive member.
      Already member → (False, None)           — nothing changed.
      Not a member   → add organizer → (True, perm_id).

    Both calls use useDomainAdminAccess=True so they work even before the
    admin is explicitly a member.
    """
    admin_lower = admin_email.lower()

    resp = drive_svc.permissions().list(
        fileId=drive_id,
        supportsAllDrives=True,
        useDomainAdminAccess=True,
        fields="permissions(id,emailAddress,type,role)",
    ).execute()

    for perm in resp.get("permissions", []):
        if (perm.get("emailAddress") or "").lower() == admin_lower:
            logger.debug(
                f"[sd-storage] Admin '{admin_email}' already a member of "
                f"'{drive_name}' (role={perm.get('role')}) — no temp add needed."
            )
            return False, None

    # Not a member — add temporarily as manager (full access, revoked after size fetch)
    logger.info(
        f"[sd-storage] Admin '{admin_email}' NOT a member of "
        f"'{drive_name}' ({drive_id}) — adding temporary 'manager'."
    )
    result = drive_svc.permissions().create(
        fileId=drive_id,
        supportsAllDrives=True,
        useDomainAdminAccess=True,
        sendNotificationEmail=False,
        body={
            "type":         "user",
            "role":         "organizer",
            "emailAddress": admin_email,
        },
        fields="id",
    ).execute()

    perm_id = result.get("id")
    if perm_id:
        logger.info(
            f"[sd-storage] ✓ Temporary manager added to '{drive_name}' "
            f"(permissionId={perm_id})"
        )
        return True, perm_id

    logger.warning(
        f"[sd-storage] permissions.create returned no id for '{drive_name}'"
    )
    return False, None


def _revoke_admin_member(
    drive_svc,
    drive_id: str,
    drive_name: str,
    perm_id: str,
) -> None:
    """Remove the temporary organizer permission. Never raises."""
    logger.info(
        f"[sd-storage] Removing temp permission '{perm_id}' from '{drive_name}'..."
    )
    try:
        drive_svc.permissions().delete(
            fileId=drive_id,
            permissionId=perm_id,
            supportsAllDrives=True,
            useDomainAdminAccess=True,
        ).execute()
        logger.info(f"[sd-storage] ✓ Temp membership removed from '{drive_name}'")
    except Exception as exc:
        logger.warning(
            f"[sd-storage] Could not remove temp permission '{perm_id}' "
            f"from '{drive_name}': {exc} — remove manually if needed."
        )


def _sum_drive_files(drive_svc, drive_id: str, drive_name: str) -> tuple:
    """
    Paginate files().list() for the Shared Drive and sum the `size` field.
    Folders are skipped (containers, not files).
    Google Workspace native files (Docs/Sheets/etc.) have no binary size —
    they are counted but contribute 0 bytes.

    Returns (total_bytes: int, file_count: int).
    """
    total_bytes = 0
    file_count  = 0
    page_token  = None

    logger.info(f"[sd-storage] Scanning files in '{drive_name}' ({drive_id})...")

    while True:
        resp = drive_svc.files().list(
            q="trashed=false",
            spaces="drive",
            corpora="drive",
            driveId=drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields="nextPageToken, files(id, mimeType, size)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()

        for f in resp.get("files", []):
            if f.get("mimeType") == "application/vnd.google-apps.folder":
                continue          # folders are containers, not files
            file_count  += 1
            total_bytes += int(f.get("size") or 0)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

        time.sleep(0.15)   # stay within Drive API quota

    logger.info(
        f"[sd-storage] '{drive_name}': {file_count} files, {total_bytes} bytes"
    )
    return total_bytes, file_count


# ─────────────────────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────────────────────

def _build_drive_service(side: str):
    """
    Build a Drive v3 service impersonating SOURCE_ADMIN_EMAIL or
    DEST_ADMIN_EMAIL via service-account domain-wide delegation.

    Credential resolution (mirrors shared_drive_routes.py):
      1. ~/flask-backend/uploads/credential/{source|dest}_credentials.json
      2. Config.SOURCE_CREDENTIALS_FILE / DEST_CREDENTIALS_FILE

    Returns (drive_service, admin_email).
    Raises FileNotFoundError if credentials cannot be found.
    """
    from config import Config
    import httplib2
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    if side == "source":
        flask_name  = "source_credentials.json"
        config_path = Config.SOURCE_CREDENTIALS_FILE
        admin_email = Config.SOURCE_ADMIN_EMAIL
    else:
        flask_name  = "dest_credentials.json"
        config_path = Config.DEST_CREDENTIALS_FILE
        admin_email = Config.DEST_ADMIN_EMAIL

    p = _FLASK_CRED_DIR / flask_name
    if p.exists():
        creds_file = str(p)
    else:
        abs_p = Path(config_path)
        if not abs_p.is_absolute():
            abs_p = BACKEND_DIR / config_path
        if not abs_p.exists():
            raise FileNotFoundError(
                f"Credential not found at '{p}' or '{abs_p}'. "
                "Upload via /api/config."
            )
        creds_file = str(abs_p)

    creds = _sa.Credentials.from_service_account_file(
        creds_file,
        scopes=Config.SCOPES,
        subject=admin_email,   # DWD — impersonate the domain admin
    )

    try:
        import google_auth_httplib2 as _gah
        http      = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=1800))
        drive_svc = _gapi_build("drive", "v3", http=http, cache_discovery=False)
    except ImportError:
        drive_svc = _gapi_build(
            "drive", "v3", credentials=creds, cache_discovery=False
        )

    logger.info(
        f"[sd-storage] Drive service built | side={side} | "
        f"admin={admin_email} | creds={creds_file}"
    )
    return drive_svc, admin_email


# ─────────────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────────────

def _drive_error(
    drive_id: str,
    drive_name: str,
    msg: str,
    was_temp: bool = False,
) -> dict:
    return {
        "drive_id":        drive_id,
        "drive_name":      drive_name,
        "total_bytes":     None,
        "total_gb":        None,
        "file_count":      None,
        "was_temp_member": was_temp,
        "error":           msg,
    }


def _empty_result(drive_id: str) -> dict:
    """Returned when a drive_id is blank in the pair."""
    return {
        "drive_id":        drive_id,
        "drive_name":      "",
        "total_bytes":     None,
        "total_gb":        None,
        "file_count":      None,
        "was_temp_member": False,
        "error":           "No drive_id provided",
    }


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
