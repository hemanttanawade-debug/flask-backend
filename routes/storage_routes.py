"""
Storage logic — three-tier approach:

TIER 1 — Admin Reports API (accounts:drive_used_quota_in_mb)
  Tries dates 2–7 days back. Fast and cheap but has a 24–48 hr data lag.
  Returns None when the user has never triggered a report (new accounts,
  inactive users, or the domain simply hasn't populated data yet).

TIER 2 — Admin Directory API users().get() fallback
  Called only when Reports API returns no data for a user.
  Impersonates SOURCE_ADMIN_EMAIL / DEST_ADMIN_EMAIL (same subject used
  by the Reports service — no per-user DWD required).
  Reads quota.storageUsed from the Directory API user resource — zero
  lag, always accurate.

  Requires the service account DWD grant to include:
    https://www.googleapis.com/auth/admin.directory.user.readonly
  and the admin subject to be a super admin of the user's domain.

  NOTE: This will 403 if the admin does not have directory rights over the
  target user's domain (e.g. cross-domain users, restricted DWD scope).

TIER 3 — Drive v3 about().get() per-user fallback
  Called only when both Tier 1 and Tier 2 fail (e.g. 403 on Directory API).
  Impersonates the individual user via DWD — requires the service account
  DWD grant to include https://www.googleapis.com/auth/drive.readonly (or
  https://www.googleapis.com/auth/drive.metadata.readonly).
  Returns storageQuota.usage (bytes) from the Drive about() response.

  NOTE: This approach works for cross-domain users as long as the service
  account's DWD covers the target user. It will fail with
  `unauthorized_client` only if DWD is NOT granted for that user/domain.

AUTH (mirrors shared_drive_storage_routes._build_drive_service):
  Tier 1+2 services impersonate SOURCE_ADMIN_EMAIL / DEST_ADMIN_EMAIL.
  Tier 3 impersonates the individual end-user. All use DWD from the same
  service account credentials file.
"""

import csv
import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

storage_bp      = Blueprint("storage", __name__)
BACKEND_DIR     = Path.home() / "amey"
_FLASK_CRED_DIR = Path.home() / "flask-backend" / "uploads" / "credential"
_MB_PER_GB      = 1024.0           # Reports API returns MB
_BYTES_PER_GB   = 1_073_741_824.0  # Drive about() / Directory API return bytes

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Route
# ─────────────────────────────────────────────────────────────────────────────

@storage_bp.route("/storage-sizes", methods=["POST"])
@require_auth
def get_storage_sizes():
    body = request.get_json(silent=True) or {}
    sid  = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    side  = (body.get("side") or "source").strip().lower()
    users = body.get("users") or []

    if side not in ("source", "destination"):
        return jsonify({
            "error": f"Invalid side '{side}'. Must be 'source' or 'destination'."
        }), 400
    
    # ── Auto-refresh admin pool whenever destination storage is fetched ──
    if side == "destination":
        try:
            fetch_and_store_dest_super_admins()
        except Exception as e:
            current_app.logger.warning(f"[admin-pool] Auto-refresh skipped: {e}")

    if not users:
        users = _read_users_from_csv(side)
        if not users:
            return jsonify({"sizes": {}})

    if not BACKEND_DIR.exists():
        return jsonify({"error": "Backend repo not found at ~/amey"}), 500

    _ensure_backend_on_path()

    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    try:
        sizes = _fetch_storage_sizes(side, users)
        return jsonify({"sizes": sizes})
    except Exception as e:
        current_app.logger.error(f"[storage-sizes] {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_storage_sizes(side: str, users: list) -> dict:
    """
    For each user, tries three tiers in order:
      1. Reports API  — fast, admin-impersonation, 24–48 hr lag
      2. Directory API — zero lag, admin-impersonation, needs directory scope
      3. Drive about() — zero lag, per-user impersonation, needs drive scope

    Falls through to the next tier only on empty data or 403.
    """
    try:
        reports_svc, admin_email = _build_reports_service(side)
    except Exception as exc:
        logger.error(f"[storage-sizes] Reports service error: {exc}")
        raise

    creds_file = _resolve_creds_file(side)

    logger.info(
        f"[storage-sizes] side={side} | admin={admin_email} | "
        f"users={len(users)}"
    )

    sizes = {}
    for email in users:
        email = email.strip().lower()
        if not email:
            continue

        # ── Tier 1: Reports API ───────────────────────────────────────────────
        storage = _get_user_storage_via_reports(reports_svc, email)

        _no_data = storage.get("drive_gb") is None
        _soft_miss = storage.get("error") in (
            None,
            "No data in Reports API yet (24–48 hr lag)",
        )

        # ── Tier 2: Directory API — only on soft miss (no hard 403/404) ───────
        if _no_data and _soft_miss:
            logger.info(
                f"[storage-sizes] {email} — Reports API empty, "
                "trying Admin Directory API users().get()"
            )
            storage = _get_user_storage_via_directory(creds_file, admin_email, email)

        # ── Tier 3: Drive about() — fallback when Directory API 403s ──────────
        # Triggered when Directory API returned an error (e.g. 403 forbidden,
        # cross-domain user, or missing directory scope in DWD grant).
        if storage.get("drive_gb") is None:
            logger.info(
                f"[storage-sizes] {email} — Directory API failed "
                f"({storage.get('error')}), "
                "falling back to Drive v3 about().get() per-user impersonation"
            )
            storage = _get_user_storage_via_drive_about(creds_file, email)

        sizes[email] = storage
        current_app.logger.info(
            f"[storage-sizes] {side} | {email} → "
            f"drive_gb={storage.get('drive_gb')} "
            f"source={storage.get('source')} "
            f"error={storage.get('error')}"
        )

    current_app.logger.info(
        f"[storage-sizes] {side} | fetched {len(sizes)} user(s)"
    )
    return sizes


# ─────────────────────────────────────────────────────────────────────────────
# Tier 1: Admin Reports API
# ─────────────────────────────────────────────────────────────────────────────

def _get_user_storage_via_reports(reports_svc, email: str) -> dict:
    """
    Tries dates 2–7 days back via Admin Reports API.
    Returns the first date that has data, or drive_gb=None on miss.
    Dict shape: { drive_gb, date, source, error }
    """
    params = "accounts:drive_used_quota_in_mb"
    now    = datetime.now(timezone.utc)
    dates  = [
        (now - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(2, 8)
    ]

    for date_str in dates:
        try:
            resp = (
                reports_svc
                .userUsageReport()
                .get(userKey=email, date=date_str, parameters=params)
                .execute()
            )

            reports = resp.get("usageReports", [])
            if not reports:
                continue

            param_list = reports[0].get("parameters", [])
            if not param_list:
                continue

            drive_mb = 0
            for p in param_list:
                name = (p.get("name") or "").lower()
                val  = int(p.get("intValue") or p.get("value") or 0)
                if "drive_used_quota" in name:
                    drive_mb = val
                    break

            return {
                "drive_gb": round(drive_mb / _MB_PER_GB, 3),
                "date":     date_str,
                "source":   "reports_api",
                "error":    None,
            }

        except Exception as e:
            err_str = str(e)
            if "403" in err_str or "404" in err_str:
                # Hard stop — no point trying other dates
                return _storage_error(email, err_str, source="reports_api")
            current_app.logger.debug(
                f"[storage-sizes] Reports | {email} date={date_str} → {err_str}"
            )
            continue

    # No data across all dates — caller will try Directory API then Drive about()
    current_app.logger.warning(
        f"[storage-sizes] Reports API: no data for {email} "
        f"(checked {dates[0]} – {dates[-1]}, 24–48 hr lag)"
    )
    return {
        "drive_gb": None,
        "date":     None,
        "source":   "reports_api",
        "error":    "No data in Reports API yet (24–48 hr lag)",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2: Admin Directory API users().get() — impersonate the domain admin
# ─────────────────────────────────────────────────────────────────────────────

def _get_user_storage_via_directory(
    creds_file: str, admin_email: str, email: str
) -> dict:
    """
    Fetch a user's storage quota via the Admin Directory API users().get().

    WHY NOT Drive about().get() impersonating the user here?
    ─────────────────────────────────────────────────────────
    The Drive about() approach requires subject=<user_email>, which needs
    per-user DWD. Directory API uses the admin subject throughout.

    WHEN THIS FAILS (403):
    ──────────────────────
    - Service account DWD grant doesn't include directory scope, OR
    - admin_email is not a super admin of the target user's domain (cross-domain).
    In either case, drive_gb=None is returned and Tier 3 takes over.

    Requires DWD scope: https://www.googleapis.com/auth/admin.directory.user.readonly
    """
    try:
        dir_svc = _build_directory_service(creds_file, admin_email)
        user    = dir_svc.users().get(
            userKey=email,
            projection="full",
        ).execute()

        quota       = user.get("quota", {})
        usage_bytes = int(quota.get("storageUsed") or 0)
        drive_gb    = round(usage_bytes / _BYTES_PER_GB, 3)

        current_app.logger.info(
            f"[storage-sizes] Directory API | {email} → "
            f"storageUsed={usage_bytes} bytes ({drive_gb} GB)"
        )
        return {
            "drive_gb": drive_gb,
            "date":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "source":   "directory_api",
            "error":    None,
        }

    except Exception as exc:
        err_str = str(exc)
        current_app.logger.error(
            f"[storage-sizes] Directory API fallback failed for {email}: {err_str}"
        )
        # Return error dict with source="directory_api" so Tier 3 can detect it
        return {
            "drive_gb": None,
            "date":     None,
            "source":   "directory_api",
            "error":    err_str,
        }


def _build_directory_service(creds_file: str, admin_email: str):
    """
    Build an Admin Directory v1 service impersonating `admin_email`.
    Uses subject=admin_email — identical to the Reports service approach,
    so no additional DWD grants are required beyond what Reports already uses.
    """
    import httplib2
    from config import Config
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    creds = _sa.Credentials.from_service_account_file(
        creds_file,
        scopes=Config.SCOPES,
        subject=admin_email,
    )

    try:
        import google_auth_httplib2 as _gah
        http    = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=300))
        dir_svc = _gapi_build("admin", "directory_v1", http=http, cache_discovery=False)
    except ImportError:
        dir_svc = _gapi_build(
            "admin", "directory_v1", credentials=creds, cache_discovery=False
        )

    return dir_svc


# ─────────────────────────────────────────────────────────────────────────────
# Tier 3: Drive v3 about().get() — impersonate the individual user
# ─────────────────────────────────────────────────────────────────────────────

def _get_user_storage_via_drive_about(creds_file: str, email: str) -> dict:
    """
    Fetch a user's Drive storage by impersonating them via DWD and calling
    Drive v3 about().get(fields="storageQuota").

    WHEN TO USE:
    ────────────
    Called as Tier 3 when both Reports API and Directory API have failed.
    Typical scenario: cross-domain users (e.g. admin is @domain-A but user is
    @domain-B), or the service account's DWD grant lacks the directory scope
    but does include the Drive scope.

    REQUIREMENTS:
    ─────────────
    - Service account DWD must grant impersonation for the target user's domain.
    - Scope needed: https://www.googleapis.com/auth/drive.metadata.readonly
      (or broader drive/drive.readonly — already in Config.SCOPES for most setups)

    WHAT IT RETURNS:
    ────────────────
    storageQuota.usage — total bytes used across Drive + Gmail + Photos.
    Same value shown in Google Account storage bar.

    FAILURE:
    ────────
    Returns drive_gb=None with source="drive_about" if DWD is not granted
    for this user (unauthorized_client) or any other error occurs.
    """
    try:
        drive_svc = _build_drive_about_service(creds_file, email)
        about     = drive_svc.about().get(fields="storageQuota").execute()

        quota       = about.get("storageQuota", {})
        usage_bytes = int(quota.get("usage") or 0)
        drive_gb    = round(usage_bytes / _BYTES_PER_GB, 3)

        current_app.logger.info(
            f"[storage-sizes] Drive about() | {email} → "
            f"usage={usage_bytes} bytes ({drive_gb} GB)"
        )
        return {
            "drive_gb": drive_gb,
            "date":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "source":   "drive_about",
            "error":    None,
        }

    except Exception as exc:
        err_str = str(exc)
        current_app.logger.error(
            f"[storage-sizes] Drive about() fallback failed for {email}: {err_str}"
        )
        return _storage_error(email, err_str, source="drive_about")


def _build_drive_about_service(creds_file: str, user_email: str):
    """
    Build a Drive v3 service impersonating `user_email` (not the admin).
    subject=user_email — requires DWD to cover the user's domain.
    """
    import httplib2
    from config import Config
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    creds = _sa.Credentials.from_service_account_file(
        creds_file,
        scopes=Config.SCOPES,
        subject=user_email,   # DWD — impersonate the individual user
    )

    try:
        import google_auth_httplib2 as _gah
        http      = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=300))
        drive_svc = _gapi_build("drive", "v3", http=http, cache_discovery=False)
    except ImportError:
        drive_svc = _gapi_build(
            "drive", "v3", credentials=creds, cache_discovery=False
        )

    return drive_svc


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers — mirror shared_drive_storage_routes._build_drive_service()
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_creds_file(side: str) -> str:
    """
    Resolve the service-account JSON path.
    Same two-step fallback as shared_drive_storage_routes:
      1. ~/flask-backend/uploads/credential/{source|dest}_credentials.json
      2. Config.SOURCE_CREDENTIALS_FILE / DEST_CREDENTIALS_FILE
    """
    from config import Config

    flask_name  = "source_credentials.json" if side == "source" else "dest_credentials.json"
    config_path = Config.SOURCE_CREDENTIALS_FILE if side == "source" else Config.DEST_CREDENTIALS_FILE

    p = _FLASK_CRED_DIR / flask_name
    if p.exists():
        return str(p)

    abs_p = Path(config_path)
    if not abs_p.is_absolute():
        abs_p = BACKEND_DIR / config_path
    if not abs_p.exists():
        raise FileNotFoundError(
            f"Credential not found at '{p}' or '{abs_p}'. "
            "Upload via /api/config."
        )
    return str(abs_p)


def _build_reports_service(side: str):
    """
    Build Admin Reports v1 service impersonating SOURCE_ADMIN_EMAIL or
    DEST_ADMIN_EMAIL via DWD. Returns (reports_svc, admin_email).
    """
    import httplib2
    from config import Config
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    admin_email = Config.SOURCE_ADMIN_EMAIL if side == "source" else Config.DEST_ADMIN_EMAIL
    creds_file  = _resolve_creds_file(side)

    creds = _sa.Credentials.from_service_account_file(
        creds_file,
        scopes=Config.SCOPES,
        subject=admin_email,
    )

    try:
        import google_auth_httplib2 as _gah
        http        = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=1800))
        reports_svc = _gapi_build("admin", "reports_v1", http=http, cache_discovery=False)
    except ImportError:
        reports_svc = _gapi_build(
            "admin", "reports_v1", credentials=creds, cache_discovery=False
        )

    logger.info(
        f"[storage-sizes] Reports service | side={side} | "
        f"admin={admin_email} | creds={creds_file}"
    )
    return reports_svc, admin_email


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _storage_error(email: str, msg: str, source: str = "unknown") -> dict:
    current_app.logger.warning(f"[storage-sizes] {email} ({source}) → {msg}")
    return {
        "drive_gb": None,
        "date":     None,
        "source":   source,
        "error":    msg,
    }


def _read_users_from_csv(side: str) -> list:
    csv_path = state.CSV_PATH
    if not csv_path.exists():
        current_app.logger.warning("[storage-sizes] uploads/users.csv not found")
        return []

    try:
        content = csv_path.read_text(encoding="utf-8-sig")
        reader  = csv.DictReader(io.StringIO(content))
        col     = "source" if side == "source" else "destination"
        emails  = []
        for row in reader:
            norm  = {k.strip().lower(): v.strip() for k, v in row.items()}
            email = norm.get(col, "")
            if email:
                emails.append(email.lower())
        current_app.logger.info(
            f"[storage-sizes] Read {len(emails)} {side} users from CSV"
        )
        return emails
    except Exception as e:
        current_app.logger.error(f"[storage-sizes] CSV parse error: {e}")
        return []


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)

# ─────────────────────────────────────────────────────────────────────────────
# Admin Pool — fetch & persist destination super admins
# ─────────────────────────────────────────────────────────────────────────────

ADMIN_FILE = Path.home() / "flask-backend" / "uploads" / "admin"

def fetch_and_store_dest_super_admins() -> list[str]:
    """
    Fetches all active Super Admins from the DESTINATION domain using
    the dest service-account credentials (delegated to dest admin email).
    Overwrites uploads/admin with the result.
    Returns the list of super-admin emails.

    Reuses _resolve_creds_file("destination") — same credential resolution
    logic already used by storage tiers 1/2/3 in this file.
    """
    _ensure_backend_on_path()

    from config import Config
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    creds_file  = _resolve_creds_file("destination")
    admin_email = (
        state.config.get("dest_admin_email") or Config.DEST_ADMIN_EMAIL
    )

    if not admin_email:
        raise ValueError("Destination admin email not configured.")

    ADMIN_SCOPE = ["https://www.googleapis.com/auth/admin.directory.user.readonly"]

    creds     = _sa.Credentials.from_service_account_file(creds_file, scopes=ADMIN_SCOPE)
    delegated = creds.with_subject(admin_email)

    try:
        import httplib2, google_auth_httplib2 as _gah
        http    = _gah.AuthorizedHttp(delegated, http=httplib2.Http(timeout=300))
        service = _gapi_build("admin", "directory_v1", http=http, cache_discovery=False)
    except ImportError:
        service = _gapi_build("admin", "directory_v1", credentials=delegated, cache_discovery=False)

    super_admins = []
    page_token   = None

    while True:
        resp = service.users().list(
            customer="my_customer",
            maxResults=500,
            pageToken=page_token,
            query="isAdmin=true isSuspended=false isArchived=false",
        ).execute()

        for user in resp.get("users", []):
            # Double-check in memory — mirrors Apps Script safety net
            if user.get("isAdmin") and not user.get("suspended") and not user.get("archived"):
                super_admins.append(user["primaryEmail"])

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # Persist in the same format the migration engine reads
    ADMIN_FILE.parent.mkdir(parents=True, exist_ok=True)
    ADMIN_FILE.write_text(
        "admin={" + ",".join(super_admins) + "}\n",
        encoding="utf-8",
    )

    logger.info(
        f"[admin-pool] Wrote {len(super_admins)} super admin(s) → {ADMIN_FILE}"
    )
    return super_admins


@storage_bp.route("/admin/refresh", methods=["POST"])
@require_auth
def refresh_admins():
    """
    POST /api/admin/refresh
    Re-fetches active dest super admins and overwrites uploads/admin.
    Called automatically before migration starts, or manually from UI.
    """
    _ensure_backend_on_path()
    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    try:
        admins = fetch_and_store_dest_super_admins()
        current_app.logger.info(
            f"[admin-pool] Refreshed: {len(admins)} super admin(s) written."
        )
        return jsonify({"success": True, "count": len(admins), "admins": admins})
    except Exception as e:
        current_app.logger.error(f"[admin-pool] Refresh failed: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
