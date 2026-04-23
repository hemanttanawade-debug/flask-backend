"""
Storage logic mirrors the Apps Script getUserStorageViaReports():
  Admin Reports API: users/<email>/dates/<date>
  parameters=accounts:drive_used_quota_in_mb

Tries dates 2–7 days back (same as the script) because the Reports API
has a 24–48 hr data lag. Falls back gracefully on missing data.
"""

import csv
import io
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

storage_bp  = Blueprint("storage", __name__)
BACKEND_DIR = Path.home() / "amey"
_MB_PER_GB  = 1024.0       # Reports API returns MB


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
# Core: fetch Drive storage per user via Admin Reports API
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_storage_sizes(side: str, users: list) -> dict:
    from config import Config
    from auth import DomainAuthManager
    from googleapiclient.discovery import build

    auth = DomainAuthManager(
        source_config={
            "domain":           Config.SOURCE_DOMAIN,
            "credentials_file": Config.SOURCE_CREDENTIALS_FILE,
            "admin_email":      Config.SOURCE_ADMIN_EMAIL,
        },
        dest_config={
            "domain":           Config.DEST_DOMAIN,
            "credentials_file": Config.DEST_CREDENTIALS_FILE,
            "admin_email":      Config.DEST_ADMIN_EMAIL,
        },
        scopes=Config.SCOPES,
    )

    domain_auth = auth.source_auth if side == "source" else auth.dest_auth
    domain_auth.authenticate()

    # Build the Reports API service with delegated credentials
    reports_svc = build(
        "admin", "reports_v1",
        credentials=domain_auth.creds,
        cache_discovery=False,
    )

    sizes = {}
    for email in users:
        email = email.strip().lower()
        if not email:
            continue
        storage = _get_user_storage_via_reports(reports_svc, email)
        sizes[email] = storage
        current_app.logger.info(
            f"[storage-sizes] {side} | {email} → drive_gb={storage.get('drive_gb')}"
        )

    current_app.logger.info(
        f"[storage-sizes] {side} | fetched {len(sizes)} user(s)"
    )
    return sizes


def _get_user_storage_via_reports(reports_svc, email: str) -> dict:
    """
    Fetches Drive storage only via Admin Reports API.
    Tries dates 2–7 days back; returns the first date that has data.
    Returns dict: { "drive_gb": float|None, "date": str|None, "error": str|None }
    """
    params = "accounts:drive_used_quota_in_mb"

    now = datetime.now(timezone.utc)

    # Candidate date list: 2 to 7 days back (same as Apps Script)
    dates = [
        (now - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(2, 8)
    ]

    for date_str in dates:
        try:
            resp = (
                reports_svc
                .userUsageReport()
                .get(
                    userKey=email,
                    date=date_str,
                    parameters=params,
                )
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
                "error":    None,
            }

        except Exception as e:
            err_str = str(e)
            # 403 = no Reports API access; 404 = user not found — stop immediately
            if "403" in err_str or "404" in err_str:
                return _storage_error(email, err_str)
            # Any other error (e.g. 400 date out of range) — try next date
            current_app.logger.debug(
                f"[storage-sizes] {email} date={date_str} → {err_str}"
            )
            continue

    # No data found across all dates
    current_app.logger.warning(
        f"[storage-sizes] No Reports API data for {email} "
        f"(checked {dates[0]} – {dates[-1]}, likely <48hr lag)"
    )
    return {
        "drive_gb": None,
        "date":     None,
        "error":    "No data in Reports API yet (24–48 hr lag)",
    }


def _storage_error(email: str, msg: str) -> dict:
    current_app.logger.warning(f"[storage-sizes] {email} → {msg}")
    return {
        "drive_gb": None,
        "date":     None,
        "error":    msg,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helper: read emails from uploads/users.csv
# ─────────────────────────────────────────────────────────────────────────────

def _read_users_from_csv(side: str) -> list:
    csv_path = state.CSV_PATH
    if not csv_path.exists():
        current_app.logger.warning(
            "[storage-sizes] uploads/users.csv not found"
        )
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
