# routes/admin_routes.py

import re
from pathlib import Path
from flask import Blueprint, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

admin_bp = Blueprint("admin", __name__)

ADMIN_FILE = Path(__file__).parent.parent / "uploads" / "admin"


def fetch_and_store_dest_super_admins() -> list[str]:
    """
    Fetches all active Super Admins from the DESTINATION domain using
    the dest service-account credentials (delegated to dest admin email).
    Overwrites uploads/admin with the result.
    Returns the list of super-admin emails.
    """
    import sys
    backend_dir = str(Path.home() / "amey")
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    from googleapiclient.discovery import build
    from google.oauth2 import service_account

    dest_creds_file = (
        state.config.get("dest_credentials_file")
        or str(state.DEST_CREDENTIALS_PATH)
    )
    dest_admin_email = state.config.get("dest_admin_email")

    if not dest_creds_file or not Path(dest_creds_file).exists():
        raise FileNotFoundError("Destination credentials file not found.")
    if not dest_admin_email:
        raise ValueError("Destination admin email not configured.")

    SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        dest_creds_file, scopes=SCOPES
    )
    # Delegate to the dest domain admin so the SDK sees 'my_customer'
    delegated = creds.with_subject(dest_admin_email)
    service   = build("admin", "directory_v1", credentials=delegated)

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
            # Double-check in memory — same safety net as your Apps Script
            if user.get("isAdmin") and not user.get("suspended") and not user.get("archived"):
                super_admins.append(user["primaryEmail"])

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # Write back to uploads/admin in the same format the engine reads
    ADMIN_FILE.parent.mkdir(parents=True, exist_ok=True)
    ADMIN_FILE.write_text(
        "admin={" + ",".join(super_admins) + "}\n",
        encoding="utf-8",
    )

    return super_admins


# ── Optional HTTP endpoint so the frontend can trigger a refresh ──────────────

@admin_bp.route("/admin/refresh", methods=["POST"])
@require_auth
def refresh_admins():
    """POST /api/admin/refresh — re-fetches dest super admins and updates uploads/admin."""
    try:
        admins = fetch_and_store_dest_super_admins()
        current_app.logger.info(f"Admin pool refreshed: {len(admins)} super admins written.")
        return jsonify({"success": True, "count": len(admins), "admins": admins})
    except Exception as e:
        current_app.logger.error(f"Admin refresh failed: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
