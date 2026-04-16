"""
routes/config_routes.py

  POST /api/config/new  — start a new wizard session, auto-delete previous
  POST /api/config      — save domain config + upload credential files
  POST /api/validate    — test source/dest connectivity
"""

import sys
import uuid
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

config_bp  = Blueprint("config", __name__)
BACKEND_DIR = Path(__file__).parent.parent.parent / "enterprise-migration"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/config/new
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/config/new", methods=["POST"])
@require_auth  
def new_session():
    """
    Call when user clicks 'Start New Migration'.
    - Generates a new session_id
    - Auto-deletes the previous session's uploaded files from disk
    - Resets all wizard state
    """
    sid = str(uuid.uuid4())
    deleted = state.new_session(sid, auto_delete_previous=True)
    return jsonify({
        "sessionId":    sid,
        "deletedFiles": deleted,
        "message":      "New migration session started. Previous files cleaned up.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/config
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/config", methods=["POST"])
@require_auth 
def save_config():
    """
    Accepts multipart/form-data:
      - sessionId                  (from /api/config/new)
      - sourceDomain, sourceAdminEmail
      - destinationDomain, destinationAdminEmail
      - sourceCredentials          (any .json file — saved as source_credentials.json)
      - destinationCredentials     (any .json file — saved as dest_credentials.json)

    The user's original filename is irrelevant — files are always saved
    under fixed names inside uploads/<sessionId>/:
        source_credentials.json
        dest_credentials.json

    Files are saved with chmod 0o600. Contents never logged or returned.
    """
    form  = request.form
    files = request.files

    sid = form.get("sessionId") or state.session_id
    if not sid:
        sid = str(uuid.uuid4())
    if sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    required_fields = [
        "sourceDomain", "sourceAdminEmail",
        "destinationDomain", "destinationAdminEmail",
    ]
    missing = [f for f in required_fields if not form.get(f)]
    if missing:
        return jsonify({"success": False,
                        "message": f"Missing fields: {', '.join(missing)}"}), 400

    state.config["source_domain"]      = form["sourceDomain"]
    state.config["source_admin_email"] = form["sourceAdminEmail"]
    state.config["dest_domain"]        = form["destinationDomain"]
    state.config["dest_admin_email"]   = form["destinationAdminEmail"]

    session_folder = state.UPLOAD_DIR / sid

    # Save each credential file under its fixed name
    for field, fixed_name, state_key in [
        ("sourceCredentials",      "source_credentials.json", "source_credentials_file"),
        ("destinationCredentials", "dest_credentials.json",   "dest_credentials_file"),
    ]:
        file = files.get(field)
        if file and file.filename:
            saved = state.save_credential_file(file, fixed_name, session_folder)
            state.config[state_key] = saved
            # Log path only — never log file contents
            current_app.logger.info(f"[{sid}] {field} saved as {fixed_name}")
        elif not state.config.get(state_key):
            return jsonify({
                "success": False,
                "message": f"{field} is required on first save.",
            }), 400

    _apply_config_to_backend()

    return jsonify({
        "success":   True,
        "sessionId": sid,
        "message":   "Configuration saved.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/validate
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/validate", methods=["POST"])
@require_auth
def validate_connection():
    """
    Builds DomainAuthManager exactly as main.py does and pings both domains.
    """
    if not _backend_available():
        return jsonify({"source": False, "destination": False,
                        "errors": ["Backend repo not found."]}), 500

    if not state.config.get("source_credentials_file") or \
       not state.config.get("dest_credentials_file"):
        return jsonify({"source": False, "destination": False,
                        "errors": ["Upload credentials first (Step 1)."]}), 400

    errors = []; source_ok = False; dest_ok = False

    try:
        _ensure_backend_on_path()
        _apply_config_to_backend()

        from auth import DomainAuthManager
        from config import Config

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

        try:
            auth.source_auth.authenticate()
            auth.source_auth.get_drive_service().about().get(fields="user").execute()
            source_ok = True
        except Exception as e:
            errors.append(f"Source: {e}")

        try:
            auth.dest_auth.authenticate()
            auth.dest_auth.get_drive_service().about().get(fields="user").execute()
            dest_ok = True
        except Exception as e:
            errors.append(f"Destination: {e}")

    except Exception as e:
        errors.append(f"Error: {e}")

    return jsonify({"source": source_ok, "destination": dest_ok, "errors": errors})


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _backend_available() -> bool:
    return BACKEND_DIR.exists()

def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)

def _apply_config_to_backend():
    """
    Override hardcoded values in enterprise-migration/config.py at runtime.
    The file on disk is NEVER modified.
    """
    if not _backend_available():
        return
    _ensure_backend_on_path()
    try:
        import config as bc
        cfg = bc.Config
        if state.config["source_domain"]:
            cfg.SOURCE_DOMAIN           = state.config["source_domain"]
            cfg.SOURCE_ADMIN_EMAIL      = state.config["source_admin_email"]
            cfg.SOURCE_CREDENTIALS_FILE = state.config["source_credentials_file"]
        if state.config["dest_domain"]:
            cfg.DEST_DOMAIN           = state.config["dest_domain"]
            cfg.DEST_ADMIN_EMAIL      = state.config["dest_admin_email"]
            cfg.DEST_CREDENTIALS_FILE = state.config["dest_credentials_file"]
    except Exception as e:
        current_app.logger.warning(f"Config patch failed: {e}")