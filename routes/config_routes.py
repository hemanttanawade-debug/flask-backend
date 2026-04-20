"""
routes/config_routes.py

  POST /api/config/new        — start a new wizard session (resets in-memory state only)
  POST /api/config            — save domain config + credential files (first time)
  PUT  /api/config            — update domain config; credential files optional
  POST /api/validate          — test source/dest connectivity
  POST /api/migration-mode    — save chosen migration mode for this session
"""

import os
import sys
import uuid
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

config_bp   = Blueprint("config", __name__)
BACKEND_DIR = Path.home() / "amey"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/config/new
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/config/new", methods=["POST"])
@require_auth
def new_session():
    """
    Resets in-memory wizard state and returns a fresh sessionId.
    Credential files on disk are NOT deleted — they will be overwritten
    only when the user explicitly uploads new ones in Step 0.
    """
    sid = str(uuid.uuid4())
    state.new_session(sid, auto_delete_previous=False)
    return jsonify({
        "sessionId": sid,
        "message":   "New migration session started.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/config  — first-time save (credential files required if not on disk)
# PUT  /api/config  — update       (credential files optional; kept if not sent)
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/config", methods=["POST", "PUT"])
@require_auth
def save_config():
    """
    Accepts multipart/form-data:
      - sessionId                  (from /api/config/new)
      - sourceDomain, sourceAdminEmail
      - destinationDomain, destinationAdminEmail
      - sourceCredentials          (.json) → overwrites uploads/credential/source_credentials.json
      - destinationCredentials     (.json) → overwrites uploads/credential/dest_credentials.json

    POST  — credential files required if they don't already exist on disk.
    PUT   — credential files are optional; existing files on disk are kept as-is
            if no new file is uploaded (allows updating only domain/email fields).

    KEY FIX — ERR_UPLOAD_FILE_CHANGED:
      The browser throws this when it tries to re-read a file that changed on
      disk after the user selected it. We fix this by calling file.read() to
      pull ALL bytes into memory immediately, then writing from that buffer —
      so Flask never touches the original file handle again.

    Files are ALWAYS saved to the same fixed paths — never in session subfolders.
    The user's original filename is ignored.
    File contents are never logged or returned.
    """
    is_update = request.method == "PUT"
    form      = request.form
    files     = request.files

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
        return jsonify({
            "success": False,
            "message": f"Missing fields: {', '.join(missing)}",
        }), 400

    # Persist domain / email fields in memory
    state.config["source_domain"]      = form["sourceDomain"]
    state.config["source_admin_email"] = form["sourceAdminEmail"]
    state.config["dest_domain"]        = form["destinationDomain"]
    state.config["dest_admin_email"]   = form["destinationAdminEmail"]

    # ── Handle credential files ───────────────────────────────────────────────
    for field, kind in [
        ("sourceCredentials",      "source"),
        ("destinationCredentials", "dest"),
    ]:
        file       = files.get(field)
        fixed_path = (
            state.SOURCE_CREDENTIALS_PATH if kind == "source"
            else state.DEST_CREDENTIALS_PATH
        )

        if file and file.filename:
            # ── Read ALL bytes into memory immediately ────────────────────────
            # Fix for ERR_UPLOAD_FILE_CHANGED: once bytes are in a Python
            # variable the browser file handle is no longer needed — we write
            # from our own in-memory buffer, immune to on-disk changes.
            try:
                file_bytes = file.read()
            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": (
                        f"Could not read {field} — the file may have changed "
                        f"after you selected it. Please re-select it and try "
                        f"again. ({e})"
                    ),
                }), 400

            if len(file_bytes) == 0:
                return jsonify({
                    "success": False,
                    "message": f"{field}: uploaded file is empty.",
                }), 400

            # Write from in-memory buffer — immune to subsequent on-disk changes
            try:
                state.CREDENTIAL_DIR.mkdir(parents=True, exist_ok=True)
                fixed_path.write_bytes(file_bytes)
                os.chmod(str(fixed_path), 0o600)
            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": f"Failed to save {field}: {e}",
                }), 500

            state.config[f"{kind}_credentials_file"] = str(fixed_path)
            current_app.logger.info(
                f"[{sid}] {field} overwritten → "
                f"uploads/credential/{kind}_credentials.json"
            )

        elif fixed_path.exists():
            # No new file uploaded — re-use the existing file on disk.
            # On PUT this is expected; on POST it means the user skipped
            # re-uploading a file they already uploaded in a previous session.
            state.config[f"{kind}_credentials_file"] = str(fixed_path)

        elif not is_update:
            # POST with no file and nothing on disk — hard error.
            return jsonify({
                "success": False,
                "message": (
                    f"{field} is required — "
                    f"no existing file found at "
                    f"uploads/credential/{kind}_credentials.json. "
                    f"Please upload the credential file."
                ),
            }), 400

        # PUT with no file and nothing on disk — silently skip.
        # The caller is responsible for ensuring credentials exist before
        # proceeding to /api/validate.

    _apply_config_to_backend()

    return jsonify({
        "success":   True,
        "sessionId": sid,
        "message":   "Configuration updated." if is_update else "Configuration saved.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/validate
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/validate", methods=["POST"])
@require_auth
def validate_connection():
    """
    Accepts JSON body: { "sessionId": "<sid>" }
    Pings both domains using the credential files at their fixed paths.
    """
    body = request.get_json(silent=True) or {}
    sid  = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    if not _backend_available():
        return jsonify({
            "source":      False,
            "destination": False,
            "errors":      ["Backend repo not found."],
        }), 500

    # Check fixed credential files exist before attempting auth
    creds = state.credentials_exist()
    if not creds["source"] or not creds["dest"]:
        missing = []
        if not creds["source"]: missing.append("source_credentials.json")
        if not creds["dest"]:   missing.append("dest_credentials.json")
        return jsonify({
            "source":      creds["source"],
            "destination": creds["dest"],
            "errors":      [f"Missing credential file(s): {', '.join(missing)}"],
        }), 400

    errors = []
    source_ok = False
    dest_ok   = False

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

    return jsonify({
        "source":      source_ok,
        "destination": dest_ok,
        "errors":      errors,
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration-mode
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/migration-mode", methods=["POST"])
@require_auth
def save_migration_mode():
    """
    Accepts JSON body: { "sessionId": "<sid>", "mode": "full|custom|shared-drives|resume" }
    Persists the chosen mode into state so /api/migrate can read it without
    the frontend needing to send it again.
    """
    body = request.get_json(silent=True) or {}
    sid  = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    mode = body.get("mode", "").strip()
    valid_modes = {"full", "custom", "shared-drives", "resume"}
    if mode not in valid_modes:
        return jsonify({
            "success": False,
            "message": (
                f"Invalid mode '{mode}'. "
                f"Must be one of: {', '.join(sorted(valid_modes))}"
            ),
        }), 400

    state.config["migration_mode"] = mode
    current_app.logger.info(f"[{sid}] Migration mode set to: {mode}")

    return jsonify({
        "success": True,
        "mode":    mode,
        "message": f"Migration mode '{mode}' saved.",
    })


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
    Override hardcoded values in amey/config.py at runtime.
    Falls back to fixed disk paths for credentials if in-memory state
    was wiped by a gunicorn worker restart.
    """
    if not _backend_available():
        return
    _ensure_backend_on_path()
    try:
        import config as bc
        cfg = bc.Config

        # ── Credential paths: use memory value if set, else fall back to
        #    the fixed absolute paths on disk. This survives worker restarts.
        src_creds = (
            state.config.get("source_credentials_file")
            or str(state.SOURCE_CREDENTIALS_PATH)
        )
        dst_creds = (
            state.config.get("dest_credentials_file")
            or str(state.DEST_CREDENTIALS_PATH)
        )

        # Always set credential paths — even if domain fields are empty
        cfg.SOURCE_CREDENTIALS_FILE = src_creds
        cfg.DEST_CREDENTIALS_FILE   = dst_creds

        if state.config.get("source_domain"):
            cfg.SOURCE_DOMAIN      = state.config["source_domain"]
            cfg.SOURCE_ADMIN_EMAIL = state.config["source_admin_email"]

        if state.config.get("dest_domain"):
            cfg.DEST_DOMAIN      = state.config["dest_domain"]
            cfg.DEST_ADMIN_EMAIL = state.config["dest_admin_email"]

    except Exception as e:
        current_app.logger.warning(f"Config patch failed: {e}")
