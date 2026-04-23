"""
routes/config_routes.py

  POST /api/config/new        — no-op if session exists; creates one only on first boot / after reset
  POST /api/config            — save domain config + credential files
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
    Returns the current session_id (or creates one if none exists yet).

    CHANGED: this route no longer resets domain/email/CSV/credential state.
    State persists across page refreshes and VM restarts.
    The only way to clear state is DELETE /api/reset.
    """
    state.new_session()   # no-op if session already exists
    return jsonify({
        "sessionId": state.session_id,
        "message":   "Session ready.",
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
      - sessionId                  (ignored — current session is always used)
      - sourceDomain, sourceAdminEmail
      - destinationDomain, destinationAdminEmail
      - sourceCredentials          (.json) → overwrites uploads/credential/source_credentials.json
      - destinationCredentials     (.json) → overwrites uploads/credential/dest_credentials.json

    POST  — credential files required if they don't already exist on disk.
    PUT   — credential files are optional; existing files on disk are kept as-is.

    All changes are immediately persisted to uploads/session.json so they
    survive a VM restart or gunicorn reload.
    """
    is_update = request.method == "PUT"
    form      = request.form
    files     = request.files

    # Ensure a session exists (idempotent)
    state.new_session()

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

    # Update domain / email fields in memory
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
            # Read ALL bytes into memory immediately to avoid ERR_UPLOAD_FILE_CHANGED
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
                f"[{state.session_id}] {field} overwritten → "
                f"uploads/credential/{kind}_credentials.json"
            )

        elif fixed_path.exists():
            state.config[f"{kind}_credentials_file"] = str(fixed_path)

        elif not is_update:
            return jsonify({
                "success": False,
                "message": (
                    f"{field} is required — "
                    f"no existing file found at "
                    f"uploads/credential/{kind}_credentials.json. "
                    f"Please upload the credential file."
                ),
            }), 400

    # ── Persist all changes to disk ───────────────────────────────────────────
    state._persist()
    # ─────────────────────────────────────────────────────────────────────────

    _apply_config_to_backend()

    return jsonify({
        "success":   True,
        "sessionId": state.session_id,
        "message":   "Configuration updated." if is_update else "Configuration saved.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/validate
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/validate", methods=["POST"])
@require_auth
def validate_connection():
    """
    Accepts JSON body: { "sessionId": "<sid>" }  (sessionId is now ignored)
    Pings both domains using the credential files at their fixed paths.
    """
    if not _backend_available():
        return jsonify({
            "source":      False,
            "destination": False,
            "errors":      ["Backend repo not found."],
        }), 500

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

    errors    = []
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
    Accepts JSON body: { "mode": "full|custom|shared-drives|resume" }
    Persists the chosen mode into state AND to disk.
    """
    body = request.get_json(silent=True) or {}

    state.new_session()   # idempotent

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

    # update_config persists to disk automatically
    state.update_config(migration_mode=mode)
    current_app.logger.info(f"[{state.session_id}] Migration mode set to: {mode}")

    return jsonify({
        "success": True,
        "mode":    mode,
        "message": f"Migration mode '{mode}' saved.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/config/current  — frontend can read back persisted state
# ─────────────────────────────────────────────────────────────────────────────

@config_bp.route("/config/current", methods=["GET"])
@require_auth
def get_current_config():
    """
    Returns the current persisted wizard state (no credential contents).
    Useful for the frontend to re-hydrate forms after a page reload.
    """
    return jsonify({
        "sessionId":            state.session_id,
        "sourceDomain":         state.config.get("source_domain",      ""),
        "sourceAdminEmail":     state.config.get("source_admin_email", ""),
        "destinationDomain":    state.config.get("dest_domain",        ""),
        "destinationAdminEmail":state.config.get("dest_admin_email",   ""),
        "migrationMode":        state.config.get("migration_mode",     "full"),
        "lastDiscoveryRunId":   state.config.get("last_discovery_run_id", ""),
        "sourceCredExists":     state.SOURCE_CREDENTIALS_PATH.exists(),
        "destCredExists":       state.DEST_CREDENTIALS_PATH.exists(),
        "csvExists":            state.CSV_PATH.exists(),
        "migrationActive":      state.is_migration_active(),
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

        src_creds = (
            state.config.get("source_credentials_file")
            or str(state.SOURCE_CREDENTIALS_PATH)
        )
        dst_creds = (
            state.config.get("dest_credentials_file")
            or str(state.DEST_CREDENTIALS_PATH)
        )

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
