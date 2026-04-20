"""
routes/migration_routes.py

  POST /api/user-mapping             — upload CSV → always saved as uploads/users.csv
  POST /api/migrate                  — start migration in background thread
  POST /api/migration/<id>/retry     — retry failed files
  DELETE /api/migration/<id>/cleanup — resets in-memory state (files kept on disk)
"""

import csv
import io
import sys
import uuid
import copy
import threading
import traceback
from pathlib import Path
from flask import Blueprint, request, jsonify
from routes.auth_routes import require_auth

import session_state as state

migration_bp = Blueprint("migration", __name__)
BACKEND_DIR  = Path.home() / "amey"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/user-mapping
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/user-mapping", methods=["POST"])
@require_auth
def upload_user_mapping():
    """
    Accepts multipart/form-data:
      - file       (required) — CSV with 'source' and 'destination' columns
      - sessionId  (optional) — reconciles session if different from current

    Always saved as uploads/users.csv — overwrites any previous upload.
    The user's original filename is ignored.
    """
    sid = request.form.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "No file provided"}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are accepted"}), 400

    # Save to fixed path — overwrites previous CSV
    csv_path = state.save_csv_file(file)
    state.csv_file_path = csv_path

    try:
        content    = Path(csv_path).read_text(encoding="utf-8-sig")
        reader     = csv.DictReader(io.StringIO(content))
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]

        if "source" not in fieldnames or "destination" not in fieldnames:
            return jsonify({
                "error": "CSV must have 'source' and 'destination' columns."
            }), 400

        mappings = []
        for row in reader:
            norm = {k.strip().lower(): v.strip() for k, v in row.items()}
            src  = norm.get("source", "")
            dst  = norm.get("destination", "")
            if src or dst:
                mappings.append({"sourceUser": src, "destinationUser": dst})

        state.user_mappings = mappings
        return jsonify({"mappings": mappings})

    except Exception as e:
        return jsonify({"error": f"Failed to parse CSV: {e}"}), 500


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migrate
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migrate", methods=["POST"])
@require_auth
def start_migration():
    """
    Body (JSON): { "sessionId": "<sid>", "mode": "full|custom|shared-drives|resume",
                   "migrationId": "<id>"  ← optional, for resume }

    Mode falls back to state.config["migration_mode"] (saved by Step 3).
    """
    if state.migration["status"] == "running":
        return jsonify({"error": "A migration is already running."}), 409

    body = request.get_json(silent=True) or {}

    sid = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    # Prefer body mode → fall back to what Step 3 stored
    mode = (body.get("mode") or state.config.get("migration_mode") or "full").strip()

    resume_id    = body.get("migrationId")
    migration_id = resume_id or str(uuid.uuid4())

    state.migration.update({
        "migration_id":   migration_id,
        "session_id":     state.session_id,
        "status":         "running",
        "total_users":    len(state.user_mappings),
        "files_migrated": 0,
        "failed_files":   0,
        "logs":           [f"[INFO] Started — mode={mode}, id={migration_id}"],
    })

    thread = threading.Thread(
        target=_run_migration, args=(mode, migration_id, resume_id), daemon=True
    )
    thread.start()
    state._migration_thread = thread

    return jsonify({"migrationId": migration_id})


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration/<id>/retry
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/<migration_id>/retry", methods=["POST"])
@require_auth
def retry_failed(migration_id: str):
    if state.migration["status"] == "running":
        return jsonify({"success": False, "error": "Migration already running."}), 409

    state.migration.update({
        "status": "running",
        "logs":   state.migration.get("logs", []) + ["[INFO] Retrying failed files…"],
    })
    thread = threading.Thread(
        target=_run_migration, args=("resume", migration_id, migration_id), daemon=True
    )
    thread.start()
    return jsonify({"success": True})


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /api/migration/<id>/cleanup
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/<migration_id>/cleanup", methods=["DELETE"])
@require_auth
def cleanup(migration_id: str):
    """
    Resets in-memory migration state.
    Credential files are NOT deleted — they live at fixed paths and are
    only replaced by uploading new ones through Step 0.
    """
    if state.migration["status"] == "running" and \
       state.migration["migration_id"] == migration_id:
        return jsonify({
            "success": False,
            "error":   "Cannot reset while migration is running.",
        }), 409

    state.migration.update({
        "migration_id":   None,
        "session_id":     None,
        "status":         "idle",
        "total_users":    0,
        "files_migrated": 0,
        "failed_files":   0,
        "logs":           [],
    })

    return jsonify({
        "success": True,
        "message": "Migration state reset. Credential files retained on disk.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Background migration runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_migration(mode: str, migration_id: str, resume_id: str | None):
    """
    All config was already persisted across wizard steps — just read from
    state.config here. Credential files are at their fixed paths.
    """
    def log(msg: str):
        state.migration["logs"].append(msg)

    try:
        if str(BACKEND_DIR) not in sys.path:
            sys.path.insert(0, str(BACKEND_DIR))

        from routes.config_routes import _apply_config_to_backend
        _apply_config_to_backend()

        from config import Config
        from auth import DomainAuthManager
        from sql_state_manager import SQLStateManager

        log("[INFO] Authenticating…")
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
        auth.authenticate_all()

        sql_mgr = SQLStateManager(Config)

        if mode in ("full", "custom", "resume"):
            from migration_engine import MigrationEngine
            from users import UserManager

            engine = MigrationEngine(
                source_auth = auth.source_auth,
                dest_auth   = auth.dest_auth,
                config      = Config,
                checkpoint  = sql_mgr,
                gcs_helper  = sql_mgr,
                run_id      = migration_id,
                get_conn    = sql_mgr.get_conn,
            )

            if mode == "custom":
                csv_path = state.csv_file_path
                if not csv_path or not Path(csv_path).exists():
                    log("[ERROR] No CSV found at uploads/users.csv — upload a mapping file first.")
                    state.migration["status"] = "failed"
                    return

                src_svc  = auth.get_source_services()
                dst_svc  = auth.get_dest_services()
                user_mgr = UserManager(
                    src_svc["admin"], dst_svc["admin"],
                    Config.SOURCE_DOMAIN, Config.DEST_DOMAIN,
                )
                result       = user_mgr.import_mapping(csv_path)
                user_mapping = result.user_mapping
                state.migration["total_users"] = len(user_mapping)
                log(f"[INFO] {len(user_mapping)} user(s) loaded from uploads/users.csv")
                engine.migrate_users(user_mapping, run_id=migration_id)

            elif mode == "full":
                log("[INFO] Running full domain migration…")
                engine.migrate_all(run_id=migration_id)

            elif mode == "resume":
                log(f"[INFO] Resuming {resume_id}…")
                engine.resume(run_id=resume_id)

        elif mode == "shared-drives":
            from shared_drive_migrator import SharedDriveMigrator
            SharedDriveMigrator(auth, Config).migrate_all(run_id=migration_id)

        else:
            log(f"[ERROR] Unknown mode: {mode}")
            state.migration["status"] = "failed"
            return

        state.migration["status"] = "completed"
        log("[INFO] ✅ Migration completed successfully.")

    except Exception as e:
        state.migration["status"] = "failed"
        state.migration["failed_files"] += 1
        log(f"[ERROR] {e}")
        log(f"[TRACE] {traceback.format_exc()}")

    finally:
        # Snapshot to history — survives new migrations starting
        state.all_migrations[migration_id] = copy.deepcopy(state.migration)
