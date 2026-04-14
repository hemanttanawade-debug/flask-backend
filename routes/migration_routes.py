"""
routes/migration_routes.py

  POST /api/user-mapping             — upload CSV (any filename → saved as users.csv)
  POST /api/migrate                  — start migration in background thread
  POST /api/migration/<id>/retry     — retry failed files
  DELETE /api/migration/<id>/cleanup — delete this session's uploaded files
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

import session_state as state

migration_bp = Blueprint("migration", __name__)
BACKEND_DIR  = Path(__file__).parent.parent.parent / "enterprise-migration"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/user-mapping
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/user-mapping", methods=["POST"])
def upload_user_mapping():
    """
    Accepts any CSV file from the user.
    Saved as uploads/<sessionId>/users.csv regardless of original filename.
    Returns parsed preview for the frontend table.

    Expected columns: source, destination[, source_drive_id, dest_drive_id]
    """
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "No file provided"}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are accepted"}), 400

    sid         = state.session_id or "default"
    session_dir = state.UPLOAD_DIR / sid
    session_dir.mkdir(parents=True, exist_ok=True)

    # Always save as fixed name — user's filename is ignored
    csv_path = session_dir / "users.csv"
    file.save(str(csv_path))
    state.csv_file_path = str(csv_path)

    try:
        content    = csv_path.read_text(encoding="utf-8-sig")
        reader     = csv.DictReader(io.StringIO(content))
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]

        if "source" not in fieldnames or "destination" not in fieldnames:
            return jsonify({"error": "CSV must have 'source' and 'destination' columns."}), 400

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
def start_migration():
    """Body: { "mode": "full|custom|shared-drives|resume", "migrationId": "<id>" }"""
    if state.migration["status"] == "running":
        return jsonify({"error": "A migration is already running."}), 409

    body         = request.get_json(silent=True) or {}
    mode         = body.get("mode", "full")
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
def cleanup(migration_id: str):
    """
    Deletes uploaded files for the session tied to this migration.
    Use this when a migration is done and you want to start fresh.
    Safe to call even if files are already gone.
    """
    if state.migration["status"] == "running" and \
       state.migration["migration_id"] == migration_id:
        return jsonify({"success": False,
                        "error": "Cannot delete files while migration is running."}), 409

    # Find the session_id from history or current state
    sid = None
    if state.migration.get("migration_id") == migration_id:
        sid = state.migration.get("session_id")
    elif migration_id in state.all_migrations:
        sid = state.all_migrations[migration_id].get("session_id")

    if not sid:
        return jsonify({"success": False,
                        "error": "Migration ID not found or no files to delete."}), 404

    deleted = state.cleanup_session_files(sid)
    return jsonify({
        "success": True,
        "deleted": deleted,
        "message": f"Cleaned up files for session {sid}. Ready for new migration.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Background migration runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_migration(mode: str, migration_id: str, resume_id: str | None):
    """
    Mirrors main.py exactly:
      _build_auth_manager()     → DomainAuthManager(source_config, dest_config, scopes)
      _get_user_mapping()       → UserManager(src_admin, dst_admin, src_domain, dst_domain)
      _build_migration_engine() → MigrationEngine(source_auth, dest_auth, config,
                                    checkpoint, gcs_helper, run_id, get_conn)
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
                if not state.csv_file_path:
                    log("[ERROR] No CSV uploaded. Upload a user mapping file first.")
                    state.migration["status"] = "failed"
                    return

                src_svc  = auth.get_source_services()
                dst_svc  = auth.get_dest_services()
                user_mgr = UserManager(
                    src_svc["admin"], dst_svc["admin"],
                    Config.SOURCE_DOMAIN, Config.DEST_DOMAIN,
                )
                result       = user_mgr.import_mapping(state.csv_file_path)
                user_mapping = result.user_mapping
                state.migration["total_users"] = len(user_mapping)
                log(f"[INFO] {len(user_mapping)} user(s) loaded from users.csv")
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