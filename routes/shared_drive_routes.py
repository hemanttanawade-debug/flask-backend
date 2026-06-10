"""
routes/shared_drive_routes.py

Endpoints for Shared Drive discovery and migration.

  POST /api/shared-drive-mapping        — Upload/save source→dest drive ID mapping CSV
  GET  /api/shared-drive-mapping        — Return currently saved mapping
  POST /api/shared-drive/discover       — List all source Shared Drives (no SQL write)
  POST /api/shared-drive/migrate/start  — Start Shared Drive migration (background thread)
  POST /api/shared-drive/migrate/resume — Resume a previously started run
  GET  /api/shared-drive/migrate/status — Poll migration status (in-memory or SQL)
  GET  /api/shared-drive/migrate/stream — SSE stream (uses same token system as My Drive)

HOW IT WIRES INTO THE EXISTING STACK
──────────────────────────────────────
- SharedDriveMigrator is constructed here using the same Config, SQLStateManager,
  and credential resolution as migration_engine_v4.run_migration().
- The in-memory _sd_runs dict mirrors _runs in migration_routes.py so the same
  SSE streaming pattern works for Shared Drives.
- Temporary admin membership (v4 feature) is automatic inside
  SharedDriveMigrator.migrate_all_shared_drives() — this route does not need to
  know about it.
- The stream-token mechanism from migration_routes.py is NOT duplicated here;
  this route issues its own tokens via POST /api/shared-drive/migrate/stream-token.

FIXES IN THIS VERSION
──────────────────────
FIX-1  [stream-token 400 — no run_id available]
       _register_sd_run() now calls state.update_config(last_sd_discovery_run_id=run_id)
       so the run_id is persisted to session.json immediately.  After a gunicorn
       worker restart _resolve_run_id() can recover it without needing a runId in the
       request body.

FIX-2  [SIGSEGV — gunicorn worker killed]
       _check_worker_reset() detects a new worker PID on every request and clears
       the stale in-process _sd_runs dict so inherited file descriptors from the
       dead worker are never reused.  Callers: start, resume, stream-token, status.
       Combined with the gunicorn recommendation (--worker-class gthread --workers 1)
       this eliminates the fork-socket corruption.

FIX-3  [_sd_runs empty after worker restart — status / stream return 404]
       sd_migration_status() re-hydrates _sd_runs from SQL when the run is not in
       memory, so /status keeps working after a crash without requiring a restart.
       stream_sd_migration() does the same: if the token is valid but the run is
       absent from memory, it re-registers a skeleton entry and streams a synthetic
       "reconnected" phase event so the frontend doesn't hang.
"""

import csv
import io
import json
import logging
import os
import queue as _queue
import secrets
import sys
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Optional

from flask import Blueprint, Response, jsonify, request, stream_with_context
from routes.auth_routes import require_auth
import session_state as state

shared_drive_bp = Blueprint("shared_drive", __name__)
BACKEND_DIR     = Path.home() / "amey"

logger = logging.getLogger(__name__)

# ── In-memory run registry (mirrors migration_routes._runs) ───────────────────
_sd_runs: Dict[str, dict]  = {}
_sd_runs_lock               = threading.Lock()

# ── Stream token store ────────────────────────────────────────────────────────
_sd_stream_tokens: Dict[str, dict] = {}
_sd_stream_tokens_lock              = threading.Lock()

# ── Drive-ID mapping store (in-memory + persisted to uploads/sd_mapping.json) ─
_SD_MAPPING_FILE = Path(__file__).parent.parent / "uploads" / "sd_mapping.json"

# ── FIX-2: worker-PID guard ───────────────────────────────────────────────────
# Gunicorn forks new worker processes on crash.  The new worker inherits
# _sd_runs with stale Queue objects and dead httplib2 socket FDs from the
# previous worker.  Detecting a PID change lets us wipe that state cleanly
# before any request handler touches it.
_worker_pid: int = os.getpid()


def _check_worker_reset() -> None:
    """
    FIX-2: Detect gunicorn worker replacement and clear stale in-process state.

    Call at the top of every route handler that reads or writes _sd_runs.
    Cost: one os.getpid() call (~40 ns) — negligible.
    """
    global _worker_pid
    current = os.getpid()
    if current != _worker_pid:
        _worker_pid = current
        with _sd_runs_lock:
            _sd_runs.clear()
        with _sd_stream_tokens_lock:
            _sd_stream_tokens.clear()
        logger.warning(
            f"[sd-routes] New gunicorn worker pid={current} — "
            "cleared stale _sd_runs and _sd_stream_tokens"
        )


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive-mapping
# Save source_drive_id → dest_drive_id mapping (from CSV or JSON body)
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive-mapping", methods=["POST"])
@require_auth
def save_drive_mapping():
    """
    Accept a mapping of source Shared Drive IDs → destination Shared Drive IDs.

    Accepts TWO formats:
      a) Multipart CSV file upload — columns: source_drive_id, dest_drive_id
         (also accepts: source, destination — same columns as the user CSV)
      b) JSON body:
         { "mapping": { "src_id_1": "dst_id_1", ... } }
         or
         { "mapping": [ {"source_drive_id": "...", "dest_drive_id": "..."}, ... ] }

    Returns:
        { "success": true, "mapping": { src: dst, ... }, "total": N }
    """
    mapping: Dict[str, str] = {}

    # ── Path A: multipart file upload ─────────────────────────────────────────
    if request.files.get("file"):
        file = request.files["file"]
        if not file.filename.lower().endswith(".csv"):
            return jsonify({"error": "Only CSV files are accepted"}), 400
        try:
            content = file.read().decode("utf-8-sig")
            reader  = csv.DictReader(io.StringIO(content))
            fields  = [f.strip().lower() for f in (reader.fieldnames or [])]

            # Support both column naming conventions
            src_col = next(
                (f for f in fields if f in ("source_drive_id", "source", "source_id")),
                None,
            )
            dst_col = next(
                (f for f in fields if f in ("dest_drive_id", "destination", "dest_id", "destination_id")),
                None,
            )

            if not src_col or not dst_col:
                return jsonify({
                    "error": (
                        "CSV must have source and destination columns. "
                        "Accepted names: source_drive_id/source/source_id and "
                        "dest_drive_id/destination/dest_id/destination_id"
                    )
                }), 400

            for row in reader:
                norm = {k.strip().lower(): v.strip() for k, v in row.items()}
                src  = norm.get(src_col, "").strip()
                dst  = norm.get(dst_col, "").strip()
                if src and dst:
                    mapping[src] = dst

        except Exception as exc:
            return jsonify({"error": f"Failed to parse CSV: {exc}"}), 500

    # ── Path B: JSON body ─────────────────────────────────────────────────────
    else:
        body = request.get_json(silent=True) or {}
        raw  = body.get("mapping", {})

        if isinstance(raw, dict):
            mapping = {k.strip(): v.strip() for k, v in raw.items() if k and v}
        elif isinstance(raw, list):
            for item in raw:
                src = (item.get("source_drive_id") or item.get("source", "")).strip()
                dst = (item.get("dest_drive_id") or item.get("destination", "")).strip()
                if src and dst:
                    mapping[src] = dst
        else:
            return jsonify({"error": "'mapping' must be a dict or list"}), 400

    if not mapping:
        return jsonify({"error": "No valid drive ID pairs found in the request"}), 400

    # Persist to disk
    _save_sd_mapping(mapping)

    logger.info(f"[shared-drive-mapping] Saved {len(mapping)} drive pair(s)")
    return jsonify({"success": True, "mapping": mapping, "total": len(mapping)})


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/shared-drive-mapping
# Return the currently saved mapping
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive-mapping", methods=["GET"])
@require_auth
def get_drive_mapping():
    mapping = _load_sd_mapping()
    return jsonify({"mapping": mapping, "total": len(mapping)})


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/discover
# List all Shared Drives visible to the source admin (no SQL write)
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive/discover", methods=["POST"])
@require_auth
def discover_shared_drives():
    """
    List all Shared Drives in the source domain.
    Does NOT write to SQL — purely informational so the frontend can show
    the admin a list to confirm or build a mapping CSV.

    Returns:
        { "success": true, "drives": [ { "id", "name", "createdTime" }, ... ] }
    """
    _ensure_backend_on_path()
    try:
        from routes.config_routes import _apply_config_to_backend
        _apply_config_to_backend()
    except Exception:
        pass

    try:
        src_drive = _build_admin_drive_service(kind="source")
        drives    = []
        page_token = None

        while True:
            resp = src_drive.drives().list(
                pageSize=100,
                pageToken=page_token,
                fields="nextPageToken, drives(id, name, createdTime)",
                useDomainAdminAccess=True,
            ).execute()
            drives.extend(resp.get("drives", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        logger.info(f"[shared-drive/discover] Found {len(drives)} drives")
        return jsonify({"success": True, "drives": drives, "total": len(drives)})

    except Exception as exc:
        logger.exception(f"[shared-drive/discover] {exc}")
        return jsonify({
            "success": False,
            "message": str(exc),
            "detail":  traceback.format_exc()[-2000:],
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/migrate/start
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive/migrate/start", methods=["POST"])
@require_auth
def start_shared_drive_migration():
    """
    Start a Shared Drive migration run.

    Body (JSON):
        {
            "runId":          "run_2026_sd_01",   // optional — uses discovery run_id if absent
            "driveFilter":    ["Drive A", "B"],   // optional — migrate only these drives by name
            "driveIdMapping": { "srcId": "dstId" } // optional — overrides saved CSV mapping
        }

    If neither driveFilter nor driveIdMapping is provided, ALL source Shared Drives
    are migrated, creating matching drives in the destination domain.

    Returns:
        { "success": true, "run_id": "...", "message": "..." }
    """
    # FIX-2: clear stale state from dead gunicorn worker if PID changed
    _check_worker_reset()

    body        = request.get_json(silent=True) or {}
    run_id      = _resolve_run_id(body)
    drive_filter     = body.get("driveFilter")      or None   # list of names or None
    drive_id_mapping = body.get("driveIdMapping")   or None   # {src_id: dst_id} or None

    if not run_id:
        return jsonify({
            "success": False,
            "message": "No runId found. Pass runId in the body or run discovery first.",
        }), 400

    cred_err = _validate_credentials()
    if cred_err:
        return jsonify({"success": False, "message": cred_err}), 400

    with _sd_runs_lock:
        if run_id in _sd_runs and _sd_runs[run_id]["status"] == "running":
            return jsonify({
                "success": False,
                "message": f"Shared Drive run '{run_id}' is already in progress.",
            }), 409
        _register_sd_run(run_id)

    # Merge: body mapping takes precedence over saved CSV mapping
    if drive_id_mapping is None:
        drive_id_mapping = _load_sd_mapping() or None

    _ensure_backend_on_path()
    _launch_sd(run_id, drive_filter, drive_id_mapping)

    logger.info(
        f"[shared-drive/start] run_id={run_id} | "
        f"drive_filter={drive_filter} | "
        f"id_mapping_pairs={len(drive_id_mapping) if drive_id_mapping else 0}"
    )
    return jsonify({
        "success": True,
        "run_id":  run_id,
        "message": "Shared Drive migration started",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/migrate/resume
# ─────────────────────────────────────────────────────────────────────────────

# REPLACE the entire resume route body (lines 354–382)

@shared_drive_bp.route("/shared-drive/migrate/resume", methods=["POST"])
@require_auth
def resume_shared_drive_migration():
    _check_worker_reset()

    body   = request.get_json(silent=True) or {}
    run_id = (body.get("runId") or body.get("run_id") or "").strip()

    if not run_id:
        return jsonify({"success": False, "message": "runId is required"}), 400

    cred_err = _validate_credentials()
    if cred_err:
        return jsonify({"success": False, "message": cred_err}), 400

    with _sd_runs_lock:
        existing = _sd_runs.get(run_id)
        # ── FIX: only block if it is ACTIVELY running ──────────────────────────
        if existing and existing["status"] == "running":
            return jsonify({
                "success": False,
                "message": f"Run '{run_id}' is already running.",
            }), 409
        # ── FIX: remove stale done/error entry so _register_sd_run creates fresh ─
        if existing:
            _sd_runs.pop(run_id, None)
        _register_sd_run(run_id)

    drive_id_mapping = _load_sd_mapping() or None

    _ensure_backend_on_path()
    # ── FIX: pass resume=True so background thread skips Phase 1 re-discovery ──
    _launch_sd(run_id, drive_filter=None, drive_id_mapping=drive_id_mapping, resume=True)

    logger.info(f"[shared-drive/resume] run_id={run_id}")
    return jsonify({
        "success": True,
        "run_id":  run_id,
        "message": "Shared Drive migration resumed",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/migrate/stream-token
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive/migrate/stream-token", methods=["POST"])
@require_auth
def get_sd_stream_token():
    """
    Issue a one-time SSE stream token (same pattern as migration_routes.py).
    EventSource cannot send Authorization headers — this is the workaround.

    Body (JSON): { "runId": "..." }   (optional — falls back to last discovery run_id)
    Returns:     { "success": true, "token": "...", "run_id": "..." }
    """
    # FIX-2: clear stale state from dead gunicorn worker if PID changed
    _check_worker_reset()

    body   = request.get_json(silent=True) or {}
    run_id = _resolve_run_id(body)

    if not run_id:
        return jsonify({"success": False, "message": "No active run_id found"}), 400

    token = secrets.token_urlsafe(32)
    with _sd_stream_tokens_lock:
        _sd_stream_tokens[token] = {"run_id": run_id}

    logger.info(f"[sd-stream-token] issued for run_id={run_id}")
    return jsonify({"success": True, "token": token, "run_id": run_id})


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/shared-drive/migrate/stream?token=<tok>
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive/migrate/stream", methods=["GET"])
def stream_sd_migration():
    """
    SSE stream for Shared Drive migration progress.

    Auth: ?token=<one-time token from /stream-token>
    Events: phase | progress | done | error

    FIX-3: if the run is absent from _sd_runs (worker restart after SIGSEGV),
    a skeleton entry is re-hydrated from SQL so the stream doesn't return 404.
    A synthetic "reconnected" phase event is emitted so the frontend knows it
    reconnected mid-run rather than starting fresh.
    """
    token = (
        request.args.get("token", "").strip()
        or request.args.get("stream_token", "").strip()
    )
    if token == "undefined":
        token = ""

    if token:
        with _sd_stream_tokens_lock:
            token_data = _sd_stream_tokens.pop(token, None)
        if not token_data:
            return jsonify({"error": "Invalid or expired stream token"}), 401
        run_id = token_data["run_id"]
    else:
        return jsonify({"error": "stream token required for SSE"}), 401

    with _sd_runs_lock:
        run = _sd_runs.get(run_id)

    # FIX-3: re-hydrate from SQL if worker restart wiped _sd_runs
    if run is None:
        sql_status = _fetch_sd_status_from_sql(run_id)
        if sql_status:
            _rehydrate_sd_run(run_id, sql_status)
            with _sd_runs_lock:
                run = _sd_runs.get(run_id)
            logger.info(
                f"[sd-stream] Re-hydrated run_id={run_id} from SQL after worker restart"
            )
        else:
            return jsonify({"error": f"Unknown run_id: {run_id}"}), 404

    def generate():
        q = run["queue"]

        # FIX-3: synthetic event so frontend knows it reconnected post-crash
        reconnected = run.get("_rehydrated", False)
        if reconnected:
            yield (
                f"event: phase\n"
                f"data: {json.dumps({'phase': 'reconnected', 'run_id': run_id})}\n\n"
            )

        while True:
            try:
                event = q.get(timeout=30)
            except _queue.Empty:
                yield ": heartbeat\n\n"
                continue
            etype = event.get("type", "progress")
            data  = json.dumps(event.get("data", {}))
            yield f"event: {etype}\ndata: {data}\n\n"
            if etype in ("done", "error"):
                break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/shared-drive/migrate/status?run_id=<run_id>
# ─────────────────────────────────────────────────────────────────────────────

@shared_drive_bp.route("/shared-drive/migrate/status", methods=["GET"])
@require_auth
def sd_migration_status():
    """
    Poll-friendly JSON snapshot of a running or completed Shared Drive migration.
    Falls back to SQL when run is not in memory (post VM restart).

    FIX-2: _check_worker_reset() clears stale in-process state on new worker.
    FIX-3: SQL fallback re-hydrates _sd_runs so subsequent calls skip SQL.
    """
    # FIX-2
    _check_worker_reset()

    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        run_id = (state.config.get("last_sd_discovery_run_id") or "").strip()
    if not run_id:
        return jsonify({"error": "run_id query param required"}), 400

    with _sd_runs_lock:
        run = _sd_runs.get(run_id)

    if run:
        return jsonify({
            "run_id":  run_id,
            "status":  run["status"],
            "totals":  run["totals"],
            "summary": run["summary"],
        })

    # FIX-3: reconstruct from SQL and re-hydrate _sd_runs for future calls
    try:
        _ensure_backend_on_path()
        sql_status = _fetch_sd_status_from_sql(run_id)
        if sql_status:
            _rehydrate_sd_run(run_id, sql_status)
            return jsonify({"run_id": run_id, **sql_status})
    except Exception as exc:
        logger.warning(f"[sd-status] SQL fallback failed: {exc}")

    return jsonify({"error": f"Unknown run_id: {run_id}"}), 404


# ─────────────────────────────────────────────────────────────────────────────
# Background worker
# ─────────────────────────────────────────────────────────────────────────────

def _launch_sd(run_id: str, drive_filter, drive_id_mapping, resume: bool = False):
    """Spin up the background daemon thread for Shared Drive migration."""
    q = _sd_runs[run_id]["queue"]
    t = threading.Thread(
        target=_run_sd_migration_bg,
        args=(run_id, drive_filter, drive_id_mapping, q, resume),
        daemon=True,
    )
    t.start()


def _run_sd_migration_bg(run_id, drive_filter, drive_id_mapping, q, resume: bool = False):
    """
    Daemon thread:
    1. Builds admin Drive services (source + dest) with full timeout.
    2. Constructs SharedDriveMigrator — temporary admin membership logic
       runs automatically inside migrate_all_shared_drives().
    3. Streams phase/progress/done/error events onto the SSE queue.

    FIX-1: state.update_config(last_sd_discovery_run_id=run_id) is called at
    the top so the run_id survives any gunicorn worker restart that follows.
    All Drive services are built INSIDE this thread — never inherited from the
    parent process — so dead FDs from a previous worker are never touched.
    """
    try:
        _ensure_backend_on_path()

        # FIX-1: persist run_id immediately so _resolve_run_id() works after
        # a worker restart even if the frontend doesn't pass runId in the body
        state.update_config(last_sd_discovery_run_id=run_id)
        logger.info(f"[shared-drive-bg] persisted last_sd_discovery_run_id={run_id}")

        from routes.config_routes import _apply_config_to_backend
        try:
            _apply_config_to_backend()
        except Exception:
            pass

        from config import Config
        from shared_drive_sql_state_manager import SharedDriveSQLStateManager

        # ── Resolve credentials ───────────────────────────────────────────────
        _FLASK_CRED_DIR = Path.home() / "flask-backend" / "uploads" / "credential"
        _AMEY_DIR       = Path(Config.__file__).parent if hasattr(Config, "__file__") else BACKEND_DIR

        def _resolve_cred(flask_name, config_path):
            p = _FLASK_CRED_DIR / flask_name
            if p.exists():
                return str(p)
            abs_p = Path(config_path)
            if not abs_p.is_absolute():
                abs_p = _AMEY_DIR / config_path
            if abs_p.exists():
                return str(abs_p)
            raise FileNotFoundError(
                f"Credential not found at '{p}' or '{abs_p}'. "
                "Upload via /api/config."
            )

        src_creds = _resolve_cred("source_credentials.json", Config.SOURCE_CREDENTIALS_FILE)
        dst_creds = _resolve_cred("dest_credentials.json",   Config.DEST_CREDENTIALS_FILE)

        gcs_key = Config.GCS_SERVICE_ACCOUNT_FILE
        if not Path(gcs_key).is_absolute():
            gcs_key = str(BACKEND_DIR / gcs_key)

        # ── SQLStateManager ───────────────────────────────────────────────────
        db_config = {
            "host":     Config.DB_HOST,
            "port":     Config.DB_PORT,
            "database": Config.DB_NAME,
            "user":     Config.DB_USER,
            "password": Config.DB_PASSWORD,
        }
        sql_mgr = SharedDriveSQLStateManager(
            db_config=db_config,
            gcs_bucket=Config.GCS_BUCKET_NAME,
            gcs_key_file=gcs_key,
            source_domain=Config.SOURCE_DOMAIN,
            dest_domain=Config.DEST_DOMAIN,
            gcs_prefix=Config.GCS_STAGING_PREFIX,
            migration_id=run_id,
        )

        # ── Build admin Drive services ────────────────────────────────────────
        # IMPORTANT: built here inside the thread, never in the parent process.
        # This prevents inheriting dead httplib2 socket FDs after SIGSEGV.
        import httplib2
        from google.oauth2 import service_account as _sa
        from googleapiclient.discovery import build as _gapi_build

        def _build_drive_svc(creds_file: str, admin_email: str):
            creds = _sa.Credentials.from_service_account_file(
                creds_file,
                scopes=Config.SCOPES,
                subject=admin_email,
            )
            try:
                import google_auth_httplib2 as _gah
                _h = httplib2.Http(timeout=1800)
                _h.follow_redirects = True
                http = _gah.AuthorizedHttp(creds, http=_h)
                return _gapi_build("drive", "v3", http=http)
            except ImportError:
                return _gapi_build("drive", "v3", credentials=creds)

        source_admin_drive = _build_drive_svc(src_creds, Config.SOURCE_ADMIN_EMAIL)
        dest_admin_drive   = _build_drive_svc(dst_creds, Config.DEST_ADMIN_EMAIL)

        # ── SharedDriveMigrator ───────────────────────────────────────────────
        from shared_drive_migrator import SharedDriveMigrator

        migrator = SharedDriveMigrator(
            source_admin_drive=source_admin_drive,
            dest_admin_drive=dest_admin_drive,
            source_domain=Config.SOURCE_DOMAIN,
            dest_domain=Config.DEST_DOMAIN,
            config=Config,
            sql_mgr=sql_mgr,
            run_id=run_id,
        )

        # ── SSE phase events ──────────────────────────────────────────────────
        q.put({"type": "phase", "data": {"phase": "shared_drive_migration", "run_id": run_id}})

        # ── Run migration (temp membership handled inside) ────────────────────
        summary = migrator.migrate_all_shared_drives(
            drive_filter=drive_filter,
            drive_id_mapping=drive_id_mapping,
            resume=resume,
        )

        # ── Push per-drive results as progress events ─────────────────────────
        for dr in summary.get("drive_results", []):
            with _sd_runs_lock:
                run = _sd_runs.get(run_id)
                if run:
                    t = run["totals"]
                    t["drives_done"]      += 1
                    t["files_migrated"]   += dr.get("files_migrated",  0)
                    t["files_failed"]     += dr.get("files_failed",    0)
                    t["folders_created"]  += dr.get("folders_created", 0)
                    t["members_migrated"] += dr.get("members_migrated",0)
                    totals = dict(t)

            q.put({"type": "progress", "data": {**dr, "totals": totals}})

        # ── Done ──────────────────────────────────────────────────────────────
        with _sd_runs_lock:
            run = _sd_runs.get(run_id)
            if run:
                run["status"]  = "done"
                run["summary"] = summary
                totals         = dict(run["totals"])

        q.put({"type": "done", "data": {
            "run_id":  run_id,
            "summary": summary,
            "totals":  totals,
        }})
        logger.info(f"[shared-drive-bg] run_id={run_id} DONE")

    except Exception as exc:
        logger.exception(f"[shared-drive-bg] run_id={run_id} FAILED: {exc}")
        with _sd_runs_lock:
            run = _sd_runs.get(run_id)
            if run:
                run["status"] = "error"
        q.put({"type": "error", "data": {
            "run_id":    run_id,
            "error":     str(exc),
            "traceback": traceback.format_exc()[-2000:],
        }})


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _register_sd_run(run_id: str):
    """
    Create in-memory run entry. Must be called inside _sd_runs_lock.

    FIX-1: persists last_sd_discovery_run_id to session.json immediately so
    _resolve_run_id() recovers the run_id after a gunicorn worker restart
    without needing a runId in the request body.
    """
    _sd_runs[run_id] = {
        "status":  "running",
        "summary": None,
        "queue":   _queue.Queue(),
        "_rehydrated": False,   # FIX-3: flag set by _rehydrate_sd_run()
        "totals": {
            "drives_total":     0,
            "drives_done":      0,
            "files_migrated":   0,
            "files_failed":     0,
            "files_ignored":    0,
            "folders_created":  0,
            "members_migrated": 0,
        },
    }
    # FIX-1: persist immediately — survives worker restart
    state.update_config(last_sd_discovery_run_id=run_id)
    logger.info(f"[sd-routes] Registered run_id={run_id} → persisted to session.json")


def _rehydrate_sd_run(run_id: str, sql_status: dict):
    """
    FIX-3: Re-populate _sd_runs from SQL after a worker restart wipes it.

    Creates a skeleton entry with the SQL-derived status and an empty Queue.
    The Queue will never receive new events (the background thread died with
    the old worker), but the status endpoint can still serve correct data,
    and the stream endpoint emits a synthetic "reconnected" event.
    """
    with _sd_runs_lock:
        if run_id in _sd_runs:
            return  # another request already re-hydrated it — don't overwrite

        sql_totals = sql_status.get("totals", {})
        _sd_runs[run_id] = {
            "status":      sql_status.get("status", "unknown"),
            "summary":     sql_status,
            "queue":       _queue.Queue(),
            "_rehydrated": True,   # triggers synthetic "reconnected" SSE event
            "totals": {
                "drives_total":     0,
                "drives_done":      0,
                "files_migrated":   int(sql_totals.get("files_migrated", 0)),
                "files_failed":     int(sql_totals.get("files_failed",   0)),
                "files_ignored":    0,
                "folders_created":  0,
                "members_migrated": 0,
            },
        }
    logger.info(f"[sd-routes] Re-hydrated run_id={run_id} from SQL (post-restart)")


def _resolve_run_id(body: dict) -> str:
    """
    Return the canonical run_id for SD: body > last_sd_discovery_run_id.

    FIX-1: last_sd_discovery_run_id is now written to state.config by both
    _register_sd_run() and _run_sd_migration_bg(), so this fallback actually
    works after a gunicorn worker restart.
    """
    frontend_id = (body.get("runId") or body.get("run_id") or "").strip()
    sd_disc_id  = (state.config.get("last_sd_discovery_run_id") or "").strip()
    return frontend_id or sd_disc_id


def _validate_credentials() -> str:
    creds   = state.credentials_exist()
    missing = []
    if not creds["source"]: missing.append("source_credentials.json")
    if not creds["dest"]:   missing.append("dest_credentials.json")
    return (
        f"Missing credential file(s): {', '.join(missing)}. Upload via /api/config."
        if missing else ""
    )


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)


def _save_sd_mapping(mapping: dict):
    """Persist drive ID mapping to uploads/sd_mapping.json."""
    try:
        _SD_MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SD_MAPPING_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(mapping, indent=2), encoding="utf-8")
        tmp.replace(_SD_MAPPING_FILE)
    except Exception as exc:
        logger.warning(f"[sd-mapping] persist failed: {exc}")


def _load_sd_mapping() -> dict:
    """Load drive ID mapping from uploads/sd_mapping.json. Returns {} if missing."""
    try:
        if _SD_MAPPING_FILE.exists():
            return json.loads(_SD_MAPPING_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"[sd-mapping] load failed: {exc}")
    return {}


def _build_admin_drive_service(kind: str = "source"):
    """
    Build an admin Drive service for listing drives (used by /discover).
    Mirrors the credential resolution logic in migration_engine_v4.
    """
    _ensure_backend_on_path()
    from config import Config
    import httplib2
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    _FLASK_CRED_DIR = Path.home() / "flask-backend" / "uploads" / "credential"

    if kind == "source":
        flask_name   = "source_credentials.json"
        config_path  = Config.SOURCE_CREDENTIALS_FILE
        admin_email  = Config.SOURCE_ADMIN_EMAIL
    else:
        flask_name   = "dest_credentials.json"
        config_path  = Config.DEST_CREDENTIALS_FILE
        admin_email  = Config.DEST_ADMIN_EMAIL

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
        subject=admin_email,
    )
    try:
        import google_auth_httplib2 as _gah
        _h = httplib2.Http(timeout=1800)
        _h.follow_redirects = True
        http = _gah.AuthorizedHttp(creds, http=_h)
        return _gapi_build("drive", "v3", http=http)
    except ImportError:
        return _gapi_build("drive", "v3", credentials=creds)


def _fetch_sd_status_from_sql(run_id: str) -> dict:
    """Minimal SQL fallback for when the run is not in memory (post-restart)."""
    try:
        from config import Config
        conn = Config.get_db_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT status, start_time, end_time, total_items, completed_items, failed_items
                  FROM migration_runs WHERE migration_id = %s
            """, (run_id,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "status":     row.get("status", "UNKNOWN"),
                "start_time": str(row["start_time"]) if row.get("start_time") else None,
                "end_time":   str(row["end_time"])   if row.get("end_time")   else None,
                "totals": {
                    "files_migrated": int(row.get("completed_items", 0) or 0),
                    "files_failed":   int(row.get("failed_items",    0) or 0),
                },
            }
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"[sd-status-sql] {exc}")
        return {}
