"""
routes/shared_drive_discovery_routes.py

POST /api/shared-drive/discovery/start
────────────────────────────────────────
Runs Shared Drive discovery synchronously and returns summary totals.

Mirrors discovery_routes.py (My Drive) exactly — same request/response shape,
same run_id enforcement, same threading model, same SSE-friendly progress_cb.

HOW IT DIFFERS FROM My Drive DISCOVERY
────────────────────────────────────────
• Input is a { source_drive_id: dest_drive_id } mapping (not a user mapping).
• The engine uses a single admin service account for ALL drives instead of
  per-user impersonation.
• Optional `driveFilter` (list of drive names) limits which drives are scanned.
• The same run_id can be used for both My Drive and Shared Drive discovery
  so both phases are tracked under one migration_run row.

REQUEST BODY (JSON)
────────────────────
{
    "sessionId":      "<sid>",            // optional
    "runId":          "run_2026_sd_01",   // required
    "driveIdMapping": {                   // required (may be empty {} for discover-all)
        "0AMsjkUJDhC7EUk9PVA": "0AIlOaJWf3SCuUk9PVA",
        "0AJyBEB19OuSEUk9PVA": "0AMq5a3IghgDrUk9PVA"
    },
    "driveFilter": ["Finance Q1", "HR"],  // optional — names to include
    "workers": 4                          // optional, default 4
}

RESPONSE (200)
──────────────
{
    "success":  true,
    "run_id":   "run_2026_sd_01",
    "totals": {
        "total_drives":      N,
        "completed_drives":  N,
        "failed_drives":     N,
        "total_files":       N,
        "total_folders":     N,
        "total_size_bytes":  N
    },
    "results": [ ...per-drive dicts... ]
}

RESPONSE (4xx / 5xx)
────────────────────
{ "success": false, "message": "...", "detail": "..." }

FIXES in this file (parallel to discovery_routes.py FIX-1..4)
──────────────────────────────────────────────────────────────
FIX-1  Module-level logger — never current_app.logger inside callbacks/threads.
FIX-2  state._persist() called after saving last_sd_discovery_run_id.
FIX-3  _accumulate_sd() is lock-protected (thread-safe totals accumulation).
FIX-4  Error response includes exc type + truncated traceback.
FIX-5  Reuses persisted run_id if SQL already has items (avoids orphaning rows).
"""

import logging
import sys
import threading
import traceback
from pathlib import Path

from flask import Blueprint, jsonify, request
from routes.auth_routes import require_auth
import session_state as state

sd_discovery_bp = Blueprint("sd_discovery", __name__)
BACKEND_DIR     = Path.home() / "amey"

# FIX-1: module-level logger — safe in any thread, no app context needed
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/shared-drive/discovery/start
# ─────────────────────────────────────────────────────────────────────────────

@sd_discovery_bp.route("/shared-drive/discovery/start", methods=["POST"])
@require_auth
def start_sd_discovery():
    """
    Runs Shared Drive discovery synchronously and returns the complete summary.
    Blocks until all drives are scanned — identical contract to My Drive's
    POST /api/discovery/start.
    """
    body = request.get_json(silent=True) or {}

    # ── Session ───────────────────────────────────────────────────────────────
    sid = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    frontend_run_id  = body.get("runId", "").strip()
    drive_id_mapping = body.get("driveIdMapping") or {}
    drive_filter     = body.get("driveFilter")    or None   # list of names or None
    workers          = int(body.get("workers", 4))

    if not isinstance(drive_id_mapping, dict):
        return jsonify({
            "success": False,
            "message": "driveIdMapping must be a JSON object {src_id: dst_id}",
        }), 400

    # ── Run-ID enforcement (FIX-5) ────────────────────────────────────────────
    # Identical logic to discovery_routes.py: reuse persisted run_id when SQL
    # already has items under it, to avoid orphaning rows.
    persisted_run_id = (state.config.get("last_sd_discovery_run_id") or "").strip()

    if persisted_run_id:
        sql_has_items = _check_sql_has_items(persisted_run_id)
        if sql_has_items:
            if frontend_run_id and frontend_run_id != persisted_run_id:
                logger.warning(
                    f"[sd-discovery] Frontend sent runId={frontend_run_id!r} but "
                    f"SQL has items under {persisted_run_id!r}. "
                    "Reusing persisted run_id."
                )
            run_id = persisted_run_id
            logger.info(f"[sd-discovery] Reusing run_id={run_id!r} (SQL has items)")
        else:
            run_id = frontend_run_id or persisted_run_id
            logger.info(f"[sd-discovery] Fresh scan, run_id={run_id!r}")
    else:
        run_id = frontend_run_id
        if not run_id:
            return jsonify({"success": False, "message": "runId is required"}), 400
        logger.info(f"[sd-discovery] First SD discovery, run_id={run_id!r}")

    # Sync session.json with the run_id we're actually using
    if run_id != persisted_run_id:
        state.config["last_sd_discovery_run_id"] = run_id
        state._persist()   # FIX-2

    _ensure_backend_on_path()

    try:
        from routes.config_routes import _apply_config_to_backend
        _apply_config_to_backend()
    except Exception:
        pass

    logger.info(
        f"[sd-discovery] run_id={run_id} started | "
        f"drives_in_mapping={len(drive_id_mapping)} | "
        f"drive_filter={drive_filter} | workers={workers}"
    )

    try:
        from shared_drive_discovery_engine import run_shared_drive_discovery

        totals      = _empty_totals()
        results     = []
        totals_lock = threading.Lock()   # FIX-3

        def on_drive_done(drive_result: dict):
            # FIX-1: logger not current_app.logger — runs in worker threads
            # FIX-3: lock protects shared totals + results
            with totals_lock:
                _accumulate_sd(totals, drive_result)
                results.append(drive_result)
            logger.info(
                f"[sd-discovery] drive done: "
                f"'{drive_result.get('drive_name', '?')}' | "
                f"files={drive_result.get('files', 0)} "
                f"folders={drive_result.get('folders', 0)} "
                f"status={drive_result.get('status', '?')}"
            )

        # Blocks until all drives are scanned
        final_results = run_shared_drive_discovery(
            run_id=run_id,
            drive_id_mapping=drive_id_mapping,
            workers=workers,
            drive_filter=drive_filter,
            progress_cb=on_drive_done,
        )

        # Prefer the authoritative list returned by the engine
        if final_results:
            results = final_results
            totals  = _empty_totals()
            for r in results:
                _accumulate_sd(totals, r)

        logger.info(
            f"[sd-discovery] run_id={run_id} done | "
            f"drives={totals['total_drives']} "
            f"files={totals['total_files']} "
            f"folders={totals['total_folders']} "
            f"bytes={totals['total_size_bytes']}"
        )

        # FIX-2: persist so the run_id survives gunicorn worker restarts
        state.config["last_sd_discovery_run_id"] = run_id
        state._persist()

        return jsonify({
            "success": True,
            "run_id":  run_id,
            "totals":  totals,
            "results": results,
        })

    except Exception as exc:
        # FIX-4: include exc type + truncated traceback for frontend diagnostics
        tb = traceback.format_exc()
        logger.exception(f"[sd-discovery] run_id={run_id} error: {exc}")
        return jsonify({
            "success": False,
            "message": str(exc),
            "detail":  tb[-2000:],
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/shared-drive/discovery/status?run_id=<run_id>
# ─────────────────────────────────────────────────────────────────────────────

@sd_discovery_bp.route("/shared-drive/discovery/status", methods=["GET"])
@require_auth
def sd_discovery_status():
    """
    Returns summary counts for a completed SD discovery run by querying SQL.
    Useful for the dashboard to show "last discovery" stats after a page reload.
    """
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        run_id = (state.config.get("last_sd_discovery_run_id") or "").strip()
    if not run_id:
        return jsonify({"error": "run_id query param required"}), 400

    _ensure_backend_on_path()

    try:
        from config import Config
        conn = Config.get_db_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT
                    COUNT(*)                                      AS total_items,
                    SUM(file_size_bytes)                          AS total_size_bytes,
                    SUM(mime_type = 'application/vnd.google-apps.folder') AS total_folders
                FROM migration_items
                WHERE migration_id = %s
            """, (run_id,))
            row = cur.fetchone() or {}
            total_items  = int(row.get("total_items",    0) or 0)
            size_bytes   = int(row.get("total_size_bytes", 0) or 0)
            folders      = int(row.get("total_folders",  0) or 0)
            files        = total_items - folders
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"[sd-discovery-status] SQL query failed: {exc}")
        return jsonify({"error": str(exc)}), 500

    return jsonify({
        "run_id":       run_id,
        "total_items":  total_items,
        "total_files":  files,
        "total_folders": folders,
        "total_size_bytes": size_bytes,
        "total_size_gb":    round(size_bytes / 1_073_741_824.0, 3),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _check_sql_has_items(run_id: str) -> bool:
    """
    Returns True if migration_items has rows for this run_id.
    Fails safe — returns False on DB error so discovery proceeds normally.
    """
    try:
        _ensure_backend_on_path()
        from config import Config
        conn = Config.get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM migration_items WHERE migration_id=%s LIMIT 1",
                (run_id,),
            )
            return cur.fetchone() is not None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        logger.warning(f"[sd-discovery] _check_sql_has_items failed (safe fallback): {exc}")
        return False


def _empty_totals() -> dict:
    return {
        "total_drives":     0,
        "completed_drives": 0,
        "failed_drives":    0,
        "total_files":      0,
        "total_folders":    0,
        "total_size_bytes": 0,
    }


def _accumulate_sd(totals: dict, drive_result: dict) -> None:
    """
    Merge one drive's result into running totals.
    NOTE: caller must hold totals_lock before calling this (FIX-3).
    """
    totals["total_drives"]     += 1
    totals["total_files"]      += drive_result.get("files",      0)
    totals["total_folders"]    += drive_result.get("folders",    0)
    totals["total_size_bytes"] += drive_result.get("size_bytes", 0)
    if drive_result.get("status") == "failed":
        totals["failed_drives"]    += 1
    else:
        totals["completed_drives"] += 1


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
