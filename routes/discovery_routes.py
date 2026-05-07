"""
routes/discovery_routes.py

  POST /api/discovery/start — runs Drive scan synchronously, returns summary totals

Frontend flow (React):
  1. POST /api/discovery/start  →  { run_id, totals, results }
     totals: { total_files, total_folders, total_size_bytes,
               total_users, completed_users, failed_users }

FIXES vs previous version
──────────────────────────
FIX-1  Replaced all current_app.logger calls inside callbacks and helper
       functions with a module-level logger. current_app is a proxy that only
       works inside a Flask request context. The on_user_done() callback is
       invoked by worker threads spawned inside run_discovery() — those threads
       have no request context, so current_app raises:
         RuntimeError: Working outside of application context.
       Using logging.getLogger(__name__) is always safe in any thread.

FIX-2  state._persist() called after saving last_discovery_run_id so the
       run_id survives a gunicorn worker restart and is available to the
       dashboard and migration routes.

FIX-3  _accumulate() made thread-safe with a lock — on_user_done() is called
       from concurrent worker threads inside run_discovery(); without a lock,
       the counter increments have a race condition on CPython (GIL helps but
       does not fully protect dict updates).

FIX-4  Improved error response: includes exc type and a truncated traceback
       so the frontend can show a meaningful message instead of a bare string.
"""

import sys
import logging
import threading
import traceback
from pathlib import Path

from flask import Blueprint, request, jsonify
from routes.auth_routes import require_auth
import session_state as state

discovery_bp = Blueprint("discovery", __name__)
BACKEND_DIR  = Path.home() / "amey"

# FIX-1: module-level logger — safe in any thread, no app context needed
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/discovery/start
# ─────────────────────────────────────────────────────────────────────────────

@discovery_bp.route("/discovery/start", methods=["POST"])
@require_auth
def start_discovery():
    """
    Runs the Drive scan synchronously and returns the complete summary.

    Body (JSON):
        {
            "sessionId":   "<sid>",          // optional
            "runId":       "run_2026_04_21", // required
            "userMapping": {                 // required
                "alice@src.com": "alice@dst.com",
                ...
            },
            "workers": 4                     // optional, default 4
        }

    Returns (200):
        {
            "success":     true,
            "run_id":      "...",
            "totals": {
                "total_users":      N,
                "completed_users":  N,
                "failed_users":     N,
                "total_files":      N,
                "total_folders":    N,
                "total_size_bytes": N
            },
            "results": [ ...per-user dicts... ]
        }

    Returns (4xx / 5xx):
        { "success": false, "message": "...", "detail": "..." }
    """
    body = request.get_json(silent=True) or {}

    sid = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    frontend_run_id = body.get("runId", "").strip()
    user_mapping    = body.get("userMapping", {})
    workers         = int(body.get("workers", 4))

    if not user_mapping:
        return jsonify({"success": False, "message": "userMapping is required"}), 400

    # ── Single run_id enforcement ─────────────────────────────────────────────
    # There is exactly ONE canonical run_id per migration lifecycle:
    #   last_discovery_run_id in session.json (persisted across restarts).
    #
    # Rule: if a discovery run_id is already persisted AND the DB already has
    # items for it, REUSE it — ignore whatever the frontend generated.
    # This prevents the frontend generating a new timestamp-based runId on every
    # click and silently orphaning the previously discovered SQL rows.
    #
    # Only create a new run_id when:
    #   a) No discovery has ever run (last_discovery_run_id is empty), OR
    #   b) The user explicitly wants a fresh scan (no existing SQL items).
    persisted_run_id = (state.config.get("last_discovery_run_id") or "").strip()

    if persisted_run_id:
        # Check whether SQL already has items for the persisted run_id
        sql_has_items = _check_sql_has_items(persisted_run_id)
        if sql_has_items:
            if frontend_run_id and frontend_run_id != persisted_run_id:
                logger.warning(
                    f"[discovery] Frontend sent runId={frontend_run_id!r} but "
                    f"SQL already has items under {persisted_run_id!r}. "
                    f"Reusing persisted run_id to avoid orphaning SQL rows."
                )
            run_id = persisted_run_id
            logger.info(f"[discovery] Reusing existing run_id={run_id!r} (SQL has items)")
        else:
            # Persisted run_id exists but SQL is empty — use frontend id or persisted
            run_id = frontend_run_id or persisted_run_id
            logger.info(f"[discovery] Starting fresh scan with run_id={run_id!r}")
    else:
        # No prior discovery — use whatever the frontend sent
        run_id = frontend_run_id
        if not run_id:
            return jsonify({"success": False, "message": "runId is required"}), 400
        logger.info(f"[discovery] First discovery, run_id={run_id!r}")

    # Always keep session.json in sync with the run_id we are actually using
    if run_id != persisted_run_id:
        state.config["last_discovery_run_id"] = run_id
        state._persist()

    _ensure_backend_on_path()

    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    # FIX-1: use module logger — safe inside request context and in threads
    logger.info(
        f"[discovery] run_id={run_id} started | "
        f"users={len(user_mapping)} | workers={workers}"
    )

    try:
        from discovery_engine import run_discovery

        totals       = _empty_totals()
        results      = []
        # FIX-3: lock protects totals dict from concurrent on_user_done() calls
        totals_lock  = threading.Lock()

        def on_user_done(user_result: dict):
            # FIX-1: logger (not current_app.logger) — this runs in worker threads
            # FIX-3: lock protects shared totals + results from race conditions
            with totals_lock:
                _accumulate(totals, user_result)
                results.append(user_result)
            logger.info(
                f"[discovery] user done: "
                f"{user_result.get('source_email', '?')} | "
                f"files={user_result.get('files', 0)} "
                f"folders={user_result.get('folders', 0)} "
                f"status={user_result.get('status', '?')}"
            )

        # Blocks until all users are scanned
        final_results = run_discovery(
            run_id=run_id,
            user_mapping=user_mapping,
            workers=workers,
            progress_cb=on_user_done,
        )

        # run_discovery may return an authoritative list; prefer it if available
        if final_results:
            results = final_results
            # Recompute totals from the authoritative list
            totals = _empty_totals()
            for r in results:
                _accumulate(totals, r)

        logger.info(
            f"[discovery] run_id={run_id} done | "
            f"files={totals['total_files']} "
            f"folders={totals['total_folders']} "
            f"bytes={totals['total_size_bytes']}"
        )

        # FIX-2: persist run_id to disk so it survives gunicorn worker restarts.
        # Without _persist(), the run_id is only in memory and lost if the worker
        # is recycled before migration starts — dashboard and migration routes
        # then can't find it via state.config["last_discovery_run_id"].
        state.config["last_discovery_run_id"] = run_id
        state._persist()

        return jsonify({
            "success": True,
            "run_id":  run_id,
            "totals":  totals,
            "results": results,
        })

    except Exception as exc:
        # FIX-4: include exc type + truncated traceback in response so the
        # frontend can show a meaningful error instead of a bare string.
        tb = traceback.format_exc()
        logger.exception(f"[discovery] run_id={run_id} error: {exc}")
        return jsonify({
            "success": False,
            "message": str(exc),
            "detail":  tb[-2000:],   # last 2000 chars — enough for diagnosis
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _check_sql_has_items(run_id: str) -> bool:
    """
    Returns True if migration_items already has rows for this run_id.
    Used to decide whether to reuse an existing run_id or start fresh.
    Fails safe — returns False on any DB error so discovery proceeds normally.
    """
    try:
        _ensure_backend_on_path()
        from config import Config
        conn = Config.get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM migration_items WHERE migration_id=%s LIMIT 1",
                (run_id,)
            )
            return cur.fetchone() is not None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        logger.warning(f"[discovery] _check_sql_has_items failed (safe fallback): {exc}")
        return False


def _empty_totals() -> dict:
    return {
        "total_users":      0,
        "completed_users":  0,
        "failed_users":     0,
        "total_files":      0,
        "total_folders":    0,
        "total_size_bytes": 0,
    }


def _accumulate(totals: dict, user_result: dict):
    """
    Merge one user's result into the running totals.
    NOTE: caller is responsible for holding totals_lock before calling this.
    """
    totals["total_users"]      += 1
    totals["total_files"]      += user_result.get("files",      0)
    totals["total_folders"]    += user_result.get("folders",    0)
    totals["total_size_bytes"] += user_result.get("size_bytes", 0)
    if user_result.get("status") == "failed":
        totals["failed_users"]    += 1
    else:
        totals["completed_users"] += 1


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
