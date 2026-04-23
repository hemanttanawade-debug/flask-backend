"""
routes/discovery_routes.py

  POST /api/discovery/start — runs Drive scan synchronously, returns summary totals

Frontend flow (React):
  1. POST /api/discovery/start  →  { run_id, totals, results }
     totals: { total_files, total_folders, total_size_bytes,
               total_users, completed_users, failed_users }
"""

import sys
import threading
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

discovery_bp = Blueprint("discovery", __name__)
BACKEND_DIR  = Path.home() / "amey"


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
        { "success": false, "message": "..." }
    """
    body = request.get_json(silent=True) or {}

    sid = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    run_id       = body.get("runId", "").strip()
    user_mapping = body.get("userMapping", {})
    workers      = int(body.get("workers", 4))

    if not run_id:
        return jsonify({"success": False, "message": "runId is required"}), 400
    if not user_mapping:
        return jsonify({"success": False, "message": "userMapping is required"}), 400

    _ensure_backend_on_path()

    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    current_app.logger.info(
        f"[discovery] run_id={run_id} started | "
        f"users={len(user_mapping)} | workers={workers}"
    )

    try:
        from discovery_engine import run_discovery

        totals  = _empty_totals()
        results = []

        def on_user_done(user_result: dict):
            _accumulate(totals, user_result)
            results.append(user_result)

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

        current_app.logger.info(
            f"[discovery] run_id={run_id} done | "
            f"files={totals['total_files']} folders={totals['total_folders']} "
            f"bytes={totals['total_size_bytes']}"
        )

        # Layer 4: persist the confirmed run_id in session so migration_routes
        # can validate that its runId matches what discovery registered in SQL.
        state.config["last_discovery_run_id"] = run_id

        return jsonify({
            "success": True,
            "run_id":  run_id,
            "totals":  totals,
            "results": results,
        })

    except Exception as exc:
        current_app.logger.exception(f"[discovery] run_id={run_id} error: {exc}")
        return jsonify({"success": False, "message": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

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
