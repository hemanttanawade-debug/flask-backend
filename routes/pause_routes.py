"""
routes/pause_routes.py

  POST /api/migration/<migration_id>/pause   — pause a running migration
  POST /api/migration/<migration_id>/resume  — resume a paused migration
  GET  /api/migration/<migration_id>/pause-status — check current pause state

How it works
────────────
A threading.Event called _pause_event is used as a global signal flag.

  • pause  → _pause_event.clear()   (workers block on event.wait())
  • resume → _pause_event.set()     (workers unblock and continue)

Your migration engine (amey/) must call check_pause() at safe checkpoints
(e.g. after each file copy). See integration note at bottom of this file.

Wire into app.py
────────────────
  from routes.pause_routes import pause_bp
  app.register_blueprint(pause_bp, url_prefix="/api")
"""

import threading
import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

pause_bp = Blueprint("pause", __name__)
logger   = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Global pause event
#   SET   = running normally  (event.set()   → workers proceed)
#   CLEAR = paused            (event.clear() → workers block on .wait())
# ─────────────────────────────────────────────────────────────────────────────

_pause_event     = threading.Event()
_pause_event.set()          # start in running state

_pause_lock      = threading.Lock()
_paused_at: datetime | None = None   # UTC timestamp of last pause


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers — import these inside amey/ migration engine
# ─────────────────────────────────────────────────────────────────────────────

def check_pause():
    """
    Call this at every safe checkpoint in the migration engine.
    Blocks indefinitely while paused; returns immediately when running.

    Usage inside amey/ worker:
        from routes.pause_routes import check_pause

        for file in files_to_migrate:
            check_pause()          # ← blocks here if paused
            migrate_file(file)
    """
    _pause_event.wait()     # blocks while event is clear (paused)


def is_paused() -> bool:
    """Returns True if the migration is currently paused."""
    return not _pause_event.is_set()


def reset_pause_state():
    """
    Call this when a new migration starts so it always begins in running state.
    Already called by POST /api/migration-mode or migration start if you wire it in.
    """
    global _paused_at
    with _pause_lock:
        _pause_event.set()
        _paused_at = None


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration/<migration_id>/pause
# ─────────────────────────────────────────────────────────────────────────────

@pause_bp.route("/migration/<migration_id>/pause", methods=["POST"])
@require_auth
def pause_migration(migration_id: str):
    """
    Pause the running migration.
    Workers will finish their current file and then block until resumed.
    """
    global _paused_at

    # Validate migration exists and is running
    m = _get_migration(migration_id)
    if m is None:
        return jsonify({"success": False, "message": "Migration ID not found."}), 404

    current_status = m.get("status", "")
    if current_status not in ("running", "active", "started"):
        return jsonify({
            "success": False,
            "message": f"Cannot pause — migration is currently '{current_status}'.",
        }), 409

    if is_paused():
        return jsonify({"success": False, "message": "Migration is already paused."}), 409

    with _pause_lock:
        _pause_event.clear()                          # signal workers to block
        _paused_at = datetime.now(timezone.utc)

    # Reflect paused state in session so status endpoint shows it
    state.migration["status"] = "paused"
    if migration_id in state.all_migrations:
        state.all_migrations[migration_id]["status"] = "paused"

    current_app.logger.info(f"[pause] migration_id={migration_id} PAUSED")

    return jsonify({
        "success":      True,
        "migrationId":  migration_id,
        "status":       "paused",
        "pausedAt":     _paused_at.isoformat(),
        "message":      "Migration paused. Workers will stop after finishing their current file.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration/<migration_id>/resume
# ─────────────────────────────────────────────────────────────────────────────

@pause_bp.route("/migration/<migration_id>/resume", methods=["POST"])
@require_auth
def resume_migration(migration_id: str):
    """
    Resume a paused migration.
    All blocked workers unblock immediately and continue from where they stopped.
    """
    global _paused_at

    m = _get_migration(migration_id)
    if m is None:
        return jsonify({"success": False, "message": "Migration ID not found."}), 404

    if not is_paused():
        return jsonify({"success": False, "message": "Migration is not paused."}), 409

    paused_duration_s = None
    with _pause_lock:
        if _paused_at:
            paused_duration_s = round(
                (datetime.now(timezone.utc) - _paused_at).total_seconds()
            )
        _pause_event.set()                            # unblock all workers
        _paused_at = None

    # Restore running state in session
    state.migration["status"] = "running"
    if migration_id in state.all_migrations:
        state.all_migrations[migration_id]["status"] = "running"

    current_app.logger.info(
        f"[pause] migration_id={migration_id} RESUMED "
        f"(was paused {paused_duration_s}s)"
    )

    return jsonify({
        "success":           True,
        "migrationId":       migration_id,
        "status":            "running",
        "pausedDurationSec": paused_duration_s,
        "message":           "Migration resumed. Workers are continuing.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/<migration_id>/pause-status
# ─────────────────────────────────────────────────────────────────────────────

@pause_bp.route("/migration/<migration_id>/pause-status", methods=["GET"])
@require_auth
def pause_status(migration_id: str):
    """
    Returns the current pause state for the frontend toggle button.

    Response:
        {
            "migrationId": "...",
            "paused":      true | false,
            "pausedAt":    "2026-04-23T10:00:00+00:00" | null,
            "pausedForSec": 42 | null
        }
    """
    m = _get_migration(migration_id)
    if m is None:
        return jsonify({"success": False, "message": "Migration ID not found."}), 404

    paused       = is_paused()
    paused_for_s = None
    paused_at_str = None

    if paused and _paused_at:
        paused_for_s  = round((datetime.now(timezone.utc) - _paused_at).total_seconds())
        paused_at_str = _paused_at.isoformat()

    return jsonify({
        "migrationId":  migration_id,
        "paused":       paused,
        "pausedAt":     paused_at_str,
        "pausedForSec": paused_for_s,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _get_migration(migration_id: str) -> dict | None:
    """Look up migration from active state or history."""
    if state.migration.get("migration_id") == migration_id:
        return state.migration
    return state.all_migrations.get(migration_id)


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION NOTE — amey/ migration engine
# ─────────────────────────────────────────────────────────────────────────────
#
# Add check_pause() calls at natural stopping points in your worker loop.
# The finer the granularity, the faster pause takes effect.
#
# Example — amey/migration_engine.py or wherever per-file copy happens:
#
#   import sys
#   sys.path.insert(0, "/path/to/flask-backend")   # so the import resolves
#   from routes.pause_routes import check_pause, reset_pause_state
#
#   def run_migration(run_id, user_mapping, ...):
#       reset_pause_state()          # ensure clean state on every new run
#       for user in user_mapping:
#           for file in get_files(user):
#               check_pause()        # ← blocks here when paused
#               copy_file(file)
#               mark_done(file)
#
# That's all that's needed — no changes to threading model required.
# ─────────────────────────────────────────────────────────────────────────────
