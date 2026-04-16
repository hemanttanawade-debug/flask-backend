"""
routes/status_routes.py

Handles:
  GET /api/migration/<id>/status   — poll live migration progress
  GET /api/migration/<id>/logs     — fetch accumulated log lines
  GET /api/migration/<id>/report   — download the text report file
"""

from pathlib import Path
from flask import Blueprint, jsonify, send_from_directory, abort
from routes.auth_routes import require_auth
import session_state as state

status_bp = Blueprint("status", __name__)

REPORT_DIR = Path(__file__).parent.parent / "reports"
BACKEND_REPORT_DIR = Path(__file__).parent.parent.parent / "enterprise-migration" / "reports"


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/<id>/status
# ─────────────────────────────────────────────────────────────────────────────

@status_bp.route("/migration/<migration_id>/status", methods=["GET"])
@require_auth
def get_status(migration_id: str):
    """
    Returns current migration progress.
    If the requested ID is not the active migration, checks history.
    This way old runs are never lost when a new migration starts.
    """
    m = state.migration

    # Active migration matches
    if m["migration_id"] == migration_id:
        data = m
    # Look up from history (completed/failed previous runs)
    elif migration_id in state.all_migrations:
        data = state.all_migrations[migration_id]
    else:
        return jsonify({"error": "Migration ID not found"}), 404

    return jsonify({
        "migrationId":   data["migration_id"] or migration_id,
        "status":        data["status"],
        "totalUsers":    data["total_users"],
        "filesMigrated": data["files_migrated"],
        "failedFiles":   data["failed_files"],
        "logs":          data["logs"][-100:],
    })


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/<id>/logs
# ─────────────────────────────────────────────────────────────────────────────

@status_bp.route("/migration/<migration_id>/logs", methods=["GET"])
@require_auth
def get_logs(migration_id: str):
    """Returns the full log buffer for the current/last migration."""
    return jsonify({"logs": state.migration.get("logs", [])})


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/<id>/report
# ─────────────────────────────────────────────────────────────────────────────

@status_bp.route("/migration/<migration_id>/report", methods=["GET"])
@require_auth
def download_report(migration_id: str):
    """
    Streams the latest report file to the browser as a download.
    Looks in both the Flask reports/ folder and the backend reports/ folder.
    """
    # Try backend reports dir first (that's where the engine writes them)
    for report_dir in [BACKEND_REPORT_DIR, REPORT_DIR]:
        if report_dir.exists():
            # Find the latest report that matches this migration_id, or just the latest
            candidates = sorted(report_dir.glob("migration_report_*.txt"), reverse=True)
            if candidates:
                latest = candidates[0]
                return send_from_directory(
                    str(report_dir),
                    latest.name,
                    as_attachment=True,
                    download_name=f"migration_report_{migration_id}.txt",
                )

    abort(404, description="No report found for this migration.")