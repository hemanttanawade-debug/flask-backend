"""
routes/dashboard_routes.py

  GET /api/dashboard?run_id=<run_id>

  Returns live aggregates for the Dashboard page by querying the SQL tables
  directly.  No background thread required — called on every manual Refresh.

  Response shape (matches DashboardSummary in frontend types/migration.ts):
  {
    "totalUsers":         int,
    "completed":          int,
    "inProgress":         int,
    "failed":             int,
    "filesMigrated":      int,
    "filesTotal":         int,
    "dataTransferredGb":  float,
    "dataTotalGb":        float,
    "rows": [
      {
        "sourceUser":      str,
        "destinationUser": str,
        "status":          "completed" | "running" | "failed" | "pending",
        "progressPct":     int,        // 0-100
        "filesDone":       int,
        "filesTotal":      int,
        "filesFailed":     int,
        "sizeDoneGb":      float,
        "sizeTotalGb":     float
      },
      ...
    ]
  }

  SQL tables used
  ───────────────
  migration_items  — one row per file/folder
                     Columns used: migration_id, source_user_email,
                                   destination_user_email, status,
                                   file_size_bytes, is_folder

  Status values written by migration_engine_v4 (all uppercase):
      PENDING | IN_PROGRESS | DONE | FAILED | IGNORED

  BUGS FIXED
  ──────────
  1. _apply_config_to_backend() now imported correctly from config_routes
     (was causing ImportError / silent DB connect failure on sync)
  2. Fixed column names: source_owner/dest_owner → source_user_email/destination_user_email
  3. Fixed status case: 'done'/'completed' → 'DONE' (engine writes uppercase)
  4. Added is_folder = 0 filter so folder rows don't inflate file counts
  5. Correct size column: size_bytes → file_size_bytes
  6. Removed dependency on non-existent migration_users table
  7. Added warning key when no run_id found so frontend shows a message
"""

import sys
import time
import logging
from pathlib import Path

from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

dashboard_bp = Blueprint("dashboard", __name__)
BACKEND_DIR  = Path.home() / "amey"
logger       = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/dashboard
# ─────────────────────────────────────────────────────────────────────────────

@dashboard_bp.route("/dashboard", methods=["GET"])
@require_auth
def get_dashboard():
    """
    Query SQL and return live aggregates for the Dashboard.

    Query params:
        run_id  (optional) — if omitted, uses the last known run_id from
                             session state (state.config["last_discovery_run_id"])
    """
    run_id = (
        request.args.get("run_id", "").strip()
        or state.config.get("last_discovery_run_id", "").strip()
    )

    if not run_id:
        current_app.logger.warning("[dashboard] No run_id available — returning empty summary")
        return jsonify({
            **_empty_summary(),
            "warning": "No run_id provided or found in session. Start a discovery run first.",
        })

    _ensure_backend_on_path()

    try:
        conn = _get_db_conn()
    except Exception as exc:
        current_app.logger.error(f"[dashboard] DB connect failed: {exc}")
        return jsonify({"error": f"DB connection failed: {exc}"}), 500

    try:
        summary = _build_summary(conn, run_id)
        return jsonify(summary)
    except Exception as exc:
        current_app.logger.exception(f"[dashboard] build_summary error: {exc}")
        return jsonify({"error": str(exc)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Core aggregation
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary(conn, run_id: str) -> dict:
    """
    Build the full DashboardSummary dict by aggregating migration_items.
    Groups by (source_user_email, destination_user_email).
    """
    rows = _aggregate_from_items(conn, run_id)

    user_rows    = []
    total_users  = len(rows)
    completed    = 0
    in_progress  = 0
    failed_users = 0
    files_done   = 0
    files_total  = 0
    size_done_b  = 0
    size_total_b = 0

    for r in rows:
        status = _normalise_status(r.get("status", "pending"))

        u_files_done   = int(r.get("files_done",   0) or 0)
        u_files_total  = int(r.get("files_total",  0) or 0)
        u_files_failed = int(r.get("files_failed", 0) or 0)
        u_size_done    = int(r.get("size_done_bytes",  0) or 0)
        u_size_total   = int(r.get("size_total_bytes", 0) or 0)

        if u_files_total > 0:
            pct = min(100, round((u_files_done / u_files_total) * 100))
        elif status == "completed":
            pct = 100
        else:
            pct = 0

        user_rows.append({
            "sourceUser":      r.get("source_email", ""),
            "destinationUser": r.get("dest_email",   ""),
            "status":          status,
            "progressPct":     pct,
            "filesDone":       u_files_done,
            "filesTotal":      u_files_total,
            "filesFailed":     u_files_failed,
            "sizeDoneGb":      round(u_size_done  / 1_073_741_824, 3),
            "sizeTotalGb":     round(u_size_total / 1_073_741_824, 3),
        })

        if status == "completed":
            completed += 1
        elif status in ("running", "in_progress"):
            in_progress += 1
        elif status == "failed":
            failed_users += 1

        files_done   += u_files_done
        files_total  += u_files_total
        size_done_b  += u_size_done
        size_total_b += u_size_total

    current_app.logger.info(
        f"[dashboard] run_id={run_id} | users={total_users} "
        f"completed={completed} running={in_progress} failed={failed_users} "
        f"files={files_done}/{files_total}"
    )

    return {
        "totalUsers":        total_users,
        "completed":         completed,
        "inProgress":        in_progress,
        "failed":            failed_users,
        "filesMigrated":     files_done,
        "filesTotal":        files_total,
        "dataTransferredGb": round(size_done_b  / 1_073_741_824, 3),
        "dataTotalGb":       round(size_total_b / 1_073_741_824, 3),
        "rows":              user_rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SQL aggregation
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_from_items(conn, run_id: str) -> list:
    """
    Aggregate migration_items grouped by user pair.

    Correct column names (confirmed from migration_routes.py SQL):
        source_user_email, destination_user_email, file_size_bytes

    Correct status values (engine writes uppercase):
        DONE | FAILED | IN_PROGRESS | PENDING | IGNORED

    Folder rows excluded via is_folder = 0.
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                COALESCE(source_user_email,      '') AS source_email,
                COALESCE(destination_user_email, '') AS dest_email,
                COUNT(*)                                                AS files_total,
                SUM(CASE WHEN status = 'DONE'
                         THEN 1 ELSE 0 END)                            AS files_done,
                SUM(CASE WHEN status = 'FAILED'
                         THEN 1 ELSE 0 END)                            AS files_failed,
                SUM(CASE WHEN status = 'IGNORED'
                         THEN 1 ELSE 0 END)                            AS files_ignored,
                SUM(CASE WHEN status IN ('PENDING', 'IN_PROGRESS')
                         THEN 1 ELSE 0 END)                            AS files_pending,
                SUM(COALESCE(file_size_bytes, 0))                       AS size_total_bytes,
                SUM(CASE WHEN status = 'DONE'
                         THEN COALESCE(file_size_bytes, 0)
                         ELSE 0 END)                                   AS size_done_bytes
            FROM migration_items
            WHERE migration_id = %s
              AND is_folder = 0
            GROUP BY source_user_email, destination_user_email
            ORDER BY source_user_email
            """,
            (run_id,)
        )
        rows = cur.fetchall() or []
        current_app.logger.info(
            f"[dashboard] migration_items: {len(rows)} user groups for run_id={run_id}"
        )

        # Derive a per-user status from item counts
        for r in rows:
            ft      = int(r.get("files_total",   0) or 0)
            fd      = int(r.get("files_done",    0) or 0)
            ff      = int(r.get("files_failed",  0) or 0)
            pending = int(r.get("files_pending", 0) or 0)

            if ft == 0:
                r["status"] = "pending"
            elif pending > 0:
                r["status"] = "running"
            elif ff > 0 and fd == 0:
                r["status"] = "failed"
            elif fd + ff >= ft:
                r["status"] = "completed"
            elif fd > 0:
                r["status"] = "running"
            else:
                r["status"] = "pending"

        return rows

    except Exception as exc:
        current_app.logger.error(
            f"[dashboard] migration_items aggregation failed: {exc}"
        )
        return []
    finally:
        cur.close()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_status(raw: str) -> str:
    """Map DB status strings to the four values the frontend knows."""
    s = (raw or "").lower().strip()
    if s in ("completed", "done", "success", "migrated"):
        return "completed"
    if s in ("running", "in_progress", "active", "started"):
        return "running"
    if s in ("failed", "error"):
        return "failed"
    return "pending"


def _empty_summary() -> dict:
    return {
        "totalUsers":        0,
        "completed":         0,
        "inProgress":        0,
        "failed":            0,
        "filesMigrated":     0,
        "filesTotal":        0,
        "dataTransferredGb": 0.0,
        "dataTotalGb":       0.0,
        "rows":              [],
    }


def _get_db_conn():
    """
    Patch runtime Config from session state, then connect with 3-attempt retry.

    WHY _apply_config_to_backend() must be called here:
      Config class attributes (DB_HOST, DB_USER, SOURCE_DOMAIN, etc.) are
      set at runtime from state.config by _apply_config_to_backend().
      Without this call, Config still has its hardcoded defaults and
      get_db_connection() either connects to the wrong DB or fails entirely.
      This is especially critical on gunicorn workers that haven't served
      a /api/config request yet and haven't had Config patched in their process.

    The import is done inside the function (not at module level) to avoid
    a circular import — dashboard_routes → config_routes → (no back-ref).
    It is safe: Python caches the module after the first import.
    """
    _ensure_backend_on_path()

    try:
        from routes.config_routes import _apply_config_to_backend
        _apply_config_to_backend()
    except Exception as exc:
        current_app.logger.warning(
            f"[dashboard] _apply_config_to_backend failed: {exc} — "
            "proceeding with existing Config values"
        )

    from config import Config

    last_exc = None
    for attempt in range(3):
        try:
            return Config.get_db_connection()
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"DB connect failed after 3 attempts: {last_exc}")


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
