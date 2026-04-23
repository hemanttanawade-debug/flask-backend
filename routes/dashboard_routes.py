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
  migration_runs         — one row per run, stores overall totals once done
  migration_users        — one row per (run, user), stores per-user totals
  migration_items        — one row per file/folder, used for live per-user counts

  If migration_users is not populated yet (discovery-only or mid-run) the
  route falls back to aggregating migration_items directly so the dashboard
  always shows something useful.

  Wire into app.py
  ────────────────
    from routes.dashboard_routes import dashboard_bp
    app.register_blueprint(dashboard_bp, url_prefix="/api")
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
        # No migration has been started yet — return zeroed summary
        return jsonify(_empty_summary())

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
    Build the full DashboardSummary dict from SQL.

    Strategy:
      1. Try migration_users table (populated by the migration engine).
         Each row has: source_email, dest_email, status,
                       files_done, files_total, files_failed,
                       size_done_bytes, size_total_bytes
      2. If migration_users is empty or absent, fall back to aggregating
         migration_items directly (works during discovery or early migration).
    """
    rows = _fetch_user_rows(conn, run_id)

    if not rows:
        # Fallback: aggregate from migration_items
        rows = _aggregate_from_items(conn, run_id)

    # Build per-user row list and roll up totals
    user_rows    = []
    total_users  = len(rows)
    completed    = 0
    in_progress  = 0
    failed       = 0
    files_done   = 0
    files_total  = 0
    size_done_b  = 0
    size_total_b = 0

    for r in rows:
        status = _normalise_status(r.get("status", "pending"))

        u_files_done   = int(r.get("files_done",    r.get("files_migrated", 0)) or 0)
        u_files_total  = int(r.get("files_total",   r.get("total_files",   0)) or 0)
        u_files_failed = int(r.get("files_failed",  r.get("failed_files",  0)) or 0)
        u_size_done    = int(r.get("size_done_bytes",  r.get("size_migrated_bytes", 0)) or 0)
        u_size_total   = int(r.get("size_total_bytes", r.get("total_size_bytes",    0)) or 0)

        # Percentage — avoid division by zero
        if u_files_total > 0:
            pct = min(100, round((u_files_done / u_files_total) * 100))
        elif status == "completed":
            pct = 100
        else:
            pct = 0

        user_rows.append({
            "sourceUser":      r.get("source_email", r.get("source_user", "")),
            "destinationUser": r.get("dest_email",   r.get("dest_user",   "")),
            "status":          status,
            "progressPct":     pct,
            "filesDone":       u_files_done,
            "filesTotal":      u_files_total,
            "filesFailed":     u_files_failed,
            "sizeDoneGb":      round(u_size_done  / 1_073_741_824, 3),
            "sizeTotalGb":     round(u_size_total / 1_073_741_824, 3),
        })

        # Roll-up counters
        if status == "completed":
            completed += 1
        elif status in ("running", "in_progress"):
            in_progress += 1
        elif status == "failed":
            failed += 1

        files_done   += u_files_done
        files_total  += u_files_total
        size_done_b  += u_size_done
        size_total_b += u_size_total

    current_app.logger.info(
        f"[dashboard] run_id={run_id} | users={total_users} "
        f"completed={completed} running={in_progress} failed={failed} "
        f"files={files_done}/{files_total}"
    )

    return {
        "totalUsers":        total_users,
        "completed":         completed,
        "inProgress":        in_progress,
        "failed":            failed,
        "filesMigrated":     files_done,
        "filesTotal":        files_total,
        "dataTransferredGb": round(size_done_b  / 1_073_741_824, 3),
        "dataTotalGb":       round(size_total_b / 1_073_741_824, 3),
        "rows":              user_rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SQL fetchers
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_user_rows(conn, run_id: str) -> list:
    """
    Fetch per-user rows from migration_users.
    Returns [] if the table doesn't exist or has no rows for this run.
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                source_email,
                dest_email,
                status,
                files_done,
                files_total,
                files_failed,
                size_done_bytes,
                size_total_bytes
            FROM migration_users
            WHERE migration_id = %s
            ORDER BY source_email
            """,
            (run_id,)
        )
        rows = cur.fetchall() or []
        current_app.logger.debug(
            f"[dashboard] migration_users: {len(rows)} rows for run_id={run_id}"
        )
        return rows
    except Exception as exc:
        # Table might not exist — fall through to item aggregation
        current_app.logger.warning(
            f"[dashboard] migration_users query failed ({exc}); "
            "falling back to migration_items aggregation"
        )
        return []
    finally:
        cur.close()


def _aggregate_from_items(conn, run_id: str) -> list:
    """
    Aggregate migration_items to get per-user stats.
    Columns expected: source_owner, dest_owner, status, size_bytes.
    Item statuses: 'migrated'/'completed' → done; 'failed' → failed; else → pending.
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                COALESCE(source_owner, '') AS source_email,
                COALESCE(dest_owner,   '') AS dest_email,
                COUNT(*)                                            AS files_total,
                SUM(CASE WHEN status IN ('migrated','completed','done')
                         THEN 1 ELSE 0 END)                        AS files_done,
                SUM(CASE WHEN status = 'failed'
                         THEN 1 ELSE 0 END)                        AS files_failed,
                SUM(COALESCE(size_bytes, 0))                        AS size_total_bytes,
                SUM(CASE WHEN status IN ('migrated','completed','done')
                         THEN COALESCE(size_bytes, 0) ELSE 0 END)  AS size_done_bytes
            FROM migration_items
            WHERE migration_id = %s
            GROUP BY source_owner, dest_owner
            ORDER BY source_owner
            """,
            (run_id,)
        )
        rows = cur.fetchall() or []
        current_app.logger.info(
            f"[dashboard] migration_items fallback: "
            f"{len(rows)} user groups for run_id={run_id}"
        )

        # Derive a status per user based on item counts
        for r in rows:
            ft = int(r.get("files_total",  0) or 0)
            fd = int(r.get("files_done",   0) or 0)
            ff = int(r.get("files_failed", 0) or 0)

            if ft == 0:
                r["status"] = "pending"
            elif fd + ff >= ft:
                r["status"] = "failed" if ff == ft else "completed"
            elif fd > 0 or ff > 0:
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
    """Map varied DB status strings to the four values the frontend knows."""
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
    """Connect with 3-attempt retry."""
    _ensure_backend_on_path()
    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    from config import Config
    last_exc = None
    for attempt in range(3):
        try:
            return Config.get_db_connection()
        except Exception as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"DB connect failed after 3 attempts: {last_exc}")


def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
