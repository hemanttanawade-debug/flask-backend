"""
routes/reset_routes.py

DELETE /api/reset
    — Wipe the current session: clears uploads/ files (credentials + CSV +
      session.json) AND deletes every SQL row tied to the given run_id(s).

Body (JSON) — all fields optional:
    {
        "runId":      "run_2026_04_21",   # single run to delete from SQL
        "runIds":     ["run_A", "run_B"], # multiple runs (merged with runId)
        "deleteAll":  true                # nuke ALL migration_* rows in the DB
    }

If neither runId / runIds / deleteAll is supplied the route still performs the
filesystem hard-reset (credentials, CSV, session.json) but leaves SQL untouched.

SQL tables cleared (in safe FK order):
    migration_permissions
    migration_folder_mapping
    migration_items
    migration_runs
    migration_users          ← if the table exists (older schemas may not have it)

Response 200:
    {
        "success":        true,
        "new_session_id": "<uuid>",
        "files_deleted":  ["credential/source_credentials.json", ...],
        "sql": {
            "runs_deleted":        2,
            "items_deleted":       1482,
            "folders_deleted":     94,
            "permissions_deleted": 310,
            "users_deleted":       8,
            "error":               null      # non-null if SQL step failed
        }
    }
"""

import sys
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

reset_bp    = Blueprint("reset", __name__)
BACKEND_DIR = Path.home() / "amey"


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /api/reset
# ─────────────────────────────────────────────────────────────────────────────

@reset_bp.route("/reset", methods=["DELETE"])
@require_auth
def delete_session():
    body       = request.get_json(silent=True) or {}
    delete_all = bool(body.get("deleteAll", False))

    # Collect run IDs to delete from SQL
    run_ids: list[str] = []
    if body.get("runId"):
        run_ids.append(str(body["runId"]).strip())
    for rid in body.get("runIds", []):
        rid = str(rid).strip()
        if rid and rid not in run_ids:
            run_ids.append(rid)

    # ── 1. Filesystem hard-reset ───────────────────────────────────────────
    files_deleted = _reset_filesystem()

    # ── 2. SQL cleanup ────────────────────────────────────────────────────
    sql_result = {"runs_deleted": 0, "items_deleted": 0, "folders_deleted": 0,
                  "permissions_deleted": 0, "users_deleted": 0, "error": None}

    if delete_all or run_ids:
        _ensure_backend_on_path()
        sql_result = _delete_sql_data(run_ids=run_ids, delete_all=delete_all)

    current_app.logger.info(
        f"[reset] files={files_deleted} | sql={sql_result} | "
        f"new_session={state.session_id}"
    )

    return jsonify({
        "success":        True,
        "new_session_id": state.session_id,
        "files_deleted":  files_deleted,
        "sql":            sql_result,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Filesystem helper
# ─────────────────────────────────────────────────────────────────────────────

def _reset_filesystem() -> list[str]:
    """
    Call session_state.hard_reset() which:
      - Deletes uploads/credential/source_credentials.json
      - Deletes uploads/credential/dest_credentials.json
      - Deletes uploads/users.csv
      - Overwrites uploads/session.json with a blank slate
      - Generates a new session_id
      - Releases any migration lock

    Returns a list of the relative paths that were removed.
    """
    # Snapshot which files exist BEFORE the reset so we can report them
    candidates = [
        state.SOURCE_CREDENTIALS_PATH,
        state.DEST_CREDENTIALS_PATH,
        state.CSV_PATH,
        state.SESSION_FILE,
    ]
    removed = [
        str(p.relative_to(state.UPLOAD_DIR))
        for p in candidates
        if p.exists()
    ]

    state.hard_reset()   # wipes files + in-memory state, writes clean session.json
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# SQL helper
# ─────────────────────────────────────────────────────────────────────────────

_SQL_TABLES_IN_ORDER = [
    # child tables first (FK constraints)
    "migration_permissions",
    "migration_folder_mapping",
    "migration_items",
    "migration_users",   # may not exist in older schemas — handled gracefully
    "migration_runs",
]


def _delete_sql_data(run_ids: list[str], delete_all: bool) -> dict:
    """
    Delete SQL rows for the given run_ids (or every row if delete_all=True).

    Uses Config.get_db_connection() with a 3-attempt retry (same pattern as
    the fixed _load_run_from_sql in migration_routes.py).

    Returns a dict with per-table deletion counts and any error string.
    """
    import time as _time
    from config import Config

    totals = {
        "runs_deleted": 0, "items_deleted": 0,
        "folders_deleted": 0, "permissions_deleted": 0,
        "users_deleted": 0, "error": None,
    }

    # ── Connect with retry ────────────────────────────────────────────────
    conn = None
    last_exc = None
    for attempt in range(3):
        try:
            conn = Config.get_db_connection()
            break
        except Exception as exc:
            last_exc = exc
            _time.sleep(2 ** attempt)

    if conn is None:
        totals["error"] = f"DB connect failed after 3 attempts: {last_exc}"
        current_app.logger.error(f"[reset] {totals['error']}")
        return totals

    try:
        cur = conn.cursor()

        if delete_all:
            # Truncate / delete all rows from every table
            for table in _SQL_TABLES_IN_ORDER:
                try:
                    cur.execute(f"DELETE FROM {table}")   # DELETE not TRUNCATE so FK checks apply
                    count = cur.rowcount
                    conn.commit()
                    _record(totals, table, count)
                    current_app.logger.info(f"[reset] DELETE ALL {table}: {count} rows")
                except Exception as exc:
                    conn.rollback()
                    # migration_users might not exist — log and continue
                    current_app.logger.warning(
                        f"[reset] Could not delete from {table}: {exc}"
                    )

        else:
            # Delete only the requested run_ids
            placeholders = ", ".join(["%s"] * len(run_ids))

            table_col_map = {
                "migration_permissions":  "file_id IN (SELECT file_id FROM migration_items WHERE migration_id IN ({ph}))",
                "migration_folder_mapping": "migration_id IN ({ph})",
                "migration_items":        "migration_id IN ({ph})",
                "migration_users":        "migration_id IN ({ph})",
                "migration_runs":         "migration_id IN ({ph})",
            }

            for table, where_tpl in table_col_map.items():
                try:
                    if table == "migration_permissions":
                        # permissions don't store migration_id directly;
                        # join through migration_items
                        sql = f"""
                            DELETE p FROM migration_permissions p
                            INNER JOIN migration_items i ON p.file_id = i.file_id
                            WHERE i.migration_id IN ({placeholders})
                        """
                        params = run_ids
                    else:
                        sql    = f"DELETE FROM {table} WHERE migration_id IN ({placeholders})"
                        params = run_ids

                    cur.execute(sql, params)
                    count = cur.rowcount
                    conn.commit()
                    _record(totals, table, count)
                    current_app.logger.info(
                        f"[reset] DELETE {table} run_ids={run_ids}: {count} rows"
                    )
                except Exception as exc:
                    conn.rollback()
                    current_app.logger.warning(
                        f"[reset] Could not delete from {table} "
                        f"for run_ids={run_ids}: {exc}"
                    )

    except Exception as exc:
        totals["error"] = str(exc)
        current_app.logger.error(f"[reset] Unexpected SQL error: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return totals


def _record(totals: dict, table: str, count: int):
    """Map table name → totals key."""
    mapping = {
        "migration_runs":         "runs_deleted",
        "migration_items":        "items_deleted",
        "migration_folder_mapping": "folders_deleted",
        "migration_permissions":  "permissions_deleted",
        "migration_users":        "users_deleted",
    }
    key = mapping.get(table)
    if key:
        totals[key] += max(0, count)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
