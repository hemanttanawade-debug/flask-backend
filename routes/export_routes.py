"""
routes/export_routes.py

Download logs and SQL data for a specific migration run_id.

ENDPOINTS
─────────
GET /api/export/logs?run_id=<run_id>&format=txt|json
    Downloads the PM2 / application log lines that mention the run_id.
    Searches all known log file locations automatically.

GET /api/export/sql?run_id=<run_id>&table=<table|all>&format=csv|json
    Downloads rows from one or all migration SQL tables filtered by run_id.

    Supported table values:
        all                    → zip archive of every table as separate CSV files
        migration_runs         → one row — the run header
        migration_items        → every file/folder discovery + status row
        migration_folder_mapping → source→dest folder ID mapping
        migration_permissions  → permission migration records
        migration_users        → per-user aggregated stats (if table exists)

WIRE INTO app.py
────────────────
    from routes.export_routes import export_bp
    app.register_blueprint(export_bp, url_prefix="/api")
"""

import io
import csv
import sys
import json
import zipfile
import logging
from datetime import datetime
from pathlib import Path

from flask import Blueprint, request, jsonify, Response, current_app
from routes.auth_routes import require_auth
import session_state as state

export_bp   = Blueprint("export", __name__)
BACKEND_DIR = Path.home() / "amey"
logger      = logging.getLogger(__name__)

# ── All log locations PM2 / gunicorn might write to ──────────────────────────
_LOG_SEARCH_PATHS = [
    Path.home() / ".pm2" / "logs",
    Path("/var/log"),
    Path.home() / "flask-backend" / "logs",
    Path.home() / "flask-backend",
]

# ── SQL tables to export, in FK-safe order ───────────────────────────────────
_ALL_TABLES = [
    "migration_runs",
    "migration_users",
    "migration_items",
    "migration_folder_mapping",
    "migration_permissions",
]

# Per-table: which column holds the migration_id / run_id so we can filter
_TABLE_RUN_COL = {
    "migration_runs":          "migration_id",
    "migration_users":         "migration_id",
    "migration_items":         "migration_id",
    "migration_folder_mapping":"migration_id",
    # permissions don't store migration_id directly — join through items
    "migration_permissions":   None,
}


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/export/logs
# ─────────────────────────────────────────────────────────────────────────────

@export_bp.route("/export/logs", methods=["GET"])
@require_auth
def download_logs():
    """
    Scans every known log file for lines containing run_id and returns them.

    Query params:
        run_id  (required) — migration run ID to filter on
        format  (optional) — "txt" (default) or "json"

    Response:
        Content-Disposition: attachment; filename="logs_<run_id>_<date>.txt"
    """
    run_id = request.args.get("run_id", "").strip()
    fmt    = request.args.get("format", "txt").strip().lower()

    if not run_id:
        return jsonify({"error": "run_id query param is required"}), 400
    if fmt not in ("txt", "json"):
        return jsonify({"error": "format must be 'txt' or 'json'"}), 400

    matched_lines = []
    files_scanned = []

    for log_dir in _LOG_SEARCH_PATHS:
        if not log_dir.exists():
            continue
        # Recurse one level — pick up flask-backend-out.log, flask-backend-error.log, etc.
        for log_file in sorted(log_dir.glob("**/*.log")):
            try:
                files_scanned.append(str(log_file))
                with log_file.open("r", encoding="utf-8", errors="replace") as f:
                    for lineno, line in enumerate(f, 1):
                        if run_id in line:
                            matched_lines.append({
                                "file":   str(log_file.name),
                                "lineno": lineno,
                                "text":   line.rstrip("\n"),
                            })
            except Exception as exc:
                current_app.logger.warning(
                    f"[export/logs] could not read {log_file}: {exc}"
                )

    current_app.logger.info(
        f"[export/logs] run_id={run_id} | "
        f"scanned={len(files_scanned)} files | matched={len(matched_lines)} lines"
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if fmt == "json":
        payload = json.dumps({
            "run_id":        run_id,
            "exported_at":   timestamp,
            "files_scanned": files_scanned,
            "total_lines":   len(matched_lines),
            "lines":         matched_lines,
        }, indent=2)
        return Response(
            payload,
            mimetype="application/json",
            headers={
                "Content-Disposition":
                    f'attachment; filename="logs_{run_id}_{timestamp}.json"'
            },
        )

    # Plain text — one line per match, with file + line number prefix
    lines_txt = "\n".join(
        f"[{m['file']}:{m['lineno']}]  {m['text']}"
        for m in matched_lines
    )
    header = (
        f"Migration log export\n"
        f"run_id      : {run_id}\n"
        f"exported_at : {timestamp} UTC\n"
        f"files scanned: {len(files_scanned)}\n"
        f"matched lines: {len(matched_lines)}\n"
        f"{'─' * 80}\n\n"
    )

    return Response(
        header + lines_txt,
        mimetype="text/plain",
        headers={
            "Content-Disposition":
                f'attachment; filename="logs_{run_id}_{timestamp}.txt"'
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/export/sql
# ─────────────────────────────────────────────────────────────────────────────

@export_bp.route("/export/sql", methods=["GET"])
@require_auth
def download_sql():
    """
    Export SQL table rows for a given run_id.

    Query params:
        run_id  (required) — migration run ID to filter on
        table   (optional) — table name or "all" (default: "all")
        format  (optional) — "csv" (default) or "json"
                             When table=all, always returns a ZIP of CSVs.

    Single table  → CSV or JSON file download
    table=all     → ZIP archive containing one CSV per table
    """
    run_id     = request.args.get("run_id", "").strip()
    table_name = request.args.get("table",  "all").strip().lower()
    fmt        = request.args.get("format", "csv").strip().lower()

    if not run_id:
        return jsonify({"error": "run_id query param is required"}), 400

    valid_tables = set(_ALL_TABLES) | {"all"}
    if table_name not in valid_tables:
        return jsonify({
            "error": f"Unknown table '{table_name}'. "
                     f"Valid values: {', '.join(sorted(valid_tables))}"
        }), 400
    if fmt not in ("csv", "json"):
        return jsonify({"error": "format must be 'csv' or 'json'"}), 400

    _ensure_backend_on_path()

    try:
        conn = _get_db_conn()
    except Exception as exc:
        return jsonify({"error": f"DB connection failed: {exc}"}), 500

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    try:
        if table_name == "all":
            # Always ZIP when exporting all tables
            return _export_all_tables_zip(conn, run_id, timestamp)
        else:
            rows, columns = _fetch_table(conn, run_id, table_name)
            if fmt == "json":
                return _respond_json(rows, columns, run_id, table_name, timestamp)
            else:
                return _respond_csv(rows, columns, run_id, table_name, timestamp)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_db_conn():
    """Connect with 3-attempt retry (same pattern as migration_routes)."""
    import time as _time
    from config import Config

    last_exc = None
    for attempt in range(3):
        try:
            return Config.get_db_connection()
        except Exception as exc:
            last_exc = exc
            _time.sleep(2 ** attempt)
    raise RuntimeError(f"DB connect failed after 3 attempts: {last_exc}")


def _fetch_table(conn, run_id: str, table: str):
    """
    Return (rows, columns) for the given table filtered by run_id.
    Handles migration_permissions specially (no direct migration_id column).
    """
    cur = conn.cursor(dictionary=True)
    run_col = _TABLE_RUN_COL.get(table)

    try:
        if table == "migration_permissions":
            # Join through migration_items to get run_id filter
            sql = """
                SELECT p.*
                  FROM migration_permissions p
                  JOIN migration_items i ON p.file_id = i.file_id
                 WHERE i.migration_id = %s
                 ORDER BY p.file_id
            """
            cur.execute(sql, (run_id,))
        elif run_col:
            cur.execute(
                f"SELECT * FROM {table} WHERE {run_col} = %s ORDER BY 1",
                (run_id,)
            )
        else:
            # Fallback: no filter — return all rows (shouldn't happen)
            cur.execute(f"SELECT * FROM {table} ORDER BY 1")

        rows    = cur.fetchall() or []
        columns = [d[0] for d in cur.description] if cur.description else []

        current_app.logger.info(
            f"[export/sql] table={table} run_id={run_id} rows={len(rows)}"
        )
        return rows, columns

    except Exception as exc:
        current_app.logger.warning(
            f"[export/sql] table={table} failed: {exc}"
        )
        return [], []
    finally:
        cur.close()


def _export_all_tables_zip(conn, run_id: str, timestamp: str) -> Response:
    """Build an in-memory ZIP with one CSV per table and return it."""
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in _ALL_TABLES:
            rows, columns = _fetch_table(conn, run_id, table)

            csv_buf = io.StringIO()
            writer  = csv.DictWriter(
                csv_buf,
                fieldnames=columns,
                extrasaction="ignore",
                lineterminator="\n",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(_stringify_row(row))

            zf.writestr(
                f"{table}_{run_id}.csv",
                csv_buf.getvalue(),
            )

        # Also write a manifest JSON
        manifest = {
            "run_id":      run_id,
            "exported_at": timestamp,
            "tables":      {t: "included" for t in _ALL_TABLES},
        }
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))

    zip_buffer.seek(0)
    return Response(
        zip_buffer.read(),
        mimetype="application/zip",
        headers={
            "Content-Disposition":
                f'attachment; filename="sql_export_{run_id}_{timestamp}.zip"'
        },
    )


def _respond_csv(rows, columns, run_id, table, timestamp) -> Response:
    buf    = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=columns,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(_stringify_row(row))

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition":
                f'attachment; filename="{table}_{run_id}_{timestamp}.csv"'
        },
    )


def _respond_json(rows, columns, run_id, table, timestamp) -> Response:
    payload = json.dumps({
        "run_id":      run_id,
        "table":       table,
        "exported_at": timestamp,
        "columns":     columns,
        "row_count":   len(rows),
        "rows":        [_stringify_row(r) for r in rows],
    }, indent=2, default=str)

    return Response(
        payload,
        mimetype="application/json",
        headers={
            "Content-Disposition":
                f'attachment; filename="{table}_{run_id}_{timestamp}.json"'
        },
    )


def _stringify_row(row: dict) -> dict:
    """Convert datetime / bytes / None to plain strings for CSV/JSON."""
    return {
        k: (
            v.isoformat() if hasattr(v, "isoformat")
            else v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray))
            else "" if v is None
            else v
        )
        for k, v in row.items()
    }


# ─────────────────────────────────────────────────────────────────────────────
# Misc
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
