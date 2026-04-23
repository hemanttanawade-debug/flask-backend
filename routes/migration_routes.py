"""
routes/migration_routes.py

  POST /api/migration/start    — Start fresh migration (SQL must have items from discovery)
  POST /api/migration/resume   — Resume a previous run_id from SQL checkpoint
  GET  /api/migration/runs     — List all past migration runs (for Resume UI dropdown)
  GET  /api/migration/stream   — SSE stream: phase / progress / done / error
  GET  /api/migration/status   — Poll-friendly JSON snapshot (works after VM restart)
  GET  /api/migration/summary  — Final summary (202 while running)

HOW RESUME WORKS:
  1. VM restarts — in-memory _runs dict is empty, but SQL has all state
  2. GET /api/migration/runs → returns list with resumable:true for unfinished runs
  3. User clicks Resume → POST /api/migration/resume { runId }
  4. Backend reads userMapping from SQL (no re-send needed)
  5. get_all_pending_items() returns only PENDING/FAILED rows — DONE rows skipped
  6. get_folder_mapping() reuses already-created folders — no duplicates
  7. SSE streams progress exactly like a fresh run
"""

import sys
import json
import csv
import io
import copy
import traceback
import threading
import queue as _queue
from pathlib import Path
from flask import Blueprint, request, jsonify, Response, stream_with_context, current_app
from routes.auth_routes import require_auth
import session_state as state

migration_bp = Blueprint("migration", __name__)
BACKEND_DIR  = Path.home() / "amey"

_runs: dict = {}
_runs_lock  = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# POST /api/user-mapping
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/user-mapping", methods=["POST"])
@require_auth
def upload_user_mapping():
    """
    Accepts multipart/form-data:
      - file       (required) — CSV with 'source' and 'destination' columns
      - sessionId  (optional)

    Always saved as uploads/users.csv — overwrites any previous upload.
    Returns the parsed mappings list so the frontend can build userMapping dict.
    """
    sid = request.form.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "No file provided"}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are accepted"}), 400

    # Save to fixed path — overwrites previous CSV
    csv_path = state.save_csv_file(file)
    state.csv_file_path = csv_path

    try:
        content    = Path(csv_path).read_text(encoding="utf-8-sig")
        reader     = csv.DictReader(io.StringIO(content))
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]

        if "source" not in fieldnames or "destination" not in fieldnames:
            return jsonify({
                "error": "CSV must have 'source' and 'destination' columns."
            }), 400

        mappings = []
        for row in reader:
            norm = {k.strip().lower(): v.strip() for k, v in row.items()}
            src  = norm.get("source", "")
            dst  = norm.get("destination", "")
            if src or dst:
                mappings.append({"sourceUser": src, "destinationUser": dst})

        state.user_mappings = mappings

        # Also build a flat dict { src_email: dst_email } for convenience
        user_mapping_dict = {
            m["sourceUser"]: m["destinationUser"]
            for m in mappings
            if m["sourceUser"] and m["destinationUser"]
        }

        return jsonify({
            "mappings":    mappings,
            "userMapping": user_mapping_dict,   # ready for /api/discovery/start + /api/migration/start
            "total":       len(mappings),
        })

    except Exception as e:
        return jsonify({"error": f"Failed to parse CSV: {e}"}), 500

# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration/start
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/start", methods=["POST"])
@require_auth
def start_migration():
    """
    Start a completely new migration run.

    Body (JSON):
        {
            "runId":        "run_2026_04_21",
            "userMapping":  { "alice@src.com": "alice@dst.com", ... },
            "folderWorkers": 4,
            "globalWorkers": 14
        }

    runId MUST match the run_id used in /api/discovery/start so SQL has items.
    """
    body           = request.get_json(silent=True) or {}
    run_id         = (body.get("runId") or "").strip()
    user_mapping   = body.get("userMapping") or {}
    folder_workers = int(body.get("folderWorkers", 4))
    global_workers = int(body.get("globalWorkers", 14))

    if not run_id:
        return jsonify({"success": False, "message": "runId is required"}), 400
    if not user_mapping:
        return jsonify({"success": False, "message": "userMapping is required"}), 400

    cred_err = _validate_credentials()
    if cred_err:
        return jsonify({"success": False, "message": cred_err}), 400

    with _runs_lock:
        if run_id in _runs and _runs[run_id]["status"] == "running":
            return jsonify({
                "success": False,
                "message": f"Run '{run_id}' is already in progress.",
            }), 409
        _register_run(run_id, len(user_mapping))

    _ensure_backend_on_path()
    _launch(run_id, user_mapping, folder_workers, global_workers)

    current_app.logger.info(
        f"[migration/start] run_id={run_id} | users={len(user_mapping)}"
    )
    return jsonify({
        "success":     True,
        "run_id":      run_id,
        "message":     "Migration started",
        "total_users": len(user_mapping),
    })


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/migration/resume
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/resume", methods=["POST"])
@require_auth
def resume_migration():
    """
    Resume a previously started migration run after VM restart / interruption.

    Body (JSON):
        {
            "runId":        "run_2026_04_21",
            "folderWorkers": 4,
            "globalWorkers": 14
        }

    userMapping is reconstructed from SQL — frontend does NOT need to send it again.
    Only PENDING / FAILED files are processed — already DONE files are skipped.
    Already-created folders are reused from SQL folder_mapping table.
    """
    body   = request.get_json(silent=True) or {}
    run_id = (body.get("runId") or "").strip()

    if not run_id:
        return jsonify({"success": False, "message": "runId is required"}), 400

    cred_err = _validate_credentials()
    if cred_err:
        return jsonify({"success": False, "message": cred_err}), 400

    with _runs_lock:
        if run_id in _runs and _runs[run_id]["status"] == "running":
            return jsonify({
                "success": False,
                "message": f"Run '{run_id}' is already running.",
            }), 409

    _ensure_backend_on_path()

    # Load user mapping + counts from SQL
    try:
        user_mapping, pending_count, done_count = _load_run_from_sql(run_id)
    except Exception as exc:
        return jsonify({
            "success": False,
            "message": f"Could not load run '{run_id}' from SQL: {exc}",
        }), 404

    if not user_mapping:
        return jsonify({
            "success": False,
            "message": (
                f"No users found for run_id='{run_id}'. "
                f"Ensure discovery ran with this run_id."
            ),
        }), 404

    folder_workers = int(body.get("folderWorkers", 4))
    global_workers = int(body.get("globalWorkers", 14))

    with _runs_lock:
        _register_run(run_id, len(user_mapping))
        # Start progress bar from where it left off
        _runs[run_id]["totals"]["files_done"]  = done_count
        _runs[run_id]["totals"]["files_total"] = pending_count + done_count

    _launch(run_id, user_mapping, folder_workers, global_workers)

    current_app.logger.info(
        f"[migration/resume] run_id={run_id} | users={len(user_mapping)} | "
        f"pending={pending_count} already_done={done_count}"
    )
    return jsonify({
        "success":       True,
        "run_id":        run_id,
        "message":       "Migration resumed",
        "total_users":   len(user_mapping),
        "pending_files": pending_count,
        "done_files":    done_count,
    })


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/runs
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/runs", methods=["GET"])
@require_auth
def list_runs():
    """
    Return all migration runs from SQL — used to populate the Resume UI.

    Returns:
        { "runs": [
            {
              "run_id", "status", "start_time", "end_time",
              "total_items", "completed", "failed", "pending", "done",
              "source_domain", "dest_domain",
              "resumable": true   ← true when status != COMPLETED and pending > 0
            }, ...
        ]}
    """
    _ensure_backend_on_path()
    try:
        runs = _fetch_all_runs_from_sql()
        return jsonify({"runs": runs})
    except Exception as exc:
        return jsonify({"error": f"SQL error: {exc}"}), 500


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/stream?run_id=<run_id>
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/stream", methods=["GET"])
@require_auth
def stream_migration():
    """
    SSE stream. Works identically for fresh starts and resumes.

    Event types:
        phase     — { phase: "folder_creation" | "file_transfer", run_id }
        progress  — { file_name, source_email, success, ignored, skipped, error,
                      done, total, totals }
        done      — { run_id, summary, totals }
        error     — { run_id, error }

    If run is not in memory (after VM restart) but is DONE in SQL,
    immediately emits a done event so the frontend shows the last result.
    """
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        return jsonify({"error": "run_id query param required"}), 400

    with _runs_lock:
        run = _runs.get(run_id)

    # Run not in memory — serve from SQL if it already completed
    if run is None:
        try:
            _ensure_backend_on_path()
            summary = _fetch_run_summary_from_sql(run_id)
            if summary:
                def _static():
                    data = json.dumps({"run_id": run_id, "summary": summary})
                    yield f"event: done\ndata: {data}\n\n"
                return Response(
                    stream_with_context(_static()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
        except Exception:
            pass
        return jsonify({"error": f"Unknown run_id: {run_id}"}), 404

    def generate():
        q = run["queue"]
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
# GET /api/migration/status?run_id=<run_id>
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/status", methods=["GET"])
@require_auth
def migration_status():
    """Poll-friendly snapshot. Falls back to SQL when run not in memory."""
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        return jsonify({"error": "run_id query param required"}), 400

    with _runs_lock:
        run = _runs.get(run_id)

    if run:
        return jsonify({
            "run_id": run_id, "status": run["status"],
            "totals": run["totals"], "summary": run["summary"],
        })

    try:
        _ensure_backend_on_path()
        sql_status = _fetch_run_status_from_sql(run_id)
        if sql_status:
            return jsonify({"run_id": run_id, **sql_status})
    except Exception:
        pass

    return jsonify({"error": f"Unknown run_id: {run_id}"}), 404


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/migration/summary?run_id=<run_id>
# ─────────────────────────────────────────────────────────────────────────────

@migration_bp.route("/migration/summary", methods=["GET"])
@require_auth
def migration_summary():
    """Final summary. Returns 202 while running."""
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        return jsonify({"error": "run_id query param required"}), 400

    with _runs_lock:
        run = _runs.get(run_id)

    if run:
        if run["status"] == "running":
            return jsonify({
                "run_id": run_id, "status": "running",
                "message": "Migration still in progress",
                "totals": run["totals"],
            }), 202
        return jsonify({
            "run_id": run_id, "status": run["status"],
            "summary": run["summary"], "totals": run["totals"],
        })

    try:
        _ensure_backend_on_path()
        sql_status = _fetch_run_status_from_sql(run_id)
        if sql_status:
            return jsonify({"run_id": run_id, **sql_status})
    except Exception:
        pass

    return jsonify({"error": f"Unknown run_id: {run_id}"}), 404


# ─────────────────────────────────────────────────────────────────────────────
# Background worker
# ─────────────────────────────────────────────────────────────────────────────

def _launch(run_id, user_mapping, folder_workers, global_workers):
    q = _runs[run_id]["queue"]
    t = threading.Thread(
        target=_run_migration_bg,
        args=(run_id, user_mapping, folder_workers, global_workers, q),
        daemon=True,
    )
    t.start()


def _run_migration_bg(run_id, user_mapping, folder_workers, global_workers, q):
    """
    Daemon thread. Calls migration_engine_v4.run_migration().

    Resume safety is entirely in the engine + SQLStateManager:
      - get_all_pending_items() → only PENDING/FAILED rows (DONE skipped)
      - get_folder_mapping()    → reuses existing dest folder IDs
      - mark_done / mark_failed → idempotent SQL updates
    """
    try:
        _ensure_backend_on_path()
        from migration_engine_v4 import run_migration

        q.put({"type": "phase", "data": {"phase": "folder_creation", "run_id": run_id}})

        _first_file = [False]

        def on_file_done(file_result: dict):
            if not _first_file[0]:
                _first_file[0] = True
                q.put({"type": "phase", "data": {"phase": "file_transfer", "run_id": run_id}})

            with _runs_lock:
                run = _runs.get(run_id)
                if run:
                    _accumulate_totals(run["totals"], file_result)
                    totals = dict(run["totals"])

            q.put({"type": "progress", "data": {**file_result, "totals": totals}})

        summary = run_migration(
            run_id=run_id,
            user_mapping=user_mapping,
            progress_cb=on_file_done,
            folder_workers=folder_workers,
            global_workers=global_workers,
        )

        with _runs_lock:
            run = _runs.get(run_id)
            if run:
                run["status"]  = "done"
                run["summary"] = summary
                totals         = dict(run["totals"])

        q.put({"type": "done", "data": {
            "run_id": run_id, "summary": summary, "totals": totals,
        }})

    except Exception as exc:
        with _runs_lock:
            run = _runs.get(run_id)
            if run: run["status"] = "error"
        q.put({"type": "error", "data": {"run_id": run_id, "error": str(exc)}})


# ─────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_run_from_sql(run_id: str):
    """
    Rebuild user_mapping from migration_items and return pending/done counts.

    FIX-1: Uses a retry loop (up to 3 attempts) around Config.get_db_connection()
            so a transient connection failure doesn't silently block resume.

    FIX-2: Resets any IN_PROGRESS rows back to PENDING before counting.
            Files that were mid-transfer when the server crashed are stuck as
            IN_PROGRESS forever — get_all_pending_items() only fetches
            PENDING/FAILED, so they would never be retried without this reset.

    Returns: (user_mapping, pending_count, done_count)
    """
    import time as _time
    from config import Config

    last_exc = None
    for attempt in range(3):
        try:
            conn = Config.get_db_connection()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                _time.sleep(2 ** attempt)
    else:
        raise RuntimeError(
            f"Could not connect to DB after 3 attempts: {last_exc}"
        )

    try:
        cur = conn.cursor(dictionary=True)

        # ── FIX-2: reset IN_PROGRESS → PENDING so crashed files are retried ──
        cur.execute("""
            UPDATE migration_items
               SET status = 'PENDING', error_message = NULL
             WHERE migration_id = %s
               AND status = 'IN_PROGRESS'
               AND is_folder = 0
        """, (run_id,))
        reset_count = cur.rowcount
        conn.commit()
        if reset_count:
            current_app.logger.warning(
                f"[migration/resume] reset {reset_count} IN_PROGRESS → PENDING "
                f"for run_id={run_id} (server likely crashed mid-transfer)"
            )

        # Rebuild user mapping from distinct email pairs stored during discovery
        cur.execute("""
            SELECT DISTINCT source_user_email, destination_user_email
              FROM migration_items
             WHERE migration_id = %s AND source_user_email != ''
        """, (run_id,))
        rows = cur.fetchall() or []
        user_mapping = {
            r["source_user_email"]: r["destination_user_email"]
            for r in rows
            if r.get("source_user_email") and r.get("destination_user_email")
        }

        # Count pending vs done (files only, not folders).
        # After the reset above, IN_PROGRESS no longer exists — all crashedfiles
        # are now PENDING and will be included in the pending counter correctly.
        cur.execute("""
            SELECT
                SUM(status IN ('PENDING', 'FAILED')) AS pending,
                SUM(status = 'DONE')                 AS done
              FROM migration_items
             WHERE migration_id = %s AND is_folder = 0
        """, (run_id,))
        counts  = cur.fetchone() or {}
        pending = int(counts.get("pending") or 0)
        done    = int(counts.get("done")    or 0)

        return user_mapping, pending, done
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_all_runs_from_sql():
    """Fetch all migration runs for the Resume UI dropdown."""
    from config import Config

    conn = Config.get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT migration_id, source_domain, destination_domain,
                   status, start_time, end_time,
                   total_items, completed_items, failed_items
              FROM migration_runs
             ORDER BY start_time DESC LIMIT 50
        """)
        runs   = cur.fetchall() or []
        result = []

        for r in runs:
            mid = r["migration_id"]
            cur.execute("""
                SELECT
                    SUM(status IN ('PENDING','FAILED','IN_PROGRESS')) AS pending,
                    SUM(status = 'DONE')                              AS done_count
                  FROM migration_items
                 WHERE migration_id = %s AND is_folder = 0
            """, (mid,))
            counts  = cur.fetchone() or {}
            pending = int(counts.get("pending")    or 0)
            done    = int(counts.get("done_count") or 0)

            result.append({
                "run_id":        mid,
                "status":        r.get("status", "UNKNOWN"),
                "start_time":    str(r["start_time"]) if r.get("start_time") else None,
                "end_time":      str(r["end_time"])   if r.get("end_time")   else None,
                "total_items":   int(r.get("total_items",     0) or 0),
                "completed":     int(r.get("completed_items", 0) or 0),
                "failed":        int(r.get("failed_items",    0) or 0),
                "pending":       pending,
                "done":          done,
                "source_domain": r.get("source_domain",       ""),
                "dest_domain":   r.get("destination_domain",  ""),
                "resumable": (
                    r.get("status", "").upper() != "COMPLETED" and pending > 0
                ),
            })

        return result
    finally:
        conn.close()


def _fetch_run_status_from_sql(run_id: str) -> dict:
    """Quick status snapshot — used when run is not in memory (post VM restart)."""
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

        cur.execute("""
            SELECT
                SUM(status IN ('PENDING','FAILED','IN_PROGRESS')) AS pending,
                SUM(status = 'DONE')                              AS done_count
              FROM migration_items WHERE migration_id = %s AND is_folder = 0
        """, (run_id,))
        counts  = cur.fetchone() or {}
        pending = int(counts.get("pending")    or 0)
        done    = int(counts.get("done_count") or 0)

        return {
            "status":     row.get("status", "UNKNOWN"),
            "start_time": str(row["start_time"]) if row.get("start_time") else None,
            "end_time":   str(row["end_time"])   if row.get("end_time")   else None,
            "totals": {
                "files_total":    pending + done,
                "files_done":     done,
                "files_migrated": int(row.get("completed_items", 0) or 0),
                "files_failed":   int(row.get("failed_items",    0) or 0),
                "files_skipped":  0,
                "files_ignored":  0,
                "pending":        pending,
            },
        }
    finally:
        conn.close()


def _fetch_run_summary_from_sql(run_id: str) -> dict:
    """Minimal summary for the SSE done event on client reconnect."""
    status = _fetch_run_status_from_sql(run_id)
    if not status:
        return {}
    totals = status.get("totals", {})
    done   = totals.get("files_migrated", 0)
    total  = totals.get("files_total",    0)
    return {
        "run_id":               run_id,
        "status":               status.get("status"),
        "accuracy_rate":        (done / total * 100) if total > 0 else 0.0,
        "total_files_migrated": done,
        "total_files_failed":   totals.get("files_failed", 0),
        "pending":              totals.get("pending",       0),
        "start_time":           status.get("start_time"),
        "end_time":             status.get("end_time"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _register_run(run_id: str, total_users: int):
    """Create in-memory run entry. Must be called inside _runs_lock."""
    _runs[run_id] = {
        "status":  "running",
        "summary": None,
        "queue":   _queue.Queue(),
        "totals": {
            "total_users":     total_users,
            "completed_users": 0,
            "failed_users":    0,
            "files_migrated":  0,
            "files_failed":    0,
            "files_skipped":   0,
            "files_ignored":   0,
            "folders_created": 0,
            "files_done":      0,
            "files_total":     0,
        },
    }


def _accumulate_totals(totals: dict, result: dict):
    totals["files_done"] += 1
    if result.get("total"):
        totals["files_total"] = result["total"]
    if result.get("success"):   totals["files_migrated"] += 1
    elif result.get("skipped"): totals["files_skipped"]  += 1
    elif result.get("ignored"): totals["files_ignored"]  += 1
    else:                       totals["files_failed"]   += 1


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
