# routes/preflight_routes.py
"""
routes/preflight_routes.py

  POST /api/preflight — run all 4 enterprise readiness checks:
    1. Service account credentials (source + dest JSON loads & authenticates)
    2. Domain-wide delegation      (admin can impersonate users on both domains)
    3. Cloud SQL connection        (backend can read/write checkpoint state)
    4. GCS bucket access           (backend can read/write staging bucket)

  Returns per-check results so the frontend can show granular pass/fail.
"""

import sys
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from routes.auth_routes import require_auth
import session_state as state

preflight_bp = Blueprint("preflight", __name__)
BACKEND_DIR  = Path.home() / "amey"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/preflight
# ─────────────────────────────────────────────────────────────────────────────

@preflight_bp.route("/preflight", methods=["POST"])
@require_auth
def run_preflight():
    """
    Accepts JSON body: { "sessionId": "<sid>" }

    Runs 4 checks and returns:
    {
      "overall": true | false,
      "checks": {
        "service_account": { "ok": true,  "message": "...", "detail": "..." },
        "domain_delegation": { "ok": false, "message": "...", "detail": "..." },
        "cloud_sql":         { "ok": true,  "message": "...", "detail": "..." },
        "gcs_bucket":        { "ok": true,  "message": "...", "detail": "..." }
      }
    }
    """
    body = request.get_json(silent=True) or {}
    sid  = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    # Ensure backend is importable
    if not BACKEND_DIR.exists():
        return jsonify({
            "overall": False,
            "checks": {
                "service_account":  _fail("Backend repo not found at ~/amey"),
                "domain_delegation": _fail("Backend repo not found at ~/amey"),
                "cloud_sql":         _fail("Backend repo not found at ~/amey"),
                "gcs_bucket":        _fail("Backend repo not found at ~/amey"),
            },
        }), 500

    _ensure_backend_on_path()

    # Apply in-memory config to backend Config class before running checks
    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    results = {}
    results["service_account"]   = _check_service_account()
    results["domain_delegation"] = _check_domain_delegation()
    results["cloud_sql"]         = _check_cloud_sql()
    results["gcs_bucket"]        = _check_gcs_bucket()

    overall = all(v["ok"] for v in results.values())

    current_app.logger.info(
        f"[preflight] overall={overall} | "
        + " | ".join(f"{k}={v['ok']}" for k, v in results.items())
    )

    return jsonify({"overall": overall, "checks": results})


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1 — Service account credentials
# ─────────────────────────────────────────────────────────────────────────────

def _check_service_account() -> dict:
    try:
        import json

        # Guard: ensure paths are absolute, not bare filenames
        for label, path in [
            ("Source",      state.SOURCE_CREDENTIALS_PATH),
            ("Destination", state.DEST_CREDENTIALS_PATH),
        ]:
            if not path.is_absolute():
                return _fail(
                    f"{label} credentials path is not absolute: '{path}'. "
                    f"This is a server configuration error — check session_state.py."
                )

        creds = state.credentials_exist()
        if not creds["source"]:
            return _fail(
                f"Source credentials file missing at: {state.SOURCE_CREDENTIALS_PATH}"
            )
        if not creds["dest"]:
            return _fail(
                f"Destination credentials file missing at: {state.DEST_CREDENTIALS_PATH}"
            )

        issues = []
        for label, path in [
            ("Source",      state.SOURCE_CREDENTIALS_PATH),
            ("Destination", state.DEST_CREDENTIALS_PATH),
        ]:
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError as e:
                issues.append(f"{label} credentials is not valid JSON: {e}")
                continue

            required_keys = {"type", "project_id", "private_key_id",
                             "private_key", "client_email"}
            missing = required_keys - data.keys()
            if missing:
                issues.append(
                    f"{label} credentials missing fields: {', '.join(sorted(missing))}"
                )
                continue

            if data.get("type") != "service_account":
                issues.append(
                    f"{label} type is '{data.get('type')}' — expected 'service_account'"
                )

        if issues:
            return _fail(" | ".join(issues))

        return _ok(
            "Both service account JSON files are valid",
            f"{state.SOURCE_CREDENTIALS_PATH} ✓  {state.DEST_CREDENTIALS_PATH} ✓",
        )

    except Exception as e:
        return _fail(f"Unexpected error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 2 — Domain-wide delegation
# ─────────────────────────────────────────────────────────────────────────────

def _check_domain_delegation() -> dict:
    """
    Attempt to impersonate the configured admin email on BOTH domains
    and call users.list() on each. This proves domain-wide delegation
    is granted in the Admin Console for both service accounts.
    """
    try:
        from config import Config
        from auth import DomainAuthManager

        auth = DomainAuthManager(
            source_config={
                "domain":           Config.SOURCE_DOMAIN,
                "credentials_file": Config.SOURCE_CREDENTIALS_FILE,
                "admin_email":      Config.SOURCE_ADMIN_EMAIL,
            },
            dest_config={
                "domain":           Config.DEST_DOMAIN,
                "credentials_file": Config.DEST_CREDENTIALS_FILE,
                "admin_email":      Config.DEST_ADMIN_EMAIL,
            },
            scopes=Config.SCOPES,
        )

        errors = []

        # Source delegation check
        try:
            src_admin = auth.source_auth.get_admin_service()
            src_admin.users().list(
                domain=Config.SOURCE_DOMAIN,
                maxResults=1,
            ).execute()
        except Exception as e:
            errors.append(f"Source delegation failed: {e}")

        # Destination delegation check
        try:
            dst_admin = auth.dest_auth.get_admin_service()
            dst_admin.users().list(
                domain=Config.DEST_DOMAIN,
                maxResults=1,
            ).execute()
        except Exception as e:
            errors.append(f"Destination delegation failed: {e}")

        if errors:
            return _fail(" | ".join(errors))

        return _ok(
            "Domain-wide delegation confirmed on both domains",
            f"{Config.SOURCE_DOMAIN} ✓  {Config.DEST_DOMAIN} ✓",
        )

    except Exception as e:
        return _fail(f"Delegation check error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 3 — Cloud SQL connection
# ─────────────────────────────────────────────────────────────────────────────

def _check_cloud_sql() -> dict:
    """
    Instantiate SQLStateManager (which opens a Cloud SQL connection)
    and run a minimal read/write probe:
      - INSERT a test row into a scratch table
      - SELECT it back
      - DELETE it
    Proves the backend can checkpoint migration state to Cloud SQL.
    """
    try:
        from config import Config
        from sql_state_manager import SQLStateManager

        mgr  = SQLStateManager(Config)
        conn = mgr.get_conn()

        with conn.cursor() as cur:
            # Use a dedicated probe table — never touches migration data
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _preflight_probe (
                    id   SERIAL PRIMARY KEY,
                    val  TEXT NOT NULL,
                    ts   TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute(
                "INSERT INTO _preflight_probe (val) VALUES (%s) RETURNING id",
                ("preflight-check",),
            )
            row_id = cur.fetchone()[0]
            cur.execute(
                "SELECT val FROM _preflight_probe WHERE id = %s", (row_id,)
            )
            fetched = cur.fetchone()[0]
            cur.execute(
                "DELETE FROM _preflight_probe WHERE id = %s", (row_id,)
            )
            conn.commit()

        if fetched != "preflight-check":
            return _fail(f"Read-back mismatch: expected 'preflight-check', got '{fetched}'")

        db_info = getattr(Config, "CLOUD_SQL_CONNECTION_NAME", "configured DB")
        return _ok(
            "Cloud SQL connection healthy — read/write probe passed",
            f"Connected to: {db_info}",
        )

    except ImportError:
        return _fail("sql_state_manager module not found in backend")
    except Exception as e:
        return _fail(f"Cloud SQL error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4 — GCS bucket access
# ─────────────────────────────────────────────────────────────────────────────

def _check_gcs_bucket() -> dict:
    """
    Write a tiny probe object to the configured GCS bucket,
    read it back, then delete it.
    Proves the backend service account has storage.objects.* on the bucket.
    """
    try:
        from config import Config
        from google.cloud import storage
        from google.oauth2 import service_account

        bucket_name = getattr(Config, "GCS_BUCKET_NAME", None)
        if not bucket_name:
            return _fail("GCS_BUCKET_NAME not set in Config")

        creds_file = getattr(Config, "SOURCE_CREDENTIALS_FILE", None)
        if not creds_file or not Path(creds_file).exists():
            return _fail("Source credentials file missing — needed for GCS auth")

        sa_creds = service_account.Credentials.from_service_account_file(
            creds_file,
            scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
        )
        client = storage.Client(
            project=sa_creds.project_id,
            credentials=sa_creds,
        )

        bucket     = client.bucket(bucket_name)
        blob_name  = "_preflight_probe/check.txt"
        probe_data = b"preflight-ok"

        # Write
        blob = bucket.blob(blob_name)
        blob.upload_from_string(probe_data, content_type="text/plain")

        # Read back
        fetched = blob.download_as_bytes()
        if fetched != probe_data:
            return _fail(f"GCS read-back mismatch: got {fetched!r}")

        # Delete
        blob.delete()

        return _ok(
            f"GCS bucket '{bucket_name}' is accessible — read/write/delete probe passed",
            f"Bucket: gs://{bucket_name}  |  Probe object: {blob_name}",
        )

    except ImportError:
        return _fail(
            "google-cloud-storage package not installed — "
            "run: pip install google-cloud-storage"
        )
    except Exception as e:
        return _fail(f"GCS error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Result helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ok(message: str, detail: str = "") -> dict:
    return {"ok": True,  "message": message, "detail": detail}

def _fail(message: str, detail: str = "") -> dict:
    return {"ok": False, "message": message, "detail": detail}

def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
