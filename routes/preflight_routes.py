# routes/preflight_routes.py
"""
routes/preflight_routes.py

  POST /api/preflight — run all 4 enterprise readiness checks:

  Sources of truth:
    - Service account JSON files  → state.SOURCE/DEST_CREDENTIALS_PATH  (flask uploads)
    - Admin email / domain        → state.config (set via /api/config form)
    - Cloud SQL creds             → amey/config.py  Config.DB_*
    - GCS bucket creds            → amey/config.py  Config.GCS_SERVICE_ACCOUNT_FILE
                                    + Config.GCS_BUCKET_NAME

  Storage logic mirrors the Google Apps Script approach:
    Admin Reports API  users/<email>/dates/<date>
    parameters=accounts:drive_used_quota_in_mb,
               accounts:gmail_used_quota_in_mb,
               accounts:gplus_photos_used_quota_in_mb
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
    body = request.get_json(silent=True) or {}
    sid  = body.get("sessionId") or state.session_id
    if sid and sid != state.session_id:
        state.new_session(sid, auto_delete_previous=False)

    if not BACKEND_DIR.exists():
        err = _fail("Backend repo not found at ~/amey")
        return jsonify({
            "overall": False,
            "checks": {
                "service_account":   err,
                "domain_delegation": err,
                "cloud_sql":         err,
                "gcs_bucket":        err,
            },
        }), 500

    _ensure_backend_on_path()

    from routes.config_routes import _apply_config_to_backend
    _apply_config_to_backend()

    results = {
        "service_account":   _check_service_account(),
        "domain_delegation": _check_domain_delegation(),
        "cloud_sql":         _check_cloud_sql(),
        "gcs_bucket":        _check_gcs_bucket(),
    }

    overall = all(v["ok"] for v in results.values())
    current_app.logger.info(
        f"[preflight] overall={overall} | "
        + " | ".join(f"{k}={v['ok']}" for k, v in results.items())
    )
    return jsonify({"overall": overall, "checks": results})


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1 — Service account credentials
# Source: uploaded JSON files saved to fixed paths in session_state
# ─────────────────────────────────────────────────────────────────────────────

def _check_service_account() -> dict:
    try:
        import json
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request

        pairs = [
            ("Source",      state.SOURCE_CREDENTIALS_PATH,
                            state.config.get("source_admin_email") or ""),
            ("Destination", state.DEST_CREDENTIALS_PATH,
                            state.config.get("dest_admin_email") or ""),
        ]

        for label, path, admin_email in pairs:
            if not path.is_absolute():
                return _fail(f"{label} credentials path not absolute: '{path}'")
            if not path.exists():
                return _fail(
                    f"{label} credentials file missing at: {path}. "
                    f"Upload it via /api/config."
                )

            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError as e:
                return _fail(f"{label} credentials is not valid JSON: {e}")

            required = {"type", "project_id", "private_key_id",
                        "private_key", "client_email"}
            missing = required - data.keys()
            if missing:
                return _fail(
                    f"{label} credentials missing fields: {', '.join(sorted(missing))}"
                )
            if data.get("type") != "service_account":
                return _fail(
                    f"{label} type='{data.get('type')}', expected 'service_account'"
                )

            # Actually authenticate — catches bad private keys, revoked keys, etc.
            if not admin_email:
                return _fail(
                    f"{label} admin email not set. "
                    f"Save config via /api/config first."
                )
            try:
                from config import Config
                creds = service_account.Credentials.from_service_account_file(
                    str(path), scopes=Config.SCOPES
                ).with_subject(admin_email)
                creds.refresh(Request())
            except Exception as e:
                return _fail(
                    f"{label} service account failed to authenticate "
                    f"(impersonating {admin_email}): {e}"
                )

        return _ok(
            "Both service account credentials are valid and authenticate successfully",
            f"{state.SOURCE_CREDENTIALS_PATH.name} ✓  "
            f"{state.DEST_CREDENTIALS_PATH.name} ✓",
        )

    except Exception as e:
        return _fail(f"Unexpected error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 2 — Domain-wide delegation
# Source: credential files (uploads) + domain/admin from state.config (form)
# Uses the same Admin SDK call pattern as the Apps Script
# ─────────────────────────────────────────────────────────────────────────────

def _check_domain_delegation() -> dict:
    try:
        from config import Config
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        errors = []

        checks = [
            ("Source",
             str(state.SOURCE_CREDENTIALS_PATH),
             Config.SOURCE_ADMIN_EMAIL,
             Config.SOURCE_DOMAIN),
            ("Destination",
             str(state.DEST_CREDENTIALS_PATH),
             Config.DEST_ADMIN_EMAIL,
             Config.DEST_DOMAIN),
        ]

        for label, creds_file, admin_email, domain in checks:
            if not Path(creds_file).exists():
                errors.append(f"{label} credentials file missing: {creds_file}")
                continue
            try:
                # Impersonate the domain admin explicitly — this is the delegation proof
                creds = service_account.Credentials.from_service_account_file(
                    creds_file, scopes=Config.SCOPES
                ).with_subject(admin_email)
                creds.refresh(Request())

                svc = build("admin", "directory_v1",
                            credentials=creds, cache_discovery=False)
                result = svc.users().list(
                    domain=domain, maxResults=1
                ).execute()

                user_count = len(result.get("users", []))
                current_app.logger.info(
                    f"[preflight] {label} delegation OK — "
                    f"listed {user_count} user(s) on {domain}"
                )
            except Exception as e:
                errors.append(f"{label} delegation failed (admin={admin_email}): {e}")

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
# Source: amey/config.py  Config.DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
# Uses mysql.connector to match the actual driver in config.py
# ─────────────────────────────────────────────────────────────────────────────

def _check_cloud_sql() -> dict:
    try:
        from config import Config

        # Config.get_db_connection() uses mysql.connector with the correct creds
        conn   = Config.get_db_connection()
        cursor = conn.cursor()

        # CREATE probe table (MySQL syntax — no SERIAL, no RETURNING)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _preflight_probe (
                id   INT AUTO_INCREMENT PRIMARY KEY,
                val  VARCHAR(64) NOT NULL,
                ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute(
            "INSERT INTO _preflight_probe (val) VALUES (%s)", ("preflight-check",)
        )
        row_id = cursor.lastrowid   # mysql.connector sets this after INSERT

        cursor.execute(
            "SELECT val FROM _preflight_probe WHERE id = %s", (row_id,)
        )
        fetched = cursor.fetchone()[0]

        cursor.execute(
            "DELETE FROM _preflight_probe WHERE id = %s", (row_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()

        if fetched != "preflight-check":
            return _fail(
                f"Cloud SQL read-back mismatch: "
                f"expected 'preflight-check', got '{fetched}'"
            )

        return _ok(
            "Cloud SQL connection healthy — read/write/delete probe passed",
            f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME} "
            f"(user: {Config.DB_USER})",
        )

    except ImportError:
        return _fail(
            "mysql-connector-python not installed — "
            "run: pip install mysql-connector-python"
        )
    except Exception as e:
        return _fail(f"Cloud SQL error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4 — GCS bucket access
# Source: amey/config.py  Config.GCS_SERVICE_ACCOUNT_FILE + Config.GCS_BUCKET_NAME
# NOT the Drive service account — GCS needs its own key with Storage Object Admin
# ─────────────────────────────────────────────────────────────────────────────

def _check_gcs_bucket() -> dict:
    try:
        from config import Config
        from google.cloud import storage
        from google.oauth2 import service_account

        bucket_name   = getattr(Config, "GCS_BUCKET_NAME", None)
        gcs_creds_file = getattr(Config, "GCS_SERVICE_ACCOUNT_FILE", None)

        if not bucket_name:
            return _fail("GCS_BUCKET_NAME not set in amey/config.py")

        if not gcs_creds_file:
            return _fail("GCS_SERVICE_ACCOUNT_FILE not set in amey/config.py")

        if not Path(gcs_creds_file).exists():
            return _fail(
                f"GCS service account key not found: '{gcs_creds_file}'. "
                f"This must be a key with Storage Object Admin on "
                f"gs://{bucket_name} — separate from the Drive SA key."
            )

        sa_creds = service_account.Credentials.from_service_account_file(
            gcs_creds_file,
            scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
        )
        client = storage.Client(
            project=sa_creds.project_id,
            credentials=sa_creds,
        )

        bucket     = client.bucket(bucket_name)
        blob_name  = "_preflight_probe/check.txt"
        probe_data = b"preflight-ok"

        blob = bucket.blob(blob_name)
        blob.upload_from_string(probe_data, content_type="text/plain")

        fetched = blob.download_as_bytes()
        if fetched != probe_data:
            return _fail(f"GCS read-back mismatch: got {fetched!r}")

        blob.delete()

        return _ok(
            f"GCS bucket '{bucket_name}' accessible — "
            f"read/write/delete probe passed",
            f"gs://{bucket_name}  |  SA: {gcs_creds_file}",
        )

    except ImportError:
        return _fail(
            "google-cloud-storage not installed — "
            "run: pip install google-cloud-storage"
        )
    except Exception as e:
        return _fail(f"GCS error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ok(message: str, detail: str = "") -> dict:
    return {"ok": True,  "message": message, "detail": detail}

def _fail(message: str, detail: str = "") -> dict:
    return {"ok": False, "message": message, "detail": detail}

def _ensure_backend_on_path():
    s = str(BACKEND_DIR)
    if s not in sys.path:
        sys.path.insert(0, s)
