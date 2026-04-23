"""
session_state.py

In-memory + on-disk store for the current migration session.

KEY DESIGN DECISIONS
────────────────────
1. Credential files are ALWAYS saved to fixed paths:
       uploads/credential/source_credentials.json
       uploads/credential/dest_credentials.json

2. User CSV is saved to a fixed path:
       uploads/users.csv

3. SESSION PERSISTENCE:
   All wizard state (session_id, domain, email, migration_mode, csv_file_path)
   is written to uploads/session.json on every change.
   On process start the file is read back automatically — a VM restart,
   gunicorn reload, or OOM-kill does NOT lose wizard state.

4. NEW SESSION POLICY:
   new_session() NO LONGER resets state. It is a no-op that returns the
   existing session_id (creating one only if none exists yet).
   State is wiped ONLY by hard_reset(), called from DELETE /api/reset.

5. Credential security:
   - Files saved with mode 0o600 (owner read/write only).
   - Contents never stored in session.json or logged.
   - Never returned to the frontend.

6. MIGRATION LOCK:
   - acquire_migration_lock() is called when a migration starts.
   - release_migration_lock() is called automatically when it finishes.
   - Lock state is persisted in session.json so it survives VM restarts.
   - hard_reset() is the only operation that overrides the lock.
"""

import os
import json
import uuid
import threading
from pathlib import Path

UPLOAD_DIR     = Path(__file__).parent / "uploads"
CREDENTIAL_DIR = UPLOAD_DIR / "credential"
SESSION_FILE   = UPLOAD_DIR / "session.json"

UPLOAD_DIR.mkdir(exist_ok=True)
CREDENTIAL_DIR.mkdir(exist_ok=True)

SOURCE_CREDENTIALS_PATH = CREDENTIAL_DIR / "source_credentials.json"
DEST_CREDENTIALS_PATH   = CREDENTIAL_DIR / "dest_credentials.json"
CSV_PATH                = UPLOAD_DIR / "users.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Migration lock
# ─────────────────────────────────────────────────────────────────────────────
migration_active: bool = False
_lock_mutex = threading.Lock()


def acquire_migration_lock() -> bool:
    global migration_active
    with _lock_mutex:
        if migration_active:
            return False
        migration_active = True
    _persist()
    return True


def release_migration_lock():
    global migration_active
    with _lock_mutex:
        migration_active = False
    _persist()


def is_migration_active() -> bool:
    with _lock_mutex:
        return migration_active


# ─────────────────────────────────────────────────────────────────────────────
# Wizard state
# ─────────────────────────────────────────────────────────────────────────────
session_id: str | None = None

config = {
    "source_domain":           "",
    "source_admin_email":      "",
    "source_credentials_file": str(SOURCE_CREDENTIALS_PATH),
    "dest_domain":             "",
    "dest_admin_email":        "",
    "dest_credentials_file":   str(DEST_CREDENTIALS_PATH),
    "migration_mode":          "full",
    "last_discovery_run_id":   "",
}

user_mappings  = []
csv_file_path: str = str(CSV_PATH)

# Legacy dict kept for status_routes compatibility
migration = {
    "migration_id":   None,
    "session_id":     None,
    "status":         "idle",
    "total_users":    0,
    "files_migrated": 0,
    "failed_files":   0,
    "logs":           [],
}
all_migrations: dict = {}
_migration_thread: threading.Thread | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────

def _persist():
    """Write current state to uploads/session.json (atomic write)."""
    data = {
        "session_id":       session_id,
        "config":           config,
        "csv_file_path":    csv_file_path,
        "migration_active": migration_active,
    }
    try:
        UPLOAD_DIR.mkdir(exist_ok=True)
        tmp = SESSION_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(SESSION_FILE)   # atomic on POSIX
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"[session_state] persist failed: {e}")


def _load():
    """
    Read uploads/session.json on module import and restore state.
    If the file is missing or corrupt a fresh session is created.
    """
    global session_id, config, csv_file_path, migration_active

    if not SESSION_FILE.exists():
        session_id = str(uuid.uuid4())
        _persist()
        return

    try:
        data = json.loads(SESSION_FILE.read_text(encoding="utf-8"))

        session_id = data.get("session_id") or str(uuid.uuid4())

        # Merge saved config over defaults so new keys are always present
        for k, v in data.get("config", {}).items():
            config[k] = v

        # Always enforce fixed credential paths
        config["source_credentials_file"] = str(SOURCE_CREDENTIALS_PATH)
        config["dest_credentials_file"]   = str(DEST_CREDENTIALS_PATH)

        csv_file_path    = data.get("csv_file_path", str(CSV_PATH))
        migration_active = bool(data.get("migration_active", False))

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            f"[session_state] session.json corrupt, starting fresh: {e}"
        )
        session_id       = str(uuid.uuid4())
        migration_active = False
        _persist()


# ─────────────────────────────────────────────────────────────────────────────
# Session management
# ─────────────────────────────────────────────────────────────────────────────

def new_session(sid: str | None = None, auto_delete_previous: bool = False):
    """
    CHANGED BEHAVIOUR — no longer resets state.

    Old behaviour wiped domain/email/CSV on every call, causing state loss
    on page refresh and during active migrations.

    New behaviour:
      - If a session already exists, do nothing and return.
      - If no session exists yet (after hard_reset or first boot), create one.
      - Persist to disk.

    State is cleared ONLY via hard_reset().
    """
    global session_id

    if session_id:
        return  # already have a session — preserve everything

    session_id = sid or str(uuid.uuid4())
    _persist()


def update_config(**kwargs):
    """
    Merge kwargs into config and persist to disk immediately.
    Use instead of mutating config directly.

    Example:
        state.update_config(source_domain="acme.com", source_admin_email="admin@acme.com")
    """
    config.update(kwargs)
    _persist()


def credentials_exist() -> dict[str, bool]:
    return {
        "source": SOURCE_CREDENTIALS_PATH.exists(),
        "dest":   DEST_CREDENTIALS_PATH.exists(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# File saving
# ─────────────────────────────────────────────────────────────────────────────

def save_credential_file(file_storage, kind: str) -> str:
    if kind not in ("source", "dest"):
        raise ValueError(f"kind must be 'source' or 'dest', got: {kind!r}")
    dest_path = SOURCE_CREDENTIALS_PATH if kind == "source" else DEST_CREDENTIALS_PATH
    CREDENTIAL_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(str(dest_path))
    os.chmod(str(dest_path), 0o600)
    return str(dest_path)


def save_csv_file(file_storage) -> str:
    global csv_file_path
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(str(CSV_PATH))
    csv_file_path = str(CSV_PATH)
    _persist()
    return str(CSV_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# Hard reset — the ONLY path that clears everything
# ─────────────────────────────────────────────────────────────────────────────

def hard_reset():
    """
    Wipe ALL state (in-memory + on-disk) and generate a fresh session.
    Called ONLY from DELETE /api/reset ("Delete all migration data").
    Releases the migration lock unconditionally.
    """
    global session_id, config, user_mappings, csv_file_path, migration_active

    with _lock_mutex:
        migration_active = False

    session_id    = str(uuid.uuid4())
    user_mappings = []
    csv_file_path = str(CSV_PATH)

    config = {
        "source_domain":           "",
        "source_admin_email":      "",
        "source_credentials_file": str(SOURCE_CREDENTIALS_PATH),
        "dest_domain":             "",
        "dest_admin_email":        "",
        "dest_credentials_file":   str(DEST_CREDENTIALS_PATH),
        "migration_mode":          "full",
        "last_discovery_run_id":   "",
    }

    for path in [SOURCE_CREDENTIALS_PATH, DEST_CREDENTIALS_PATH]:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
    try:
        CSV_PATH.unlink(missing_ok=True)
    except Exception:
        pass

    migration.update({
        "migration_id":   None,
        "session_id":     None,
        "status":         "idle",
        "total_users":    0,
        "files_migrated": 0,
        "failed_files":   0,
        "logs":           [],
    })
    all_migrations.clear()

    _persist()   # write clean slate to disk


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap — runs once when this module is first imported
# ─────────────────────────────────────────────────────────────────────────────
_load()
