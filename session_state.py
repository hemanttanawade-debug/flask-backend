"""
session_state.py

In-memory store for the current migration session.

KEY DESIGN DECISIONS
────────────────────
1. Uploaded files are always renamed to fixed names:
       uploads/<session_id>/source_credentials.json
       uploads/<session_id>/dest_credentials.json
       uploads/<session_id>/users.csv
   The user's original filename is irrelevant.

2. Starting a new session (POST /api/config/new) auto-deletes the
   previous session's uploaded files from disk — no stale credentials
   or CSVs accumulate. The user can also manually trigger cleanup via
   DELETE /api/migration/<id>/cleanup.

3. Credential security:
   - Files saved with mode 0o600 (owner read/write only).
   - Contents never stored in memory or logged.
   - Never sent back to the frontend in any response.

4. Only one migration can RUN at a time (one background thread).
"""

import os
import shutil
import threading
from pathlib import Path

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Current wizard session
# ─────────────────────────────────────────────────────────────────────────────
session_id: str | None = None

config = {
    "source_domain":           "",
    "source_admin_email":      "",
    "source_credentials_file": "",   # always: uploads/<sid>/source_credentials.json

    "dest_domain":             "",
    "dest_admin_email":        "",
    "dest_credentials_file":   "",   # always: uploads/<sid>/dest_credentials.json
}

user_mappings = []    # [{"sourceUser": ..., "destinationUser": ...}]
csv_file_path: str | None = None   # always: uploads/<sid>/users.csv

# ─────────────────────────────────────────────────────────────────────────────
# Active migration
# ─────────────────────────────────────────────────────────────────────────────
migration = {
    "migration_id":   None,
    "session_id":     None,   # which session's files to use
    "status":         "idle",
    "total_users":    0,
    "files_migrated": 0,
    "failed_files":   0,
    "logs":           [],
}

# History: { migration_id: snapshot_dict }
all_migrations: dict = {}

_migration_thread: threading.Thread | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Session management
# ─────────────────────────────────────────────────────────────────────────────

def new_session(sid: str, auto_delete_previous: bool = True):
    """
    Reset wizard state for a brand-new migration.
    If auto_delete_previous=True, deletes the previous session's upload folder.
    """
    global session_id, config, user_mappings, csv_file_path

    # Delete previous session files if requested and session differs
    if auto_delete_previous and session_id and session_id != sid:
        cleanup_session_files(session_id)

    session_id    = sid
    user_mappings = []
    csv_file_path = None
    config = {
        "source_domain":           "",
        "source_admin_email":      "",
        "source_credentials_file": "",
        "dest_domain":             "",
        "dest_admin_email":        "",
        "dest_credentials_file":   "",
    }


def cleanup_session_files(sid: str) -> list[str]:
    """
    Delete all uploaded files for a given session.
    Returns list of deleted paths (for response feedback).
    Called explicitly via DELETE /api/migration/<id>/cleanup
    or automatically when a new session starts.
    """
    deleted = []
    folder  = UPLOAD_DIR / sid
    if folder.exists():
        try:
            shutil.rmtree(str(folder))
            deleted.append(str(folder))
        except Exception as e:
            pass   # log in caller if needed
    return deleted


# ─────────────────────────────────────────────────────────────────────────────
# Secure file saving
# ─────────────────────────────────────────────────────────────────────────────

def save_credential_file(file_storage, fixed_filename: str, folder: Path) -> str:
    """
    Save an uploaded file under a fixed name regardless of the user's filename.
    e.g.  user uploads 'my-company-sa-key.json'
          saved as     'source_credentials.json'

    Permissions set to 0o600 (owner read/write only).
    File contents are never stored in memory beyond this write.
    Returns absolute path string.
    """
    folder.mkdir(parents=True, exist_ok=True)
    dest_path = folder / fixed_filename
    file_storage.save(str(dest_path))
    os.chmod(str(dest_path), 0o600)
    return str(dest_path)