"""
session_state.py

In-memory store for the current migration session.

KEY DESIGN DECISIONS
────────────────────
1. Credential files are ALWAYS saved to fixed paths — no session subfolders:
       uploads/credential/source_credentials.json
       uploads/credential/dest_credentials.json
   Uploading a new file simply overwrites the existing one in-place.

2. User CSV is saved to a fixed path:
       uploads/users.csv
   Overwritten on each upload — no session subfolder.

3. Starting a new session resets in-memory state only.
   Credential files on disk are intentionally kept so the user does not
   have to re-upload them on every page refresh. They are only replaced
   when the user uploads new files.

4. Credential security:
   - Files saved with mode 0o600 (owner read/write only).
   - Contents never stored in memory or logged.
   - Never sent back to the frontend in any response.

5. Only one migration can RUN at a time (one background thread).
"""

import os
import threading
from pathlib import Path

UPLOAD_DIR     = Path(__file__).parent / "uploads"
CREDENTIAL_DIR = UPLOAD_DIR / "credential"

# Ensure both dirs exist at import time
UPLOAD_DIR.mkdir(exist_ok=True)
CREDENTIAL_DIR.mkdir(exist_ok=True)

# ── Fixed file paths — these never change regardless of session ───────────────
SOURCE_CREDENTIALS_PATH = CREDENTIAL_DIR / "source_credentials.json"
DEST_CREDENTIALS_PATH   = CREDENTIAL_DIR / "dest_credentials.json"
CSV_PATH                = UPLOAD_DIR / "users.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Current wizard session
# ─────────────────────────────────────────────────────────────────────────────
session_id: str | None = None

config = {
    "source_domain":           "",
    "source_admin_email":      "",
    # Always points to the same fixed file — never a session subfolder
    "source_credentials_file": str(SOURCE_CREDENTIALS_PATH),

    "dest_domain":             "",
    "dest_admin_email":        "",
    # Always points to the same fixed file — never a session subfolder
    "dest_credentials_file":   str(DEST_CREDENTIALS_PATH),

    "migration_mode":          "full",
}

user_mappings = []               # [{"sourceUser": ..., "destinationUser": ...}]
csv_file_path: str = str(CSV_PATH)   # fixed — always uploads/users.csv

# ─────────────────────────────────────────────────────────────────────────────
# Active migration
# ─────────────────────────────────────────────────────────────────────────────
migration = {
    "migration_id":   None,
    "session_id":     None,
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

def new_session(sid: str, auto_delete_previous: bool = False):
    """
    Reset in-memory wizard state for a new migration attempt.

    auto_delete_previous is False by default because credential files now live
    at a fixed location and must NOT be deleted between sessions — they are
    only overwritten when the user explicitly uploads new files.
    """
    global session_id, config, user_mappings, csv_file_path

    session_id    = sid
    user_mappings = []
    csv_file_path = str(CSV_PATH)

    # Reset domain/email fields but keep credential paths pointing at fixed files
    config = {
        "source_domain":           "",
        "source_admin_email":      "",
        "source_credentials_file": str(SOURCE_CREDENTIALS_PATH),

        "dest_domain":             "",
        "dest_admin_email":        "",
        "dest_credentials_file":   str(DEST_CREDENTIALS_PATH),

        "migration_mode":          "full",
    }


def credentials_exist() -> dict[str, bool]:
    """
    Check whether the fixed credential files are present on disk.
    Used by /api/validate to surface a clear error before attempting auth.
    """
    return {
        "source": SOURCE_CREDENTIALS_PATH.exists(),
        "dest":   DEST_CREDENTIALS_PATH.exists(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Secure file saving
# ─────────────────────────────────────────────────────────────────────────────

def save_credential_file(file_storage, kind: str) -> str:
    """
    Save an uploaded credential file to the fixed credential directory,
    always using the canonical filename. Overwrites in-place.

    kind must be "source" or "dest".

    Example:
        user uploads  →  'my-company-sa-key.json'
        saved as      →  'uploads/credential/source_credentials.json'

    Permissions set to 0o600 (owner read/write only).
    Returns the absolute path string.
    """
    if kind not in ("source", "dest"):
        raise ValueError(f"kind must be 'source' or 'dest', got: {kind!r}")

    dest_path = SOURCE_CREDENTIALS_PATH if kind == "source" else DEST_CREDENTIALS_PATH

    CREDENTIAL_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(str(dest_path))
    os.chmod(str(dest_path), 0o600)
    return str(dest_path)


def save_csv_file(file_storage) -> str:
    """
    Save the uploaded user-mapping CSV to the fixed path uploads/users.csv.
    Overwrites any previous upload.
    Returns the absolute path string.
    """
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(str(CSV_PATH))
    return str(CSV_PATH)
