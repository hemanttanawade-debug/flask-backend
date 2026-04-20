import os
from functools import wraps
from flask import Blueprint, request, jsonify, current_app, g
from google.oauth2 import id_token
from google.auth.transport import requests as grequests

auth_bp = Blueprint("auth", __name__)

# Config from Environment
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
if not CLIENT_ID:
    raise RuntimeError("GOOGLE_CLIENT_ID environment variable not set")

ALLOWED_USERS = [
    "hemant@dev.shivaami.in",
    "uzer@dev.shivaami.in","hemant.tanawade@shivaami.com"
]

def require_auth(f):
    """Decorator — validates the Google ID Token directly on every request."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or malformed token"}), 401
        
        token = auth_header.split(" ", 1)[1]

        try:
            # Re-verify the Google Token directly
            # This handles signature, expiry, and audience (CLIENT_ID)
            idinfo = id_token.verify_oauth2_token(
                token, 
                grequests.Request(), 
                CLIENT_ID
            )
            
            # Check Whitelist
            email = idinfo.get("email")
            if email not in ALLOWED_USERS:
                return jsonify({"error": f"User {email} not authorized"}), 403
            
            # Store in 'g' for use in the actual route function
            g.user_email = email
            g.user_info = idinfo
            
        except ValueError as e:
            return jsonify({"error": f"Invalid or expired Google token: {e}"}), 401

        return f(*args, **kwargs)
    return decorated

@auth_bp.route("/auth/verify", methods=["POST"])
def verify_initial_login():
    """
    Called once by React after Google Login.
    Checks if the user is in the whitelist before React allows them into the UI.
    """
    data = request.get_json(silent=True) or {}
    google_token = data.get("token")

    if not google_token:
        return jsonify({"error": "No token provided"}), 400

    try:
        idinfo = id_token.verify_oauth2_token(google_token, grequests.Request(), CLIENT_ID)
        email = idinfo.get("email")

        if email not in ALLOWED_USERS:
            return jsonify({"error": "Access denied"}), 403

        # Success - just return the user info. 
        # React will keep using the SAME google_token for other requests.
        return jsonify({
            "status": "authenticated",
            "email": email,
            "name": idinfo.get("name")
        })
    except ValueError:
        return jsonify({"error": "Invalid Google token"}), 401

@auth_bp.route("/auth/me", methods=["GET"])
@require_auth
def me():
    """Returns info for the currently logged-in user."""
    return jsonify({
        "email": g.user_email,
        "name": g.user_info.get("name"),
        "picture": g.user_info.get("picture")
    })
