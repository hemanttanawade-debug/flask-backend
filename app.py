"""
Flask Application Factory
Enterprise Drive Migration - API Server
"""
# app.py — correct order
from dotenv import load_dotenv
load_dotenv()  # ← MUST be before any blueprint import

from flask import Flask
from flask_cors import CORS

from routes.config_routes import config_bp
from routes.migration_routes import migration_bp
from routes.status_routes import status_bp
from routes.auth_routes import auth_bp 
from routes.preflight_routes import preflight_bp 

def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB max upload

    # Allow requests from the React frontend (Vite dev server + production)
    # Fix
    CORS(app, resources={r"/api/*": {
    "origins": ["https://migration.shivaami.in"],
    "allow_headers": ["Authorization", "Content-Type"],
    "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "supports_credentials": True
    }})


    # Register blueprints
    app.register_blueprint(auth_bp,      url_prefix="/api")
    app.register_blueprint(config_bp,    url_prefix="/api")
    app.register_blueprint(migration_bp, url_prefix="/api")
    app.register_blueprint(status_bp,    url_prefix="/api")
    app.register_blueprint(preflight_bp, url_prefix="/api")
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=8000, debug=True)
