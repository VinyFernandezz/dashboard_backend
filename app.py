import os
from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Register blueprints
    from services.health import bp as health_bp
    from services.courses import bp as courses_bp
    
    # --- UPDATED: Import the two new main service files ---
    from services.enrollments import bp as enrollments_bp
    from services.analysis import bp as analysis_bp 

    app.register_blueprint(health_bp)       # /health
    app.register_blueprint(courses_bp)      # /courses
    
    app.register_blueprint(enrollments_bp)  # /enrollments
    app.register_blueprint(analysis_bp)     # /analysis

    return app

# Expose the app for Gunicorn
app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)