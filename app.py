import os
from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Register blueprints
    from services.matriculas import bp as matriculas_bp
    from services.health     import bp as health_bp

    app.register_blueprint(health_bp)      # /health
    app.register_blueprint(matriculas_bp)  # /matriculas

    return app

if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
