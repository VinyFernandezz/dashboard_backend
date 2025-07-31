import os
from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Register blueprints
    from services.matriculas import bp as matriculas_bp
    from services.health     import bp as health_bp
    from services.courses    import bp as courses_bp
    from services.years_suap import bp as years_suap_bp 

    app.register_blueprint(health_bp)      # /health
    app.register_blueprint(matriculas_bp)  # /matriculas
    app.register_blueprint(courses_bp)     # /cursos
    app.register_blueprint(years_suap_bp)  # /years_suap

    return app

# Exponha o app para o Gunicorn
app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
