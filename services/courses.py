from flask import Blueprint, jsonify, request
import os
import mysql.connector

bp = Blueprint("matriculas", __name__, url_prefix="/courses")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("",  methods=["GET"])
def get_courses():
    # Conexão ao banco (substitua por seu host real ou use mock)
    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=5)
    cursor = conn.cursor()
    
    query = """
        SELECT id, fullname FROM mdl_course
    """
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()

    dados = [{"id": r[0], "nome": r[1]} for r in results]
    
    return jsonify({"cursos": dados})