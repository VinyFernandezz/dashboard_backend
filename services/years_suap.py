from flask import Blueprint, jsonify, request
import os
import mysql.connector

bp = Blueprint("year", __name__, url_prefix="/years_suap")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("",  methods=["GET"])
def get_years():
    # Conexão ao banco (substitua por seu host real ou use mock)
    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=5)
    cursor = conn.cursor()
    query = """
        SELECT DISTINCT `Year of Entry` AS ano
        FROM suap_students
        WHERE `Year of Entry` IS NOT NULL
        ORDER BY ano;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    dados = [{"ano": r[0]} for r in results]
    return jsonify(dados)
