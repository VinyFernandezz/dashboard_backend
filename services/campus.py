from flask import Blueprint, jsonify
import os
import mysql.connector

bp = Blueprint("campus", __name__, url_prefix="/campus")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("", methods=["GET"])
def list_campi():
    """
    Retorna lista distinta de campi em formato de array de strings.
    Usa a coluna `Campus` da tabela `suap_students`.
    """
    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=8)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT TRIM(`Campus`) AS campus
        FROM suap_students
        WHERE `Campus` IS NOT NULL AND TRIM(`Campus`) <> ''
        ORDER BY campus
        """
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify([r[0] for r in rows])
