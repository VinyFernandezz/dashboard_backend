from flask import Blueprint, jsonify, request
import os
import mysql.connector

bp = Blueprint("matriculas", __name__, url_prefix="/matriculas")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "10.17.34.202"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("",  methods=["GET"])
def get_matriculas():
    ano_inicio = int(request.args.get("inicio", 2010))
    ano_fim    = int(request.args.get("fim",    2025))

    # Conexão ao banco (substitua por seu host real ou use mock)
    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=5)
    cursor = conn.cursor()
    query = """
        SELECT YEAR(FROM_UNIXTIME(timecreated)) ano, COUNT(*) total
        FROM mdl_user_enrolments
        WHERE YEAR(FROM_UNIXTIME(timecreated)) BETWEEN %s AND %s
        GROUP BY ano
        ORDER BY ano
    """
    cursor.execute(query, (ano_inicio, ano_fim))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    dados = [{"ano": r[0], "total": r[1]} for r in results]
    return jsonify(dados)