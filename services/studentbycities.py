from flask import Blueprint, jsonify, request
import os
import mysql.connector

bp = Blueprint("studentbycities", __name__, url_prefix="/studentbycities")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("",  methods=["GET"])
def get_studentbycities():
    from flask import request, jsonify
    import mysql.connector
    import pandas as pd

    ano_inicio = int(request.args.get("inicio", 2010))
    ano_fim    = int(request.args.get("fim", 2025))
    courses    = request.args.get("cursos", "").split(",")
    typelocal  = request.args.get("typelocal", "municipio").lower()
    modalility = request.args.get("coverage", "EAD").upper() 
    
    if not courses or courses == [""]:
        courses = None

    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=5)
    cursor = conn.cursor()

    if typelocal == "municipio":
        base_query = """
            SELECT 
                `City of Residence` AS municipio,
                COUNT(*) AS total
            FROM 
                suap_students
            WHERE 
                `State` = "CE"
                AND `Year of Entry` BETWEEN %s AND %s
        """
        params = [ano_inicio, ano_fim]
        
        if modalility=="EAD":
            base_query += " AND UPPER(`Nature of Participation`) = %s"
            params.append(modalility)

        if courses:
            placeholders = ', '.join(['%s'] * len(courses))
            base_query += f" AND `Nome do Curso` IN ({placeholders})"
            params.extend(courses)

        base_query += """
            GROUP BY `City of Residence`
            ORDER BY municipio ASC;
        """

    else:  # assume "estado"
        base_query = """
            SELECT 
                `State` AS estado,
                COUNT(*) AS total
            FROM 
                suap_students
            WHERE 
                `Year of Entry` BETWEEN %s AND %s
        """
        params = [ano_inicio, ano_fim]

        if modalility=="EAD":
            base_query += " AND UPPER(`Nature of Participation`) = %s"
            params.append(modalility)


        if courses:
            placeholders = ', '.join(['%s'] * len(courses))
            base_query += f" AND `Nome do Curso` IN ({placeholders})"
            params.extend(courses)

        base_query += """
            GROUP BY `State`
            ORDER BY estado ASC;
        """

    cursor.execute(base_query, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if typelocal == "municipio":
        df = pd.DataFrame(results, columns=["municipio", "total"])
        df["municipio"] = df["municipio"].str.replace("-CE", "", regex=False).str.strip()
    else:
        df = pd.DataFrame(results, columns=["estado", "total"])

    return jsonify(df.to_dict(orient="records"))