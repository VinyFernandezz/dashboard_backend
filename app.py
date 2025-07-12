import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# Configuração do banco de dados (pode mover para variáveis de ambiente)
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', '10.17.34.202'),
    'user': os.environ.get('DB_USER', 'danilo'),
    'password': os.environ.get('DB_PASS', '5yZ2UjcLA9SU6Rh1'),
    'database': os.environ.get('DB_NAME', 'moodle')
}

@app.route("/")
def home():
    return "✅ Microserviço Flask ativo!"

@app.route("/matriculas")
def get_matriculas():
    """
    Exemplo: /matriculas?inicio=2010&fim=2025
    Retorna JSON: [{ "ano": 2010, "total": 1234 }, ...]
    """
    ano_inicio = request.args.get('inicio', default=2010, type=int)
    ano_fim    = request.args.get('fim',    default=2025, type=int)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        SELECT YEAR(FROM_UNIXTIME(timecreated)) AS ano, COUNT(*) AS total
        FROM mdl_user_enrolments
        WHERE YEAR(FROM_UNIXTIME(timecreated)) BETWEEN %s AND %s
        GROUP BY ano
        ORDER BY ano ASC;
    """
    cursor.execute(query, (ano_inicio, ano_fim))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    # Monta a lista de dicionários
    dados = [{"ano": row[0], "total": row[1]} for row in results]
    return jsonify(dados)

if __name__ == "__main__":
    # Porta vem da variável PORT, definida pela maioria dos PaaS (Railway, Heroku, etc.)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
