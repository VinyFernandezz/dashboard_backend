
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route("/matriculas")
def get_matriculas():
    inicio = int(request.args.get("inicio", 2010))
    fim = int(request.args.get("fim", 2025))
    data = []
    for ano in range(inicio, fim + 1):
        total = 45000 + (ano - inicio) * 1000
        data.append({"ano": ano, "total": total})
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
