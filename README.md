
# Moodle Microservice Recharts

Este microserviço Flask fornece dados de matrículas para um dashboard React com Recharts.

## Rodar localmente

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Acesse em http://127.0.0.1:5000/matriculas

## Deploy no Railway

1. Suba este projeto no GitHub.
2. Crie um novo projeto no Railway e conecte o repositório.
3. Railway detecta o `Procfile` e inicia com `gunicorn`.

## Endpoint

- **GET** `/matriculas?inicio=2010&fim=2025`

Retorna lista de matrículas.

---

Feito para integrar com frontend React + Recharts.
