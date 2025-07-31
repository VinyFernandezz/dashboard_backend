from flask import Blueprint, jsonify, request
import os
import mysql.connector
import re
import unicodedata
import pandas as pd

bp = Blueprint("courses", __name__, url_prefix="/courses")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

# Função corrigida para extrair disciplina e turma
def split_course_name(course_name):
    match = re.search(r'^(.*?)(?:\s*[-–]?\s*)?(Turma\s\d+)$', course_name, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    else:
        return course_name.strip(), ""

def extract_semester(course_name):
    # Expressão regular para capturar o semestre (ex: 2024/1, 2024.1, 2024-1)
    match = re.search(r'(20\d{2})[\/\.\-](\d)', course_name)
    if match:
        semester = f"{match.group(1)}.{match.group(2)}"  # normaliza para 2024.1
        cleaned_name = re.sub(re.escape(match.group(0)), '', course_name).strip()
        return cleaned_name, semester
    else:
        return course_name.strip(), ""

def extract_course_code(course_name):
    # Procura um número de 6 dígitos
    match = re.search(r'\b(\d{6})\b', course_name)
    if match:
        code = match.group(1)
        cleaned_name = re.sub(r'\b' + re.escape(code) + r'\b', '', course_name).strip()
        return cleaned_name, code
    else:
        return course_name.strip(), ""

def clean_course_title(text):
    # Remove colchetes, traços, underscores e espaços do início e fim
    cleaned = re.sub(r'^[\[\]\-\_\s]+|[\[\]\-\_\s]+$', '', text)
    return cleaned.strip()

def normalize_discipline(name):
    if pd.isnull(name):
        return ""

    name = str(name)

    # Remove termos relacionados a turma, coordenação, campus, espaço etc
    name = re.sub(r'\b[Tt]urma\s*\d+\b', '', name)
    name = re.sub(r'\bTURMA\s*\d+\b', '', name)
    name = re.sub(r'\b[Cc]oordenação\b.*', '', name)
    name = re.sub(r'[Cc]ampus\s+\w+', '', name)
    name = re.sub(r'\b[Ee]spaço.*$', '', name)
    name = re.sub(r'\b[Ll]aboratório.*$', '', name)
    name = re.sub(r'^\s*\*\s*', '', name)  # Ex: '* CIBERSEGURANCA' -> 'CIBERSEGURANCA'
    name = re.sub(r'^\s*[0-9]{4}[\./-]?[1-2]?\s*', '', name)  # Ex: '2025.1 BANCO DE DADOS' -> 'BANCO DE DADOS'
    name = re.sub(r'\bTURMA\s*\d+\s*[A-Z]+\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^\s*\d+\s+', '', name)
    name = re.sub(r'\b[Tt]este.*$', '', name)
    name = re.sub(r'\b[Mm]ódulo.*$', '', name)
    name = re.sub(r'\b_[A-Za-z0-9]+$', '', name)
    name = re.sub(r'\b[Oo]ficina.*$', '', name)
    name = re.sub(r'\s*[-–—_]+\s*', ' ', name)
    name = re.sub(r'\[\s*\]', '', name)
    name = re.sub(r'[…]|\.{3,}', '', name)
    name = re.sub(r'\s*TURMA\s*\d+\s*[A-Z]{1,3}\s*$', '', name, flags=re.IGNORECASE)

    # Remove campus do IFCE no final do nome
    name = re.sub(r'\b(FORTALEZA|BEBERIBE|CAUCAIA|ITAPIPOCA|OROS|UBAJARA|CRATEUS|MARANGUAPE|JAGUARIBE|ACARAU|ARACATI|QUIXADA|TAUA|SOBRAL|BATURITE|LIMOEIRO|MORADA NOVA|IGUATU|CANINDE|CAMOCIM|FORTALEZA|PECEM|HORIZONTE|ICAPUI|TIANGUA|RUSSAS|TABULEIRO DO NORTE|CRATO|JUAZEIRO DO NORTE|LAVRAS DA MANGABEIRA|MARACANAU)\b$', '', name, flags=re.IGNORECASE)

    # Remove símbolos isolados e letras finais soltas exceto algarismos romanos
    name = re.sub(r'\b\*+\b', '', name)
    name = re.sub(r'\b([A-Z])\b$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b([A-Z])\b(?=\s|$)', '', name, flags=re.IGNORECASE)

    # Remover letras no final que não sejam algarismos romanos I, II, III, IV, etc.
    name = re.sub(r'\b([A-Z])\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(?!I{1,3}|IV|V|VI{0,3}|IX|X)$[A-Z]{1,3}\b', '', name, flags=re.IGNORECASE)

    # Reduz espaços e remove pontuação indesejada
    name = re.sub(r'[-_]', ' ', name)
    name = re.sub(r'[\[\]–—]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # Remove acentos
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')

    # Padroniza para maiúsculas
    name = name.upper()

    # Padroniza nomes semelhantes para uma forma única
    name = re.sub(r'\bINTRODUCAO A EDUCACAO INCLUSIVA E TECNOLOGIA(S)? ASSISTIVA(S)?\b', 
                  'INTRODUCAO A EDUCACAO INCLUSIVA E TECNOLOGIAS ASSISTIVAS', name)

    return name
@bp.route("/", methods=["GET"], strict_slashes=False)
@bp.route("",  methods=["GET"])
def get_courses():
    # Conexão ao banco
    conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=5)
    cursor = conn.cursor()

    query = """
        SELECT id, fullname FROM mdl_course
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    # Cria o DataFrame
    df = pd.DataFrame(results, columns=["id", "nome"])

    # Conversão e transformação
    df['nome'] = df['nome'].astype(str)

    df[['nome', 'class']] = df['nome'].apply(
        lambda name: pd.Series(split_course_name(name))
    )

    df[['nome', 'semester']] = df['nome'].apply(
        lambda name: pd.Series(extract_semester(name))
    )

    df[['nome', 'academic_code']] = df['nome'].apply(
        lambda name: pd.Series(extract_course_code(name))
    )

    df['nome'] = df['nome'].apply(lambda name: clean_course_title(name))
    df['nome'] = df['nome'].apply(lambda name: normalize_discipline(name))

    # Remove colunas auxiliares
    df.drop(columns=['class', 'semester', 'academic_code'], inplace=True)

    # Remove nomes vazios e duplicados
    df = df[df['nome'].str.strip() != ""]
    df.drop_duplicates(subset="nome", inplace=True)

    # Ordena os cursos por nome
    df.sort_values(by="nome", inplace=True)

    # Converte para JSON no formato {"cursos": [...]}
    return jsonify({"cursos": df.to_dict(orient="records")})