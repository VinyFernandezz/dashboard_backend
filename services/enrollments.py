from flask import Blueprint, jsonify, request
import mysql.connector
import os
import pandas as pd
import re
import unicodedata

bp = Blueprint("enrollments", __name__, url_prefix="/enrollments")

# --- Database Configuration (Remote Server) ---
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

# --- UPDATED Data Enrichment Dictionaries ---
POLE_COORDINATES = {
    "ACARAÚ": {"lat": -2.8890, "lng": -40.1132}, 
    "ACOPIARA": {"lat": -6.0954, "lng": -39.4503}, 
    "ARACATI": {"lat": -4.5684, "lng": -37.7681}, 
    "ARACOIABA": {"lat": -4.3725, "lng": -38.8111},
    "BARBALHA": {"lat": -7.3085, "lng": -39.3039},
    "BATURITÉ": {"lat": -4.3434, "lng": -38.8639},
    "BEBERIBE": {"lat": -4.1800, "lng": -38.1303},
    "BOA VIAGEM": {"lat": -5.1278, "lng": -39.7311},
    "BREJO SANTO": {"lat": -7.4935, "lng": -38.9956},
    "CAMOCIM": {"lat": -2.9033, "lng": -40.8436},
    "CAMPOS SALES": {"lat": -7.0735, "lng": -40.3758},
    "CANINDÉ": {"lat": -4.3575, "lng": -39.3125},
    "CASCAVEL": {"lat": -4.1331, "lng": -38.2408},
    "CAUCAIA": {"lat": -3.7361, "lng": -38.6531},
    "CEDRO": {"lat": -6.6039, "lng": -39.0625},
    "CRATEÚS": {"lat": -5.1783, "lng": -40.6775},
    "CRATO": {"lat": -7.2344, "lng": -39.4094},
    "DEPUTADO IRAPUAN PINHEIRO": {"lat": -6.0125, "lng": -39.6389},
    "EUSÉBIO": {"lat": -3.8894, "lng": -38.4503},
    "FORTALEZA": {"lat": -3.7319, "lng": -38.5267},
    "GUARAMIRANGA": {"lat": -4.2608, "lng": -38.9333},
    "HORIZONTE": {"lat": -4.0994, "lng": -38.4950},
    "IBICUITINGA": {"lat": -4.9667, "lng": -38.6333},
    "ICÓ": {"lat": -6.4014, "lng": -38.8617},
    "IGUATU": {"lat": -6.3594, "lng": -39.2983},
    "INDEPENDÊNCIA": {"lat": -5.3961, "lng": -40.3094},
    "IPUEIRAS": {"lat": -4.5444, "lng": -40.1553},
    "ITAPIPOCA": {"lat": -3.4942, "lng": -39.5786},
    "JAGUARIBE": {"lat": -5.8953, "lng": -38.6253},
    "JAGUARUANA": {"lat": -4.8339, "lng": -37.7817},
    "JUAZEIRO DO NORTE": {"lat": -7.2128, "lng": -39.3144},
    "LAVRAS DA MANGABEIRA": {"lat": -6.7486, "lng": -38.9664},
    "LIMOEIRO DO NORTE": {"lat": -5.1458, "lng": -38.0983},
    "MARACANAÚ": {"lat": -3.8661, "lng": -38.6264},
    "MARANGUAPE": {"lat": -3.8897, "lng": -38.6839},
    "MAURITI": {"lat": -7.3894, "lng": -38.7750},
    "MISSÃO VELHA": {"lat": -7.2469, "lng": -39.1436},
    "MORADA NOVA": {"lat": -5.1067, "lng": -38.3719},
    "NOVA RUSSAS": {"lat": -4.7083, "lng": -40.5608},
    "ORÓS": {"lat": -6.2411, "lng": -38.9133},
    "PARAMBU": {"lat": -6.2114, "lng": -40.6922},
    "PARACURU": {"lat": -3.4111, "lng": -39.0267},
    "PEDRA BRANCA": {"lat": -5.4522, "lng": -39.7153},
    "PENTECOSTE": {"lat": -3.7919, "lng": -39.2689},
    "PEREIRO": {"lat": -6.0403, "lng": -38.4608},
    "QUIXADÁ": {"lat": -4.9711, "lng": -39.0156},
    "QUIXERAMOBIM": {"lat": -5.1969, "lng": -39.2933},
    "REDENÇÃO": {"lat": -4.2258, "lng": -38.7308},
    "RUSSAS": {"lat": -4.9397, "lng": -37.9739},
    "SANTA QUITÉRIA": {"lat": -4.3319, "lng": -40.1564},
    "SÃO GONÇALO DO AMARANTE": {"lat": -3.6067, "lng": -38.9686},
    "SENADOR POMPEU": {"lat": -5.5881, "lng": -39.3703},
    "SOBRAL": {"lat": -3.6894, "lng": -40.3486},
    "TABULEIRO DO NORTE": {"lat": -5.2319, "lng": -38.1256},
    "TAMBORIL": {"lat": -4.8317, "lng": -40.3236},
    "TAUÁ": {"lat": -6.0039, "lng": -40.2931},
    "TIANGUÁ": {"lat": -3.7319, "lng": -40.9914},
    "UBAJARA": {"lat": -3.8497, "lng": -40.9208},
    "VÁRZEA ALEGRE": {"lat": -6.7869, "lng": -39.2953},
    "VIÇOSA DO CEARÁ": {"lat": -3.5606, "lng": -41.0919}
}

POLO_TO_REGION_MAP = {
    'DEFAULT': 'Não Mapeada', 'ACARAÚ': 'Litoral Norte', 'ACOPIARA': 'Sertão Central', 'ARACATI': 'Litoral Leste',
    'ARACOIABA': 'Maciço de Baturité', 'BARBALHA': 'Cariri', 'BATURITÉ': 'Maciço de Baturité', 'BEBERIBE': 'Litoral Leste',
    'BOA VIAGEM': 'Sertão Central', 'BREJO SANTO': 'Cariri', 'CAMOCIM': 'Litoral Norte', 'CAMPOS SALES': 'Sertão dos Inhamuns',
    'CANINDÉ': 'Sertão de Canindé', 'CASCAVEL': 'Litoral Leste', 'CAUCAIA': 'Metropolitana de Fortaleza', 'CEDRO': 'Centro-Sul',
    'CRATEÚS': 'Sertão dos Inhamuns', 'CRATO': 'Cariri', 'DEPUTADO IRAPUAN PINHEIRO': 'Sertão Central', 'EUSÉBIO': 'Metropolitana de Fortaleza',
    'FORTALEZA': 'Metropolitana de Fortaleza', 'GUARAMIRANGA': 'Maciço de Baturité', 'HORIZONTE': 'Metropolitana de Fortaleza',
    'IBICUITINGA': 'Vale do Jaguaribe', 'ICÓ': 'Centro-Sul', 'IGUATU': 'Centro-Sul', 'INDEPENDÊNCIA': 'Sertão dos Inhamuns',
    'IPUEIRAS': 'Serra da Ibiapaba', 'ITAPIPOCA': 'Litoral Oeste', 'JAGUARIBE': 'Vale do Jaguaribe', 'JAGUARUANA': 'Vale do Jaguaribe',
    'JUAZEIRO DO NORTE': 'Cariri', 'LAVRAS DA MANGABEIRA': 'Centro-Sul', 'LIMOEIRO DO NORTE': 'Vale do Jaguaribe', 'MARACANAÚ': 'Metropolitana de Fortaleza',
    'MARANGUAPE': 'Metropolitana de Fortaleza', 'MAURITI': 'Cariri', 'MISSÃO VELHA': 'Cariri', 'MORADA NOVA': 'Vale do Jaguaribe',
    'NOVA RUSSAS': 'Sertão de Crateús', 'ORÓS': 'Centro-Sul', 'PARAMBU': 'Sertão dos Inhamuns', 'PARACURU': 'Litoral Oeste',
    'PEDRA BRANCA': 'Sertão Central', 'PENTECOSTE': 'Médio Curu', 'PEREIRO': 'Serra do Pereiro', 'QUIXADÁ': 'Sertão Central',
    'QUIXERAMOBIM': 'Sertão Central', 'REDENÇÃO': 'Maciço de Baturité', 'RUSSAS': 'Vale do Jaguaribe', 'SANTA QUITÉRIA': 'Sertão de Crateús',
    'SÃO GONÇALO DO AMARANTE': 'Metropolitana de Fortaleza', 'SENADOR POMPEU': 'Sertão Central', 'SOBRAL': 'Norte',
    'TABULEIRO DO NORTE': 'Vale do Jaguaribe', 'TAMBORIL': 'Sertão de Crateús', 'TAUÁ': 'Sertão dos Inhamuns',
    'TIANGUÁ': 'Serra da Ibiapaba', 'UBAJARA': 'Serra da Ibiapaba', 'VÁRZEA ALEGRE': 'Cariri', 'VIÇOSA DO CEARÁ': 'Serra da Ibiapaba'
}

### --- ENROLLMENTS BY YEAR  ---
@bp.route("/by_year", methods=["GET"])
def get_enrollments_by_year():
    """Fetches the total number of enrollments grouped by year."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = """
            SELECT YEAR(`Registration Date`) as year, COUNT(*) as total 
            FROM suap_students
            WHERE `Registration Date` IS NOT NULL
            GROUP BY year ORDER BY year;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # --- ATUALIZADO: Padronizando a chave de saída para 'name' e 'value' ---
        data = [{"name": str(row[0]), "value": row[1]} for row in results]
        
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

### --- ENROLLMENTS BY LOCATION  ---
@bp.route("/by_location", methods=["GET"])
def get_enrollments_by_location():
    start_year = int(request.args.get("inicio", 2010))
    end_year = int(request.args.get("fim", 2025))
    courses_str = request.args.get("cursos", "")
    typelocal = request.args.get("typelocal", "municipio").lower()
    modality = request.args.get("modality", "ALL").upper() 
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        where_clauses = ["`Year of Entry` BETWEEN %s AND %s"]
        params = [start_year, end_year]
        if modality == "EAD":
            where_clauses.append("UPPER(`Nature of Participation`) = %s")
            params.append(modality)
        if courses_str:
            courses = courses_str.split(",")
            placeholders = ', '.join(['%s'] * len(courses))
            where_clauses.append(f"`Nome do Curso` IN ({placeholders})")
            params.extend(courses)
        final_where_clause = " AND ".join(where_clauses)
        
        if typelocal == "municipio":
            query = f"SELECT `City of Residence` AS name, COUNT(*) AS value FROM suap_students WHERE `State` = 'CE' AND {final_where_clause} GROUP BY `City of Residence` ORDER BY name ASC;"
        else:
            query = f"SELECT `State` AS name, COUNT(*) AS value FROM suap_students WHERE {final_where_clause} GROUP BY `State` ORDER BY name ASC;"
            
        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if typelocal == "municipio":
            df["name"] = df["name"].str.replace("-CE", "", regex=False).str.strip().str.upper()
            
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

### --- DISTINCT ENROLLMENT YEARS  ---
@bp.route("/distinct_years", methods=["GET"])
def get_distinct_years():
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = "SELECT DISTINCT `Year of Entry` AS ano FROM suap_students WHERE `Year of Entry` IS NOT NULL ORDER BY ano;"
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close(); conn.close()
        data = [{"ano": row[0]} for row in results]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

### --- ENROLLMENTS BY POLO  ---
def get_processed_polo_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=15)
        query = "SELECT `Registration`, `Pole`, `Registration Date` FROM suap_students;"
        df = pd.read_sql(query, conn)
        conn.close()
        df['year'] = pd.to_datetime(df['Registration Date'], errors='coerce').dt.year
        df.dropna(subset=['year'], inplace=True)
        df['year'] = df['year'].astype(int)
        df.rename(columns={'Pole': 'polo'}, inplace=True)
        df.dropna(subset=['polo'], inplace=True)
        df['polo'] = df['polo'].str.strip()
        df['polo'] = df['polo'].apply(lambda x: ''.join(c for c in unicodedata.normalize('NFD', x) if unicodedata.category(c) != 'Mn')).str.upper()
        return df, None
    except Exception as e:
        return None, str(e)

@bp.route('/by_polo_yearly', methods=['GET'])
def get_polo_yearly_matriculas():
    df_ead, error = get_processed_polo_data()
    if error:
        return jsonify({"error": "Database query failed", "details": error}), 500
    yearly_totals = df_ead.groupby('year')['Registration'].nunique().to_dict()
    polo_yearly_counts = df_ead.groupby(['year', 'polo'])['Registration'].nunique().reset_index()
    polo_yearly_counts.rename(columns={'Registration': 'absoluteFrequency'}, inplace=True)
    all_polo_data = []
    for index, row in polo_yearly_counts.iterrows():
        year = int(row['year'])
        polo_name = row['polo']
        absolute_freq = int(row['absoluteFrequency'])
        coordinates = POLE_COORDINATES.get(polo_name)
        total_for_year = yearly_totals.get(year, 1)
        relative_freq = (absolute_freq / total_for_year) * 100
        polo_entry = {
            'id': f"{polo_name}-{year}", 'polo': polo_name, 'year': year,
            'absoluteFrequency': absolute_freq, 'relativeFrequency': relative_freq,
            'region': POLO_TO_REGION_MAP.get(polo_name, 'Não Mapeada')
        }
        if coordinates:
            polo_entry['lat'] = coordinates['lat']
            polo_entry['lng'] = coordinates['lng']
        all_polo_data.append(polo_entry)
    return jsonify(all_polo_data)

### --- TOTAL ENROLLMENTS BY YEAR ---
@bp.route('/total_yearly', methods=['GET'])
def get_total_yearly_enrollments():
    """
    Provides the total number of unique student registrations per year.
    """
    df_ead, error = get_processed_polo_data()

    if error:
        return jsonify({"error": "Database query failed", "details": error}), 500
    
    yearly_totals = df_ead.groupby('year')['Registration'].nunique().reset_index()
    
    # --- ATUALIZAÇÃO CRÍTICA: Renomeia as colunas para o padrão 'name' e 'value' ---
    yearly_totals.rename(columns={'Registration': 'value', 'year': 'name'}, inplace=True)
    
    # Garante que o 'name' (ano) seja uma string para consistência
    yearly_totals['name'] = yearly_totals['name'].astype(str)
    
    return jsonify(yearly_totals.to_dict(orient='records'))

@bp.route('/by_polo_total', methods=['GET'])
def get_total_enrollments_by_polo():
    """
    Provides the total number of unique student registrations per pole across all years.
    """
    df_ead, error = get_processed_polo_data()
    if error:
        return jsonify({"error": "Database query failed", "details": error}), 500

    polo_totals = df_ead.groupby('polo')['Registration'].nunique().reset_index()
    polo_totals.rename(columns={'Registration': 'value', 'polo': 'name'}, inplace=True)
    
    # Ordena do maior para o menor
    polo_totals.sort_values(by='value', ascending=False, inplace=True)

    return jsonify(polo_totals.to_dict(orient='records'))