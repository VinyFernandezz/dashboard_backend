
import pandas as pd
import mysql.connector
import json
from flask import Blueprint, jsonify
from flask_cors import CORS
import unicodedata
import os
from dotenv import load_dotenv

load_dotenv() # This loads variables from a .env file

# CREATE a Blueprint instead of a full Flask App
polo_chart_bp = Blueprint('polo_chart_bp', __name__)

# --- My Database Configuration ---
config = {
    'user': os.getenv('DB_USER'),
    'password':  os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME')


# ADD this dictionary with pole coordinates
POLE_COORDINATES = {
    "ACARAU": {"lat": -2.8884, "lng": -40.1204},
    "ACOPIARA": {"lat": -6.0954, "lng": -39.4503},
    "ARACATI": {"lat": -4.5619, "lng": -37.7700},
    "ARACOIABA": {"lat": -4.3725, "lng": -38.8111},
    "BARBALHA": {"lat": -7.3085, "lng": -39.3039},
    "BATURITE": {"lat": -4.3283, "lng": -38.8828},
    "BEBERIBE": {"lat": -4.1800, "lng": -38.1303},
    "BOA VIAGEM": {"lat": -5.1278, "lng": -39.7317},
    "CAMOCIM": {"lat": -2.9033, "lng": -40.8433},
    "CAMPOS SALES": {"lat": -7.0734, "lng": -40.3739},
    "CANINDE": {"lat": -4.3571, "lng": -39.3115},
    "CAUCAIA": {"lat": -3.7361, "lng": -38.6531},
    "CAUCAIA JUREMA": {"lat": -3.7744, "lng": -38.6572},
    "CEDRO": {"lat": -6.6053, "lng": -39.0628},
    "CRATEUS": {"lat": -5.1783, "lng": -40.6775},
    "CRATO": {"lat": -7.2343, "lng": -39.4093},
    "FORTALEZA": {"lat": -3.7327, "lng": -38.5267},
    "GUARAMIRANGA": {"lat": -4.2600, "lng": -38.9322},
    "HORIZONTE": {"lat": -4.0994, "lng": -38.4947},
    "IGUATU": {"lat": -6.3592, "lng": -39.2981},
    "ITAPIPOCA": {"lat": -3.4939, "lng": -39.5786},
    "JAGUARIBE": {"lat": -5.8950, "lng": -38.6253},
    "JAGUARUANA": {"lat": -4.8336, "lng": -37.7808},
    "JUAZEIRO DO NORTE": {"lat": -7.2128, "lng": -39.3133},
    "LIMOEIRO DO NORTE": {"lat": -5.1458, "lng": -38.0975},
    "MARACANAU": {"lat": -3.8767, "lng": -38.6264},
    "MARANGUAPE": {"lat": -3.8889, "lng": -38.6833},
    "MERUOCA": {"lat": -3.5411, "lng": -40.4533},
    "MOMBACA": {"lat": -5.7450, "lng": -39.6289},
    "MORADA NOVA": {"lat": -5.1067, "lng": -38.3711},
    "OROS": {"lat": -6.2419, "lng": -38.9133},
    "PACAJUS": {"lat": -4.1731, "lng": -38.4611},
    "PARACURU": {"lat": -3.4072, "lng": -39.0239},
    "PECEM": {"lat": -3.5381, "lng": -38.8350},
    "QUIXADA": {"lat": -4.9708, "lng": -39.0153},
    "QUIXERAMOBIM": {"lat": -5.1956, "lng": -39.2931},
    "RUSSAS": {"lat": -4.9400, "lng": -37.9753},
    "SAO GONCALO DO AMARANTE": {"lat": -3.6067, "lng": -38.9669},
    "SOBRAL": {"lat": -3.6894, "lng": -40.3486},
    "TABULEIRO DO NORTE": {"lat": -5.0267, "lng": -38.1250},
    "TAUA": {"lat": -5.9731, "lng": -40.2944},
    "TIANGUA": {"lat": -3.7319, "lng": -40.9911},
    "UBAJARA": {"lat": -3.8525, "lng": -40.9208},
    "UMIRIM": {"lat": -3.6797, "lng": -39.3517}
}


# ADD a Polo to Region Map (customize as needed)
POLO_TO_REGION_MAP = {
    'FORTALEZA': 'Metropolitana',
    'JAGUARUANA': 'Jaguaribe',
    'CAUCAIA': 'Metropolitana',
    'GUARAMIRANGA': 'Maciço de Baturité',
    'HORIZONTE': 'Metropolitana',
    'PARACURU': 'Litoral Oeste',
    'ITAPIPOCA': 'Litoral Oeste',
    'BATURITÉ': 'Maciço de Baturité',
    'TABULEIRO DO NORTE': 'Baixo Jaguaribe',
    'LIMOEIRO DO NORTE': 'Baixo Jaguaribe',
    'PECÉM': 'Metropolitana',
    'BOA VIAGEM': 'Centro Sul',
    'TIANGUÁ': 'Ibiapaba',
    'UBAJARA': 'Ibiapaba',
    'MARANGUAPE': 'Metropolitana',
    'QUIXADÁ': 'Sertão Central',
    'SOBRAL': 'Norte',
    'CRATO' : 'Cariri',
    'MARACANAÚ' : 'Metropolitana',
    'CEDRO' : 'Centro Sul',
    'CAMOCIM' : 'Litoral Norte',
    'ACARAÚ' : 'Litoral Norte',
    'CRATEÚS' : 'Sertão dos Inhamuns',
    'QUIXERAMOBIM' : 'Sertão Central',
    'IGUATÚ' : 'Centro Sul',
    'ARACATI' : 'Jaguaribe',
    'CAUCAIA JUREMA' : 'Metropolitana',
    'TAUÁ' : 'Sertão dos Inhamuns',
    'JUAZEIRO DO NORTE' : 'Cariri',
    'ORÓS' : 'Sertão Central',
    'SÃO GONÇALO' : 'Centro Sul',
    'JAGUARIBE' : 'Jaguaribe',
    'ACOPIARA' : 'Centro Sul',
    'UMIRIM' : 'Litoral Norte',
    'MERUOCA' : 'Norte',
    'MORADA NOVA' : 'Baixo Jaguaribe',
    'CANINDÉ' : 'Sertão Central',
    'RUSSAS' : 'Baixo Jaguaribe',
    'MOMBAÇA' : 'Sertão dos Inhamuns',


    # ... Add all other polos and their regions here
    'DEFAULT': 'Não Classificada' # A fallback for any unmapped polo
}




# ---  NEW: Canonical Name Mapping ---
# This dictionary defines the "correct" name for each polo.
# The key is a standardized version (uppercase, no accents, no spaces).
# The value is the final name you want to display.
CANONICAL_POLE_NAMES = {
    'ACARAU': 'ACARAÚ',
    'BOAVIAGEM': 'BOA VIAGEM',
    'MORADANOVA': 'MORADA NOVA',
    'MOMBACA': 'MOMBAÇA',
    'BATURITE': 'BATURITÉ',
    'CANINDE': 'CANINDÉ',
    'CRATEUS': 'CRATEÚS',
    'FLORIANOPOLIS': 'FLORIANÓPOLIS',
    'IGUATU': 'IGUATÚ',
    'JUAZEIRODONORTE': 'JUAZEIRO DO NORTE',
    'JUAZEIRO': 'JUAZEIRO DO NORTE',
    'LIMOEIRODONORTE': 'LIMOEIRO DO NORTE',
    'LIMOEIRO': 'LIMOEIRO DO NORTE',
    'MARACANAU': 'MARACANAÚ',
    'OROS': 'ORÓS',
    'PACAJUS': 'PACAJÚS',
    'PECEM': 'PECÉM',
    'PORTUARIO': 'PORTUÁRIO',
    'QUIXADA': 'QUIXADÁ',
    'SAOGONCALO': 'SÃO GONÇALO',
    'TABULEIRODONORTE': 'TABULEIRO DO NORTE',
    'TABLUEIRODONORTE': 'TABULEIRO DO NORTE',
    'TABULEIRO' : 'TABULEIRO DO NORTE',
    'TAUA': 'TAUÁ',
    'TIANGUA': 'TIANGUÁ',
    # Add any other specific corrections here
}

def get_canonical_name(pole_name):
    """
    Finds the correct, canonical name for a polo using the mapping.
    """
    if not isinstance(pole_name, str):
        return pole_name

    # 1. Create a consistent key from the input string
    # Remove accents, convert to uppercase, and remove spaces
    key = ''.join(c for c in unicodedata.normalize('NFD', pole_name)
                  if unicodedata.category(c) != 'Mn')
    key = key.upper().replace(' ', '')

    # 2. Look up the key in our map.
    # If a canonical name exists, return it. Otherwise, return the original
    # name in uppercase as a fallback.
    return CANONICAL_POLE_NAMES.get(key, pole_name.upper())

    



def get_processed_data():
    """
    Connects to the database and runs your data processing logic.
    This function is now reusable for different endpoints.
    """
    try:
        conn = mysql.connector.connect(**config)
        
        query = "SELECT `Registration`, CPF, Pole, Campus, `Nature of Participation`, `Registration Date`, `Registration Status` FROM suap_students;"
        
        # Use pandas to read directly from the SQL query
        df = pd.read_sql(query, conn)

        # --- KEY STEP 1: DROP DUPLICATES ---
        df.drop_duplicates(subset=['Registration', 'CPF'], keep='first', inplace=True)
        
        # --- Reclassify ' - ' as 'EaD' for named poles ---
        df.loc[
            (df['Pole'] != '-') & (df['Nature of Participation'] == '-'),
            'Nature of Participation'
        ] = 'EaD'

        # --- KEY STEP 2: CONSOLIDATE POLES ---
        df['Consolidated_Pole'] = df['Pole']
        df.loc[df['Pole'] == '-', 'Consolidated_Pole'] = df['Campus']
        
        # --- Fix character encoding ---
        df['Consolidated_Pole'] = df['Consolidated_Pole'].astype(str).apply(
            lambda x: x.encode('latin1').decode('utf-8') if '\\' in x else x
        )
        
        # ---  APPLY NEW CANONICAL NAME FUNCTION  ---
        df['Consolidated_Pole'] = df['Consolidated_Pole'].apply(get_canonical_name)


        df['Registration Date'] = pd.to_datetime(df['Registration Date'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        df['Ano_de_Entrada'] = df['Registration Date'].dt.year

        # --- Filter for years from 2010 onwards and EaD students ---
        df_ead = df[(df['Ano_de_Entrada'] >= 2010) & (df['Nature of Participation'].str.lower() == 'ead')]
        
        return df_ead

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return None
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()


#  CREATE a new, unified endpoint
@polo_chart_bp.route('/api/poles/comprehensive', methods=['GET'])
def get_comprehensive_data():
    df_ead = get_processed_data()
    if df_ead is None:
        return jsonify({"error": "Could not retrieve data from database"}), 500
    
    # Calculate total students for EACH year
    yearly_totals = df_ead.groupby('Ano_de_Entrada').size().to_dict()


    all_polo_data = []
    
    
    # Group by both polo and year to get historical counts
    yearly_counts = df_ead.groupby(['Consolidated_Pole', 'Ano_de_Entrada']).size().reset_index(name='frequenciaAbsoluta')

    for index, row in yearly_counts.iterrows():
        polo_name = row['Consolidated_Pole']
        year = int(row['Ano_de_Entrada'])
        absolute_freq = int(row['frequenciaAbsoluta'])

        # ✨ ADD a lookup for coordinates
        coordinates = POLE_COORDINATES.get(polo_name, None) # Use .get for a safe lookup
        
         # Calculate relative frequency based on that specific year's total.
        total_for_year = yearly_totals.get(year, 0)
        relative_freq = (absolute_freq / total_for_year) * 100 if total_for_year > 0 else 0

        polo_entry = {
            'id': f"{polo_name}-{year}",
            'polo': polo_name,
            'year': year,
            'frequenciaAbsoluta': absolute_freq,
            'frequenciaRelativa': relative_freq,
            'region': POLO_TO_REGION_MAP.get(polo_name, POLO_TO_REGION_MAP['DEFAULT'])
        }

        # ✨ ADD coordinates to the response if they were found
        if coordinates:
            polo_entry['lat'] = coordinates['lat']
            polo_entry['lng'] = coordinates['lng']

        all_polo_data.append(polo_entry)
        
    return jsonify(all_polo_data)

# --- API Endpoint 2: Total Yearly Enrollments ---
@polo_chart_bp.route('/api/matriculas/total_yearly', methods=['GET'])
def get_total_yearly_matriculas():
    df_ead = get_processed_data()
    if df_ead is None:
        return jsonify({"error": "Could not retrieve data from database"}), 500
    
    yearly_totals = df_ead.groupby('Ano_de_Entrada').size().reset_index(name='total')
    result = yearly_totals.rename(columns={'Ano_de_Entrada': 'ano'}).to_dict('records')
    
    return jsonify(result)
