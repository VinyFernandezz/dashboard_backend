from flask import Blueprint, jsonify, request
import mysql.connector
import os
import pandas as pd
import re
import unicodedata

bp = Blueprint("analysis", __name__, url_prefix="/analysis")

# --- Database Configuration (Remote Server) ---
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
}

# --- Demographic Analysis Endpoints ---

# --- Helper function to build the dynamic WHERE clause for modality ---
def get_modality_filter():
    """
    Reads the 'modality' URL parameter and returns the corresponding
    SQL condition and parameters.
    """
    modality = request.args.get("modality", "ALL").upper()
    if modality == 'EAD':
        # Returns the SQL part and the value to be safely inserted
        return "UPPER(`Nature of Participation`) = %s", [modality]
    return "", [] # Returns empty strings if modality is 'ALL' or anything else

@bp.route("/gender", methods=["GET"])
def get_gender_distribution():
    """Fetches the distribution of students by gender."""
    modality_sql, params = get_modality_filter()
    where_clause = "WHERE `Gender` IS NOT NULL AND `Gender` != ''"
    if modality_sql:
        where_clause += f" AND {modality_sql}"

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = f"""
            SELECT `Gender`, COUNT(*) as total FROM suap_students
            {where_clause}
            GROUP BY `Gender` ORDER BY total DESC;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        gender_map = {"M": "Masculino", "F": "Feminino", "I": "Indefinido"}
        data = [{"name": gender_map.get(row[0], row[0]), "value": row[1]} for row in results]
        
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/race", methods=["GET"])
def get_ethnicity_distribution():
    """
    Fetches the distribution of students by ethnicity (Race).
    """
    modality_sql, params = get_modality_filter()
    
    column_name = "`Ethnicity/Race`"
    
    where_clauses = [f"{column_name} IS NOT NULL", f"{column_name} != ''"]
    if modality_sql:
        where_clauses.append(modality_sql)

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = f"""
            SELECT {column_name}, COUNT(*) as total FROM suap_students
            WHERE {" AND ".join(where_clauses)}
            GROUP BY {column_name} ORDER BY total DESC;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        data = [{"name": row[0], "value": row[1]} for row in results]
        
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@bp.route("/marital_status", methods=["GET"])
def get_marital_status_distribution():
    """Fetches the distribution of students by marital status."""
    modality_sql, params = get_modality_filter()
    where_clause = "WHERE `Marital Status` IS NOT NULL AND `Marital Status` != ''"
    if modality_sql:
        where_clause += f" AND {modality_sql}"

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = f"""
            SELECT `Marital Status`, COUNT(*) as total FROM suap_students
            {where_clause}
            GROUP BY `Marital Status` ORDER BY total DESC;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        data = [{"name": row[0], "value": row[1]} for row in results]
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/income", methods=["GET"])
def get_income_distribution():
    """
    Fetches the distribution by per capita income bracket,
    sorted in a logical order.
    """
    modality_sql, params = get_modality_filter()
    column_name = "`Per Capita Income`"
    where_clause = f"WHERE {column_name} IS NOT NULL AND {column_name} != ''"
    if modality_sql:
        where_clause += f" AND {modality_sql}"
        
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        
        # Uses a CASE statement to define a custom sort order.
        query = f"""
            SELECT {column_name}, COUNT(*) as total 
            FROM suap_students
            {where_clause}
            GROUP BY {column_name}
            ORDER BY
                CASE {column_name}
                    WHEN 'Nenhuma Renda' THEN 1
                    WHEN 'Até 1 Salário Mínimo' THEN 2
                    WHEN 'De 1 a 2 Salários Mínimos' THEN 3
                    WHEN 'De 2 a 3 Salários Mínimos' THEN 4
                    WHEN 'De 3 a 5 Salários Mínimos' THEN 5
                    WHEN 'Acima de 5 Salários Mínimos' THEN 6
                    ELSE 7
                END;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        data = [{"name": row[0], "value": row[1]} for row in results]
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/age_distribution", methods=["GET"])
def get_age_distribution():
    """Calculates the distribution of students by age brackets."""
    modality_sql, params = get_modality_filter()
    
    registration_date_col = "Registration Date"
    birth_date_col = "Date of Birth"
    
    # Base WHERE clause for the inner query
    date_where = f"`{birth_date_col}` IS NOT NULL AND `{registration_date_col}` IS NOT NULL"
    if modality_sql:
        # Append the modality filter to the inner query
        date_where += f" AND {modality_sql}"

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()

        query = f"""
            SELECT 
                CASE
                    WHEN age < 18 THEN 'Menor de 18'
                    WHEN age BETWEEN 18 AND 24 THEN '18-24 anos'
                    WHEN age BETWEEN 25 AND 34 THEN '25-34 anos'
                    WHEN age BETWEEN 35 AND 44 THEN '35-44 anos'
                    WHEN age BETWEEN 45 AND 54 THEN '45-54 anos'
                    ELSE '55+ anos'
                END AS age_bracket,
                COUNT(*) as total
            FROM (
                SELECT 
                    TIMESTAMPDIFF(YEAR, 
                        STR_TO_DATE(`{birth_date_col}`, '%d/%m/%Y'), 
                        STR_TO_DATE(`{registration_date_col}`, '%d/%m/%Y')
                    ) AS age
                FROM suap_students
                WHERE {date_where}
            ) AS age_calculated
            WHERE age IS NOT NULL AND age >= 0
            GROUP BY age_bracket
            ORDER BY age_bracket;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        data = [{"name": row[0], "value": row[1]} for row in results]
        cursor.close()
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@bp.route("/origin_state", methods=["GET"])
def get_origin_state_distribution():
    """Calculates the distribution of students from Ceará vs. other states."""
    modality_sql, params = get_modality_filter()
    where_clause = "WHERE `State` IS NOT NULL AND `State` != ''"
    if modality_sql:
        where_clause += f" AND {modality_sql}"

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()
        query = f"""
            SELECT
                CASE WHEN `State` = 'CE' THEN 'Ceará' ELSE 'Fora do Ceará' END AS origin,
                COUNT(*) AS total
            FROM suap_students
            {where_clause}
            GROUP BY origin;
        """
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        data = [{"name": row[0], "value": row[1]} for row in results]
        cursor.close(); conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/status_by_profile", methods=["GET"])
def get_status_by_profile():
    """
    Calculates student statuses crossed with a demographic profile,
    correctly applying modality filters and custom sorting.
    """
    profile_param = request.args.get('profile')
    modality = request.args.get("modality", "ALL").upper()

    allowed_profiles = {
        "gender": "`Gender`",
        "marital_status": "`Marital Status`",
        "race": "`Ethnicity/Race`",
        "income": "`Per Capita Income`",
        "age": "age_bracket",
        "origin_state": "origin"
    }

    if not profile_param or profile_param.lower() not in allowed_profiles:
        return jsonify({"error": "A valid profile parameter is required."}), 400

    db_column_name = allowed_profiles[profile_param.lower()]
    status_column_name = "`Registration Status`"
    
    # --- Build base WHERE clause and params for modality ---
    where_clauses = []
    params = []
    if modality == 'EAD':
        where_clauses.append("UPPER(`Nature of Participation`) = %s")
        params.append(modality)

    query = ""

    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=15)
        cursor = conn.cursor()

        if profile_param.lower() == 'age':
            registration_date_col = "`Registration Date`"
            birth_date_col = "`Date of Birth`"
            
            age_where_clauses = [f"{birth_date_col} IS NOT NULL", f"{registration_date_col} IS NOT NULL"]
            if where_clauses:
                age_where_clauses.extend(where_clauses)

            query = f"""
                SELECT age_bracket AS profile_category, {status_column_name} AS status, COUNT(*) as total
                FROM (
                    SELECT 
                        {status_column_name}, `Nature of Participation`,
                        CASE
                            WHEN TIMESTAMPDIFF(YEAR, STR_TO_DATE({birth_date_col}, '%d/%m/%Y'), STR_TO_DATE({registration_date_col}, '%d/%m/%Y')) < 18 THEN 'Menor de 18'
                            WHEN TIMESTAMPDIFF(YEAR, STR_TO_DATE({birth_date_col}, '%d/%m/%Y'), STR_TO_DATE({registration_date_col}, '%d/%m/%Y')) BETWEEN 18 AND 24 THEN '18-24 anos'
                            WHEN TIMESTAMPDIFF(YEAR, STR_TO_DATE({birth_date_col}, '%d/%m/%Y'), STR_TO_DATE({registration_date_col}, '%d/%m/%Y')) BETWEEN 25 AND 34 THEN '25-34 anos'
                            WHEN TIMESTAMPDIFF(YEAR, STR_TO_DATE({birth_date_col}, '%d/%m/%Y'), STR_TO_DATE({registration_date_col}, '%d/%m/%Y')) BETWEEN 35 AND 44 THEN '35-44 anos'
                            WHEN TIMESTAMPDIFF(YEAR, STR_TO_DATE({birth_date_col}, '%d/%m/%Y'), STR_TO_DATE({registration_date_col}, '%d/%m/%Y')) BETWEEN 45 AND 54 THEN '45-54 anos'
                            ELSE '55+ anos'
                        END AS age_bracket
                    FROM suap_students
                    WHERE {" AND ".join(age_where_clauses)}
                ) AS calculated_table
                WHERE age_bracket IS NOT NULL AND {status_column_name} IS NOT NULL AND {status_column_name} != ''
                GROUP BY profile_category, status ORDER BY profile_category, status;
            """
        elif profile_param.lower() == 'origin_state':
            state_column = "`State`"
            origin_where_clauses = [f"{state_column} IS NOT NULL", f"{state_column} != ''"]
            if where_clauses:
                origin_where_clauses.extend(where_clauses)

            query = f"""
                SELECT origin AS profile_category, {status_column_name} AS status, COUNT(*) as total
                FROM (
                    SELECT
                        {status_column_name}, `Nature of Participation`,
                        CASE WHEN {state_column} = 'CE' THEN 'Ceará' ELSE 'Fora do Ceará' END AS origin
                    FROM suap_students
                    WHERE {" AND ".join(origin_where_clauses)}
                ) AS calculated_table
                WHERE {status_column_name} IS NOT NULL AND {status_column_name} != ''
                GROUP BY profile_category, status ORDER BY profile_category, status;
            """
        else:
            simple_where_clauses = [f"{db_column_name} IS NOT NULL", f"{db_column_name} != ''", f"{status_column_name} IS NOT NULL", f"{status_column_name} != ''"]
            if where_clauses:
                simple_where_clauses.extend(where_clauses)
            
            # Initialize order_by_clause with a default value
            order_by_clause = "ORDER BY profile_category, status"
            if profile_param.lower() == 'income':
                order_by_clause = f"""
                ORDER BY
                    CASE {db_column_name}
                        WHEN 'Nenhuma Renda' THEN 1
                        WHEN 'Até 1 Salário Mínimo' THEN 2
                        WHEN 'De 1 a 2 Salários Mínimos' THEN 3
                        WHEN 'De 2 a 3 Salários Mínimos' THEN 4
                        WHEN 'De 3 a 5 Salários Mínimos' THEN 5
                        WHEN 'Acima de 5 Salários Mínimos' THEN 6
                        ELSE 7
                    END,
                    status
                """
            
            query = f"""
                SELECT {db_column_name} AS profile_category, {status_column_name} AS status, COUNT(*) AS total
                FROM suap_students
                WHERE {" AND ".join(simple_where_clauses)}
                GROUP BY profile_category, status
                {order_by_clause};
            """
        
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        cursor.close(); conn.close()

        processed_data = {}
        for row in results:
            profile_category, status, total = row
            if profile_param.lower() == 'gender':
                gender_map = {"M": "Masculino", "F": "Feminino", "I": "Indefinido"}
                profile_category = gender_map.get(profile_category, profile_category)
            if profile_category not in processed_data:
                processed_data[profile_category] = {}
            processed_data[profile_category][status] = total

        return jsonify(processed_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

''' Function to show all the columns
@bp.route("/discover_columns", methods=["GET"])
def discover_student_columns():
    """
    A temporary utility to fetch all column names from the suap_students table.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
        cursor = conn.cursor()

        # This query inspects the database's metadata
        query = "SHOW COLUMNS FROM suap_students;"

        cursor.execute(query)
        results = cursor.fetchall()

        # Extract just the first item (the column name) from each row
        column_names = [row[0] for row in results]

        cursor.close(); conn.close()
        return jsonify(column_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    '''