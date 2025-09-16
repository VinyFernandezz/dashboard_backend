from flask import Blueprint, jsonify, request, Response
import os
import mysql.connector
import datetime
import json

bp = Blueprint("analytics_behavour", __name__, url_prefix="/analytics_behavour")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "200.129.17.231"),
    "user": os.environ.get("DB_USER", "danilo"),
    "password": os.environ.get("DB_PASS", "5yZ2UjcLA9SU6Rh1"),
    "database": os.environ.get("DB_NAME", "moodle"),
    "connection_timeout": 5,
    "autocommit": True,
}

# ---------------------------- helpers ----------------------------

def _parse_date(arg_name, default=None):
    val = request.args.get(arg_name, default)
    if val is None:
        return None
    try:
        return str(datetime.date.fromisoformat(str(val)))
    except Exception:
        # tenta formatos comuns
        try:
            return str(datetime.datetime.strptime(str(val), "%Y-%m-%d").date())
        except Exception:
            raise ValueError(f"Parâmetro inválido: {arg_name}={val!r}. Use YYYY-MM-DD.")

def _parse_int(arg_name):
    v = request.args.get(arg_name)
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        raise ValueError(f"Parâmetro inválido: {arg_name}={v!r}. Use inteiro.")

def _json_download(data, filename: str):
    download = request.args.get("download", "0") == "1"
    if not download:
        return jsonify(data)
    payload = json.dumps(data, ensure_ascii=False)
    resp = Response(payload, mimetype="application/json; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

def _window_defaults():
    # últimos 180 dias por padrão
    end = datetime.date.today()
    start = end - datetime.timedelta(days=180)
    return str(start), str(end)

# ---------------------------- endpoints ----------------------------

@bp.route("/health", methods=["GET"], strict_slashes=False)
def health():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        v = cur.fetchone()[0]
        cur.close(); conn.close()
        return jsonify({"ok": True, "db": "up", "result": int(v)})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)}), 500


@bp.route("/top-hours-days", methods=["GET"], strict_slashes=False)
def top_hours_days():
    """
    Retorna hits por HORA x DIA (0=Dom..6=Sáb), completando horas/dias sem acesso com 0.
    """
    try:
        d0_default, d1_default = _window_defaults()
        d0 = _parse_date("start", d0_default)
        d1 = _parse_date("end",   d1_default)
        cid = _parse_int("course_id")
        uid = _parse_int("user_id")

        # Query usando placeholders %s (mysql.connector)
        # Repetimos os parâmetros para as condições com OR.
        sql = """
        SELECT h.hour, d.dow, d.label AS dow_label, COALESCE(SUM(c.hits),0) AS hits
        FROM dim_hour h
        CROSS JOIN dim_dow d
        LEFT JOIN moodle_agg_df_cube c
          ON c.hour = h.hour
         AND c.dow  = d.dow
         AND c.ymd >= %s AND c.ymd < %s
         AND (%s IS NULL OR c.courseid = %s)
         AND (%s IS NULL OR c.userid   = %s)
        GROUP BY h.hour, d.dow, d.label
        ORDER BY h.hour ASC, d.dow ASC
        """
        params = [d0, d1, cid, cid, uid, uid]

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        out = {
            "params": {"start": d0, "end": d1, "course_id": cid, "user_id": uid},
            "data": rows,
        }
        return _json_download(out, "top_hours_days.json")

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@bp.route("/resources-usage", methods=["GET"], strict_slashes=False)
def resources_usage():
    """
    Retorna hits por tipo de recurso (forum/quiz/video/pdf/resource_other/other).
    """
    try:
        d0_default, d1_default = _window_defaults()
        d0 = _parse_date("start", d0_default)
        d1 = _parse_date("end",   d1_default)
        cid = _parse_int("course_id")
        uid = _parse_int("user_id")

        sql = """
        SELECT module_bucket, SUM(hits) AS hits
        FROM moodle_agg_df_cube
        WHERE ymd >= %s AND ymd < %s
          AND (%s IS NULL OR courseid = %s)
          AND (%s IS NULL OR userid   = %s)
        GROUP BY module_bucket
        ORDER BY hits DESC
        """
        params = [d0, d1, cid, cid, uid, uid]

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        out = {
            "params": {"start": d0, "end": d1, "course_id": cid, "user_id": uid},
            "data": rows,
        }
        return _json_download(out, "resources_usage.json")

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@bp.route("/avg-session-time", methods=["GET"], strict_slashes=False)
def avg_session_time():
    """
    Retorna tempo médio por sessão (global) e série semanal.
    """
    try:
        d0_default, d1_default = _window_defaults()
        d0 = _parse_date("start", d0_default)
        d1 = _parse_date("end",   d1_default)
        cid = _parse_int("course_id")
        uid = _parse_int("user_id")

        q_global = """
        SELECT
          CASE WHEN SUM(t.sessions_count) > 0
               THEN SUM(t.avg_session_minutes * t.sessions_count) / SUM(t.sessions_count)
               ELSE NULL END AS avg_minutes_per_session
        FROM (
          SELECT DISTINCT courseid, userid, week_start, sessions_count, avg_session_minutes
          FROM moodle_agg_df_cube
          WHERE ymd >= %s AND ymd < %s
            AND (%s IS NULL OR courseid = %s)
            AND (%s IS NULL OR userid   = %s)
            AND sessions_count IS NOT NULL
            AND avg_session_minutes IS NOT NULL
        ) AS t
        """
        pg = [d0, d1, cid, cid, uid, uid]

        q_weekly = """
        SELECT
          t.week_start,
          CASE WHEN SUM(t.sessions_count) > 0
               THEN SUM(t.avg_session_minutes * t.sessions_count) / SUM(t.sessions_count)
               ELSE NULL END AS avg_minutes_per_session
        FROM (
          SELECT DISTINCT courseid, userid, week_start, sessions_count, avg_session_minutes
          FROM moodle_agg_df_cube
          WHERE ymd >= %s AND ymd < %s
            AND (%s IS NULL OR courseid = %s)
            AND (%s IS NULL OR userid   = %s)
            AND sessions_count IS NOT NULL
            AND avg_session_minutes IS NOT NULL
        ) AS t
        GROUP BY t.week_start
        ORDER BY t.week_start
        """
        pw = [d0, d1, cid, cid, uid, uid]

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(q_global, pg)
        g = cur.fetchone()
        cur.execute(q_weekly, pw)
        w = cur.fetchall()
        cur.close(); conn.close()

        out = {
            "params": {"start": d0, "end": d1, "course_id": cid, "user_id": uid},
            "global": g or {},
            "weekly": w,
        }
        return _json_download(out, "avg_session_time.json")

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@bp.route("/activation", methods=["GET"], strict_slashes=False)
def activation():
    """
    Retorna ativação 7/30 dias por curso (ou de um curso específico).
    """
    try:
        cid = _parse_int("course_id")
        if cid is None:
            sql = """
              SELECT courseid, pct_7d, pct_30d, total_matriculados, total_com_acesso, last_updated
              FROM moodle_agg_df_activation
              ORDER BY courseid
            """
            params = []
        else:
            sql = """
              SELECT courseid, pct_7d, pct_30d, total_matriculados, total_com_acesso, last_updated
              FROM moodle_agg_df_activation
              WHERE courseid = %s
            """
            params = [cid]

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        out = {
            "params": {"course_id": cid},
            "data": rows,
        }
        return _json_download(out, "activation.json")

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
