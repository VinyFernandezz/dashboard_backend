from flask import Blueprint, jsonify

bp = Blueprint("health", __name__, url_prefix="/health")

@bp.route("/", methods=["GET"])
def health_check():
    return jsonify(status="ok", service="dashboard_backend")
