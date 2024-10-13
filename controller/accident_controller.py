from datetime import datetime

from flask import Blueprint, jsonify, request
from repository.accident_repository import *
import json
from bson import json_util
from repository.csv_repository import init_accident_db


def parse_json(data):
    return json.loads(json_util.dumps(data))



accident_bluprint = Blueprint("accident", __name__)

@accident_bluprint.route('/injection/<beat>', methods=['GET'])
def get_accident_injection_by_beat(beat):
    res = get_accident_statistic__by_beat(beat)
    return parse_json(res), 200


@accident_bluprint.route('/<beat>', methods=['GET'])
def get_accident_by_beat(beat):
    res = get_by_beat(beat)
    return parse_json(res), 200



@accident_bluprint.route('/byDay/', methods=['POST'])
def get_accident_by_beat_and_day():
    data = request.get_json()
    beat = data.get("beat")
    date = data.get("date")
    res = get_by_day_and_beat(beat, date)
    return parse_json(res), 200


@accident_bluprint.route('/byWeek/', methods=["POST"])
def get_accident_by_beat_and_week():
    data = request.get_json()
    beat = data.get("beat")
    date = data.get("date")
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    res = get_by_week_and_beat(beat, date_obj)
    return parse_json(res), 200


@accident_bluprint.route('/byMonth/', methods=['POST'])
def get_accident_by_beat_and_month():
    data = request.get_json()
    beat = data.get("beat")
    date = data.get("date")
    res = get_by_month_and_beat(beat, date)
    return parse_json(res), 200


@accident_bluprint.route('/init', methods=['GET'])
def seed_db():
    init_accident_db()
    return jsonify({"message": "Dada uploaded successfully"}), 200