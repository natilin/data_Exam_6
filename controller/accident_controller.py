from flask import Blueprint, jsonify
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



@accident_bluprint.route('/init', methods=['GET'])
def seed_db():
    init_accident_db()
    return jsonify({"result": "success"}), 200