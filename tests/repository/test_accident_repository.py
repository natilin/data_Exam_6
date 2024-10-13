from datetime import datetime

from repository.accident_repository import *


def test_get_by_day_and_beat():
    res = get_by_day_and_beat("411", "2023-09-22")
    assert res


def test_get_by_beat():
    res = get_by_beat("41111")
    assert res


def test_get_by_week_and_beat():
    date_obj = datetime.strptime("2023-09-18", "%Y-%m-%d")
    res = get_by_week_and_beat("1655", date_obj)
    return res


def test_get_by_month_and_beat():
    date = {"year": 2023, "month": 7}
    beat = "111"
    res = get_by_month_and_beat(beat, date)
    assert res


def test_get_accident_statistic__by_beat():
    beat = "1235"
    res = get_accident_statistic__by_beat(beat)
    assert res
