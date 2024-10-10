from repository.accident_repository import *


def test_get_by_day_and_beat():
    res = get_by_day_and_beat("411", "2023-09-22")
    assert res


def test_get_by_beat():
    res = get_by_beat("411")
    assert res