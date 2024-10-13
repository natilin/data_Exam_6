from database.connect import by_days, by_weeks, by_months, by_areas, all_accident
from utils.csv_utils import get_week_range



def get_by_beat(beat):
    res = by_areas.find_one({"beat": beat},{"injuries_total": 0, "injuries_fatal": 0})
    return {"amount": len(res["crashs_list"]) if res else 0}



def get_by_day_and_beat(beat, date):
    return by_days.find_one(
        {"$and": [
            {"crash_date": date},
            {f"beats.{beat}": {"$exists": True}},
        ]},
        {f"beats.{beat}": 1, "_id": 0, "crash_date": 1}
    )


def get_by_week_and_beat(beat, date):
    start_day_of_week = get_week_range(date)[0]
    return by_weeks.find_one(
        {"$and": [
            {"week.0": start_day_of_week},
            {f"beats.{beat}": {"$exists": True}},
        ]},
        {f"beats.{beat}": 1, "_id": 0, "week": 1}
    )



def get_by_month_and_beat(beat, date):
    return by_months.find_one(
        {"$and":[
            {"year": date["year"]},
            {"month": date["month"]},
            {f"beats.{beat}": {"$exists": True}},
        ]},
        {f"beats.{beat}": 1, "_id": 0, "month": 1, "year": 1}
    )







def get_accident_statistic__by_beat(beat):
    res =  list(by_areas.aggregate([
        {
            "$match": {"beat": beat}
        },
        {
            "$lookup": {
                "from": "all_accident",
                "localField": "crashs_list",
                "foreignField": "_id",
                "as": "crashs_list"
            }
        },
    ]))
    res = res[0]
    res["injuries_non_fatal"] = res["injuries_total"] - res["injuries_fatal"]
    return res



