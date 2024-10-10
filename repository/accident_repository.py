from database.connect import by_days, by_weeks, by_months, by_areas, all_accident

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


def get_by_day_and_beat(beat, date):
    res =  by_days.find_one({"crash_date": date}, { "_id": 0})
    return res["beats"][0][beat]


def get_by_beat(beat):
    res = by_areas.find_one({"beat": beat},{"injuries_total": 0, "injuries_fatal": 0})
    return {"amount": len(res["crashs_list"])}

