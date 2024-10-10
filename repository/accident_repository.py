from database.connect import by_days, by_weeks, by_months, by_areas, all_accident

def get_accident_statistic__by_beat(beat):
    return list(by_areas.aggregate([
        {
            "$match": {"beat": beat}
        },
        {
            "$lookup": {
                "from": "all_accident",
                "localField": "crashs_list",
                "foreignField": "_id",
                "as": "accident_details"
            }
        }
    ]))


