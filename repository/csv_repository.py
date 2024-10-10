import csv
import os
from utils.csv_utils import parse_date, to_int, get_week_range
import uuid



from database.connect import by_days, by_weeks, by_months, by_areas, all_accident

def read_csv(csv_path):
    with open(csv_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def init_accident_db():
    by_days.drop()
    by_weeks.drop()
    by_months.drop()
    by_areas.drop()
    all_accident.drop()

    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'data.csv')
    accident_list = []
    by_days_list = []
    by_weeks_list = []
    by_months_list = []
    by_areas_list = []

    line = 0
    for row in read_csv(csv_path):
        # if line > 1000:
        #     break
        line += 1
        day = parse_date(row["CRASH_DATE"])
        week = get_week_range(day)
        id = str(uuid.uuid4())
        total = to_int(row["INJURIES_TOTAL"])

        accident = {
            "_id": id,
            "beat": row["BEAT_OF_OCCURRENCE"],
            "injuries_total": to_int(row["INJURIES_TOTAL"]),
            "injuries_fatal": to_int(row["INJURIES_FATAL"]),
            "injuries_incapacitating": to_int(row["INJURIES_INCAPACITATING"]),
            "injuries_non_incapacitating": to_int(row["INJURIES_NON_INCAPACITATING"]),
            "prim_contributory_cause": row["PRIM_CONTRIBUTORY_CAUSE"],
            "sec_contributory_cause": row["SEC_CONTRIBUTORY_CAUSE"],
            "crash_date": day.strftime('%Y-%m-%d')
        }
        accident_list.append(accident)


        updated_day_list = False
        in_the_day_beats = False
        for crash in by_days_list:
            if updated_day_list:
                break
            if day.strftime('%Y-%m-%d') == crash["crash_date"]:
                for beat_in_day in crash["beats"]:
                    if in_the_day_beats:
                        break

                    elif row["BEAT_OF_OCCURRENCE"] in beat_in_day:
                        beat_in_day[row["BEAT_OF_OCCURRENCE"]]["accident_total"] += 1
                        in_the_day_beats = True
                        updated_day_list = True
                        break

                if not in_the_day_beats:
                    crash["beats"].append({row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}})
                    updated_day_list = True
                break



        if not updated_day_list:
            by_day = {
                "crash_date": day.strftime('%Y-%m-%d'),
                "beats": [{row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}}]
            }
            by_days_list.append(by_day)

        # week collection

        updated_week_list = False
        in_beats = False

        for week_stats in by_weeks_list:

            if updated_week_list:
                break

            elif week_stats["week"] == week:

                for beat in week_stats["beats"]:
                    if in_beats:
                        break

                    elif row["BEAT_OF_OCCURRENCE"] in beat:
                        beat[row["BEAT_OF_OCCURRENCE"]]["accident_total"] += 1
                        in_beats = True
                        updated_week_list = True
                        break

                if not in_beats:
                    week_stats["beats"].append({row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}})
                    updated_week_list = True
                break


        if not updated_week_list:
            by_week = {
                "week": week,
                "beats": [{row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}}]
            }
            by_weeks_list.append(by_week)


        # month collection
        in_beat_of_month = False
        updated_month_list = False

        for month in by_months_list:
            if updated_month_list:
                break

            if month["month"] == day.month and month["year"] == day.year:
                for beat in month["beats"]:
                    if in_beat_of_month:
                        break

                    elif row["BEAT_OF_OCCURRENCE"] in beat:
                        beat[row["BEAT_OF_OCCURRENCE"]]["accident_total"] += 1
                        in_beat_of_month = True
                        updated_month_list = True
                        break

                if not in_beat_of_month:
                    month["beats"].append({row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}})
                    updated_month_list = True
                break

        if not updated_month_list:
            by_month = {
                "month": day.month,
                "year": day.year,
                "beats": [{row["BEAT_OF_OCCURRENCE"]: {"accident_total": 1}}]
            }
            by_months_list.append(by_month)


        # area collection


        in_areas_list = False
        for area in by_areas_list:
            if area == row["BEAT_OF_OCCURRENCE"]:
                area["injuries_total"] += to_int(row["INJURIES_TOTAL"])
                area["injuries_fatal"] += to_int(row["INJURIES_FATAL"])
                area["crashs_list"].append(id)
                in_areas_list = True

        if not in_areas_list:
            by_area = {
                "beat": row["BEAT_OF_OCCURRENCE"],
                "injuries_total": to_int(row["INJURIES_TOTAL"]),
                "injuries_fatal": to_int(row["INJURIES_FATAL"]),
                "crashs_list": [id]
            }
            by_areas_list.append(by_area)

    print("list_created_successful")

    all_accident.insert_many(accident_list)
    by_days.insert_many(by_days_list)
    by_weeks.insert_many(by_weeks_list)
    by_months.insert_many(by_months_list)
    by_areas.insert_many(by_areas_list)








