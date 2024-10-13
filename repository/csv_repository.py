import csv
import os
from tqdm import tqdm
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

    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'Traffic_Crashes_Crashes.csv')


    accident_list = []
    by_days_dict = {}
    by_weeks_dict = {}
    by_months_dict = {}
    by_areas_dict = {}

    # Read CSV and process rows
    for row in tqdm(read_csv(csv_path), desc="Uploading data", unit="rows"):
        day = parse_date(row["CRASH_DATE"])
        week = get_week_range(day)
        beat = row["BEAT_OF_OCCURRENCE"]
        id = str(uuid.uuid4())

        # Accident document
        accident = {
            "_id": id,
            "beat": beat,
            "injuries_total": to_int(row["INJURIES_TOTAL"]),
            "injuries_fatal": to_int(row["INJURIES_FATAL"]),
            "injuries_incapacitating": to_int(row["INJURIES_INCAPACITATING"]),
            "injuries_non_incapacitating": to_int(row["INJURIES_NON_INCAPACITATING"]),
            "prim_contributory_cause": row["PRIM_CONTRIBUTORY_CAUSE"],
            "sec_contributory_cause": row["SEC_CONTRIBUTORY_CAUSE"],
            "crash_date": day.strftime('%Y-%m-%d'),
            "second_id": row["CRASH_RECORD_ID"]
        }
        accident_list.append(accident)

        # update daily
        if day not in by_days_dict:
            by_days_dict[day] = {"crash_date": day.strftime('%Y-%m-%d'), "beats": {}}
        if beat not in by_days_dict[day]["beats"]:
            by_days_dict[day]["beats"][beat] = {"accident_total": 0}
        by_days_dict[day]["beats"][beat]["accident_total"] += 1

        # update weekly
        if week not in by_weeks_dict:
            by_weeks_dict[week] = {"week": week, "beats": {}}
        if beat not in by_weeks_dict[week]["beats"]:
            by_weeks_dict[week]["beats"][beat] = {"accident_total": 0}
        by_weeks_dict[week]["beats"][beat]["accident_total"] += 1

        # update monthly
        month_key = (day.year, day.month)
        if month_key not in by_months_dict:
            by_months_dict[month_key] = {"month": day.month, "year": day.year, "beats": {}}
        if beat not in by_months_dict[month_key]["beats"]:
            by_months_dict[month_key]["beats"][beat] = {"accident_total": 0}
        by_months_dict[month_key]["beats"][beat]["accident_total"] += 1

        # update area
        if beat not in by_areas_dict:
            by_areas_dict[beat] = {"beat": beat, "injuries_total": 0, "injuries_fatal": 0, "crashs_list": []}
        by_areas_dict[beat]["injuries_total"] += to_int(row["INJURIES_TOTAL"])
        by_areas_dict[beat]["injuries_fatal"] += to_int(row["INJURIES_FATAL"])
        by_areas_dict[beat]["crashs_list"].append(id)


    print("list_created_successful")
    all_accident.insert_many(accident_list)
    by_days.insert_many(list(by_days_dict.values()))
    by_weeks.insert_many(list(by_weeks_dict.values()))
    by_months.insert_many(list(by_months_dict.values()))
    by_areas.insert_many(list(by_areas_dict.values()))
    print("Data_uploaded_successful")




