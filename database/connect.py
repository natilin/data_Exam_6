from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')

db_accident = client["chicago-accident_BD"]

by_days = db_accident["by_day"]
by_weeks = db_accident["by_week"]
by_months = db_accident["by_month"]
by_areas = db_accident["by_area"]
all_accident = db_accident["all_accident"]




