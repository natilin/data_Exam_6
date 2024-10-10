from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')

db_accident = client["chicago-accident"]

by_day = db_accident["by_day"]
by_week = db_accident["by_week"]
by_month = db_accident["by_month"]


