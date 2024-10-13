from repository.csv_repository import init_accident_db
from database.connect import all_accident
def test_init_accident_db():
    init_accident_db()
    res = all_accident.count_documents({})
    assert res

