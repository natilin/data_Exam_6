from repository.csv_repository import init_accident_db
from flask import Flask




from controller.accident_controller import accident_bluprint

app = Flask(__name__)

if __name__ == '__main__':
    app.register_blueprint(accident_bluprint, url_prefix="/api/accident")
    app.run(debug=True)