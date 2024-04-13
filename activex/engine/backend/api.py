

from flask import Flask
from flask_cors import CORS

app=Flask(__name__)
global db

@app.route("/object/<alias>",methods=["GET"])
def objects(alias):
    global db
    print(db)
    respose = db.find(alias)
    print(respose)
    return respose


def start_app(obj_service):
    global db
    db=obj_service._db
    app.run(host=obj_service._host, port=obj_service._port, debug=True, threaded=True)