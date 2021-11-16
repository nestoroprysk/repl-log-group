from itertools import count
from threading import Lock
from time import sleep

from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

data = []
tr_id = []
counter = count()

lock = Lock()


@app.route("/messages", methods=["POST"])
def post():
    print(request.json, file=sys.stderr)

    delay = request.json.get("delay")
    if delay:
        sleep(float(delay))

    noreply = request.json.get("noreply")
    if noreply == True:
        return

    id = request.json.get("id")

    msg = request.json.get("message")
    with lock:
        check_id = next(counter)
        if not id in tr_id:
            while check_id != id:
                sleep(0.01)
            else:
                data.append(msg)
                tr_id.append(id)

    return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
