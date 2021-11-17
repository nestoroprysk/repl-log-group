from threading import Lock
from time import sleep

from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

data = {}
tr_id = []
lock = Lock()


def extract_data(data: dict):
    result = [data[i] for i in sorted(data)]
    if len(result) == 1:
        return result
    result = [result[0]]
    sorted_data_id = sorted(data)
    for i in range(1, len(sorted_data_id)):
        if sorted_data_id[i]-1 == sorted_data_id[i-1]:
            result.append(data[sorted_data_id[i]])
    return result


@app.route("/messages", methods=["POST"])
def post():
    print(request.json, file=sys.stderr)

    delay = request.json.get("delay")
    if delay:
        sleep(float(delay))

    noreply = request.json.get("noreply")
    if noreply == True:
        return "", 500

    id = request.json.get("id")

    msg = request.json.get("message")
    with lock:
        if not id in tr_id:
            data[id] = msg
            tr_id.append(id)

    return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    result = extract_data(data)
    return jsonify(result)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
