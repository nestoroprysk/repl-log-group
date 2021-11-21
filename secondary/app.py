from threading import Lock
from time import sleep

from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

data = []
message_ids = set()
lock = Lock()


@app.route("/messages", methods=["POST"])
def post():
    print(request.json, file=sys.stderr)

    noreply = request.json.get("noreply")
    if noreply:
        return "failing by the noreply field", 500

    delay = request.json.get("delay")
    if delay:
        sleep(float(delay))

    id_ = request.json.get("id")

    msg = request.json.get("message")
    with lock:
        if id_ not in message_ids:
            data.append(msg)
            message_ids.add(id_)

    return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
