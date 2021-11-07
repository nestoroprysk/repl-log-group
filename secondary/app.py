from threading import Lock
from time import sleep

from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

data = []
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

    msg = request.json.get("message")
    with lock:
        data.append(msg)

    return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
