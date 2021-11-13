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

    msg = request.json.get("message")
    delay = request.json.get("delay")
    noreply = request.json.get("noreply")

    if delay:
        sleep(float(delay))

    if noreply:
        return ''
    else:
        with lock:
            data.append(msg):

        return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
