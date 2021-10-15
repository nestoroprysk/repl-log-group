from time import sleep

from flask import Flask, jsonify, request

app = Flask(__name__)

data = []


@app.route("/messages", methods=["GET", "POST"])
def get_msgs():
    if request.method == "POST":
        msg = request.json.get("message")
        delay = request.json.get("delay")

        if delay:
            sleep(float(delay))

        data.append(msg)

        return jsonify(msg)
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
