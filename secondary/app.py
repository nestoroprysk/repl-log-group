import json

from flask import Flask, jsonify, request

app = Flask(__name__)

data = []


@app.route("/messages", methods=["GET", "POST"])
def get_msgs():
    if request.method == "POST":
        msg = json.loads(request.data)
        data.append(msg)
        return jsonify(msg)
    return jsonify(data)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"
