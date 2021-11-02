import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
messages = deque()

client_ports = [os.getenv("SECONDARY_1_PORT"), os.getenv("SECONDARY_2_PORT")]
lock = Lock()


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"


@app.route("/messages", methods=["POST"])
def add_message():
    desired_replication_count = 3
    message = request.json.get("message")
    successful_replication_count = request.json.get("w")
    desired_replication_count -= 1

    futures = list()
    data = request.get_json()

    with ThreadPoolExecutor() as executor:
        for index, port in enumerate(client_ports):
            future = executor.submit(replicate_message, data, index, port)
            futures.append(future)

    for f in futures:
        r = f.result()
        if r.status_code == 200:
            desired_replication_count -= 1
        else:
            return f"{r.status_code}: {r.reason}"
    with lock:
        if successful_replication_count >= desired_replication_count:
            messages.append(message)
            return jsonify(message)
    raise Exception(
        f"write concern violated: successfully replicated to {3-desired_replication_count} node, failed to replicate to {desired_replication_count} node"
    )


@app.route("/messages", methods=["GET"])
def list_messages():
    return jsonify(list(messages))


def replicate_message(data: dict, index: int, port: str) -> requests.Response:
    url = f"http://secondary-{index + 1}:{port}/messages"
    response = requests.post(
        url,
        json=data,
    )

    return response
