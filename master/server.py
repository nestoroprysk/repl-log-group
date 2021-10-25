import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]
lock = Lock()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    with lock:
        message = request.json.get('message')
        write_concern = request.json.get('w')
        if write_concern and not isinstance(write_concern, int):
            write_concern = int(write_concern)
        messages.append(message)
        write_concern -= 1

    futures = list()
    data = request.get_json()

    with ThreadPoolExecutor() as executor:
        for index, port in enumerate(client_ports):
            future = executor.submit(replicate_message, data, index, port)
            futures.append(future)

    responses = [f.result() for f in futures]
    for r in responses:
        if r.status_code is not 200:
            return f'{r.status_code}: {r.reason}'
        else:
            write_concern -= 1
    if write_concern <= 0:
        return jsonify(message)
    return None


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


def replicate_message(data: dict, index: int, port: str) -> requests.Response:
    url = f'http://secondary-{index + 1}:{port}/messages'
    response = requests.post(
        url,
        json=data,
    )

    return response
