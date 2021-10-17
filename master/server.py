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
        messages.append(message)

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

    return jsonify(message)


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
