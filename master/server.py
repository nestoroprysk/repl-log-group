import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import requests
from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

lock = Lock()
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    print(request.json, file=sys.stderr)

    futures = list()
    data = request.get_json()

    with ThreadPoolExecutor() as executor:
        for index, port in enumerate(client_ports):
            d = data
            delay = 0
            noreply = False

            conf = request.json.get(f'secondary-{index + 1}')
            if conf:
                if conf.get('delay'):
                    delay = conf.get('delay')

                if conf.get('noreply'):
                    noreply = conf.get('noreply')

            d = {
                'message': d.get('message'),
                'delay': delay,
                'noreply': noreply
            }

            future = executor.submit(replicate_message, d, index, port)
            futures.append(future)

    responses = [f.result() for f in futures]
    for r in responses:
        if r.status_code is not 200:
            return f'{r.status_code}: {r.reason}'

    with lock:
        message = request.json.get('message')
        messages.append(message)

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
