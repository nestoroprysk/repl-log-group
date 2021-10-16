import os
from threading import Thread, Lock
from collections import deque
from typing import Optional

import requests
from flask import Flask, request, jsonify

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

    threads = list()
    data = request.get_json()
    for index, port in enumerate(client_ports):

        thread = Thread(target=replicate_message, args=(data, index, port))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


def replicate_message(data: dict, index: int, port: str, ) -> Optional[str]:
    url = f'http://secondary-{index + 1}:{port}/messages'
    r = requests.post(
        url,
        json=data,
    )

    if r.status_code is not 200:
        return f'{r.status_code}: {r.reason}'
