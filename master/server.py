import os
import threading
from typing import Union, Tuple
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from threading import Lock

import requests
from flask import Flask, jsonify, request

try:
    from latch import CountDownLatch
except ImportError:
    from .latch import CountDownLatch


app = Flask(__name__)
messages = deque()

secondaries_number = int(os.getenv('SECONDARIES_NUMBER'))
client_ports = [os.getenv(f'SECONDARY_{i}_PORT') for i in range(1, secondaries_number + 1)]
lock = threading.Lock()

counter = count()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    with lock:
        message = request.json.get('message')
        current_id = next(counter)

        wait_count = secondaries_number
        w = request.json.get('w')
        if w is not None and w > 0 and w <= secondaries_number + 1:
            wait_count = w - 1

        messages.append(message)

    latch = CountDownLatch(
        requests_count=len(client_ports),
        success_count=wait_count,
    )

    data = request.get_json()

    executor = ThreadPoolExecutor()

    for index, port in enumerate(client_ports):
        conf = request.json.get(f'secondary-{index + 1}')
        if conf:
            delay = conf.get('delay', 0)
            noreply = conf.get('noreply', False)
        else:
            delay = 0
            noreply = False

        d = {
            'message': data.get('message'),
            'delay': delay,
            'noreply': noreply,
            'id': current_id,
        }

        executor.submit(replicate_message, d, index, port, latch)

    executor.shutdown(wait=False)

    latch.wait()

    if latch.success_count > 0:
        return 'write concern {w} violated ({lath.success_count} failed)', 500
    else:
        return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


def replicate_message(
        data: dict,
        index: int,
        port: str,
        latch: CountDownLatch,
) -> Union[requests.Response, Tuple[str, int]]:
    url = f'http://secondary-{index + 1}:{port}/messages'
    try:
        response = requests.post(
            url,
            json=data,
        )
        response.raise_for_status()

        if response.status_code == 200:
            latch.success_count_down()

        latch.request_count_down()

        return response

    except Exception as e:
        latch.request_count_down()
        return str(e), 500
