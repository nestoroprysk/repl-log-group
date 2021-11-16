import os
import threading
from typing import Optional
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]
lock = threading.Lock()


class CountDownLatch:
    def __init__(self, count: Optional[int] = None, success_count: Optional[int] = None):
        self.requests_count = count
        self.success_count = success_count
        self.lock = threading.Condition()

    def request_count_down(self):
        with self.lock:
            self.requests_count -= 1
            if self.requests_count <= 0:
                self.lock.notify_all()

    def success_count_down(self):
        with self.lock:
            self.success_count -= 1
            if self.success_count <= 0:
                self.lock.notify_all()

    def wait(self):
        with self.lock:
            while self.requests_count > 0 and self.success_count > 0:
                self.lock.wait()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    with lock:
        message = request.json.get('message')
        messages.append(message)
        write_concern = request.json.get('w') - 1

    latch = CountDownLatch(
        count=len(client_ports),
        success_count=write_concern,
    )

    data = request.get_json()

    futures = list()

    if write_concern == 0:
        return jsonify(message)
    else:
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
            }

            future = executor.submit(replicate_message, d, index, port, latch)
            futures.append(future)

        executor.shutdown(wait=False)

    latch.wait()

    if latch.success_count > 0:
        # todo: detailed message
        return 'Error', 500
    else:
        return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


def replicate_message(data: dict, index: int, port: str, latch: CountDownLatch) -> requests.Response:
    url = f'http://secondary-{index + 1}:{port}/messages'
    try:
        response = requests.post(
            url,
            json=data,
            timeout=10,
        )
        response.raise_for_status()

        if response.status_code == 200:
            latch.success_count_down()

        latch.request_count_down()

        return response

    except Exception as e:
        latch.request_count_down()
        return str(e), 500

