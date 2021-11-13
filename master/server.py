import os
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]
lock = threading.Lock()


class CountDownLatch:
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()

    def count_down(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notify_all()

    def wait(self):
        with self.lock:
            while self.count > 0:
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

    latch = CountDownLatch(write_concern)

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

    return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


def replicate_message(data: dict, index: int, port: str, latch: CountDownLatch) -> requests.Response:
    url = f'http://secondary-{index + 1}:{port}/messages'
    response = requests.post(
        url,
        json=data,
    )

    if response.status_code == 200:
        latch.count_down()
    else:
        pass

    return response
