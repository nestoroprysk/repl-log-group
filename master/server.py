import os
import threading
from typing import Union, Tuple
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import count

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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
        if w is not None and 0 < w <= secondaries_number + 1:
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

        secondary_data = {
            'message': data.get('message'),
            'delay': delay,
            'noreply': noreply,
            'id': current_id,
        }

        executor.submit(replicate_message, secondary_data, index, port, latch)

    executor.shutdown(wait=False)

    latch.wait()

    if latch.success_count > 0:
        return f'write concern {w} violated ({latch.success_count} failed)', 500
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
    """
    Replicates message for a single secondary node
    :param data: request body
    :param index: node index
    :param port: secondary node port
    :param latch: CountDownLatch object for synchronization
    :return: requests.Response object or (error_message, status_code)
    """
    url = f'http://secondary-{index + 1}:{port}/messages'
    try:
        response = _send_request(url, data)
        response.raise_for_status()

        if response.status_code == 200:
            latch.success_count_down()

        latch.request_count_down()

        return response

    except Exception as e:
        latch.request_count_down()
        return str(e), 500


def _send_request(url: str, data: dict) -> requests.Response:
    """
    Performs POST request with retry
    :param url: node url
    :param data: request body
    :return: requests.Response object
    """
    noreply = data.get('noreply')

    with requests.Session() as session:
        if not noreply:
            retries = Retry(
                total=None,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=frozenset(['GET', 'POST']),
            )

            session.mount('http://', HTTPAdapter(max_retries=retries))

        response = session.post(url, json=data, timeout=5)

        return response
