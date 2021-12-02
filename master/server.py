import os
import threading
import logging
from typing import Union, Tuple
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from flask import Flask, jsonify, request

try:
    from latch import CountDownLatch
    from health import NodesStatus, monitor_nodes_status
    from quorum import monitor_quorum
except ImportError:
    from .latch import CountDownLatch
    from .health import NodesStatus, monitor_nodes_status
    from .quorum import monitor_quorum

logging.basicConfig(level=logging.WARNING)
logging.getLogger(__name__).setLevel(logging.DEBUG)

app = Flask(__name__)

secondaries_number = int(os.getenv('SECONDARIES_NUMBER'))
secondary_ports = [os.getenv(f'SECONDARY_{i}_PORT') for i in range(1, secondaries_number + 1)]

lock = threading.Lock()
messages = deque()
next_id = 0

status_lock = threading.Lock()
nodes_status = NodesStatus(secondaries_number)
health_monitoring = threading.Thread(target=monitor_nodes_status, args=(nodes_status, status_lock))
health_monitoring.start()

system_has_quorum = {'value': True}
quorum_monitoring = threading.Thread(target=monitor_quorum, args=(nodes_status, system_has_quorum, status_lock))
quorum_monitoring.start()


@app.route('/messages', methods=['POST'])
def add_message():
    global next_id
    global system_has_quorum

    if not system_has_quorum['value']:
        return 'ERROR: >50% nodes are unavailable (no quorum)', 500

    with lock:
        message = request.json.get('message')
        current_id = next_id
        next_id = next_id + 1

        wait_count = secondaries_number
        w = request.json.get('w')
        if w is not None and 0 < w <= secondaries_number + 1:
            wait_count = w - 1

        messages.append(message)

    latch = CountDownLatch(
        requests_count=len(secondary_ports),
        success_count=wait_count,
    )

    data = request.get_json()

    executor = ThreadPoolExecutor()

    for index, port in enumerate(secondary_ports):
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
        with status_lock:
            status = nodes_status[index]

        if status == 'healthy':
            response = requests.post(url, json=data)
        else:
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


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/flush', methods=['POST'])
def flush():
    global next_id

    with lock:
        messages.clear()
        next_id = 0

    return 'ok'


@app.route('/health', methods=['GET'])
def check_health():
    result = {f'secondary-{index + 1}': status for index, status in enumerate(nodes_status)}

    return jsonify(result)
