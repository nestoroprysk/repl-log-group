import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from threading import Lock

import requests
import time
import sys
from flask import Flask, jsonify, request

app = Flask(__name__)

lock = Lock()
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]

counter = count()
tr_id = []


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    print(request.json, file=sys.stderr)

    # Waiting for both replicas by default (equivalent to w=3).
    expected_success_count = 2

    concern = request.json.get('w')
    if concern:
        if concern == 3:
            expected_success_count = 2
        if concern == 2:
            expected_success_count = 1
        if concern == 1:
            expected_success_count = 0

    result_lock = Lock()
    success = []
    fail = []

    executor = ThreadPoolExecutor()
    current_id = next(counter)

    for index, port in enumerate(client_ports):
        d = request.get_json()
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
            'noreply': noreply,
            'id': current_id,
        }

        def replicate_message(data: dict, index: int, port: str):
            url = f'http://secondary-{index + 1}:{port}/messages'
            response = requests.post(
                url,
                json=data,
            )

            with result_lock:
                if response.status_code == 200:
                    success.append(f'node secondary-{index + 1} replicated just fine')
                else:
                    fail.append(f'node secondary-{index + 1} returned the response code {response.status_code}: {response.reason}')

        executor.submit(replicate_message, d, index, port)

    executor.shutdown(wait=False)

    timeout = time.time() + 60/2   # half a minute from now.
    while True:
        if expected_success_count == 0:
            break # Even if no nodes succeed, whatever.

        if time.time() > timeout:
            with result_lock:
                return f"timeout exceeded with success {success} and fail {fail}", 500

        with result_lock:
            if expected_success_count == 1 and len(success) > 0:
                break # Satisfied w=2.
            if expected_success_count == 2 and len(success) == 2:
                break # Satisfied w=3.
            if len(fail) == 2:
                return f"write concern is {expected_success_count + 1} (not 1) and failed to replicate to both of the nodes: {fail}", 500
            if expected_success_count == 2 and len(fail) != 0:
                return f"write concern is {expected_success_count + 1} and failed to replicate to one of the nodes: {fail}", 500

        time.sleep(0.2)

    with lock:
        message = request.json.get('message')
        id = request.json.get('id')
        if not id in tr_id:
            messages.append(message)
            tr_id.append(id)

    return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))
