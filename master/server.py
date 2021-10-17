from collections import deque

import requests, os
from flask import Flask, request, jsonify

app = Flask(__name__)
messages = deque()

client_ports = [os.getenv('SECONDARY_1_PORT'), os.getenv('SECONDARY_2_PORT')]


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/messages', methods=['POST'])
def add_message():
    message = request.json.get('message')

    messages.append(message)

    # TODO: Do it in parallel
    for index, port in enumerate(client_ports):
        url = f'http://secondary-{index + 1}:{port}/messages'
        r = requests.post(
            url,
            json=request.get_json(),
        )

        if r.status_code is not 200:
            return f'{r.status_code}: {r.reason}'

    return jsonify(message)


@app.route('/messages', methods=['GET'])
def list_messages():
    return jsonify(list(messages))
