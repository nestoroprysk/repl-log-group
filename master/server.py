from collections import deque

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
messages = deque()

client_ports = ['8081', '8082']


@app.route('/ping/', methods=['GET'])
def ping():
    return 'ping'


@app.route('/message/', methods=['POST'])
def add_message():
    message = request.json.get('message')
    messages.append(message)

    for index, port in enumerate(client_ports):
        url = f'http://secondary{index}:{port}/message'
        r = requests.post(
            url,
            json={'message': message},
        )

        if not r.text == 'replicated':
            return f'{r.status_code}: {r.reason}'

    return jsonify(message)


@app.route('/messages/', methods=['GET'])
def list_messages():
    return jsonify(list(messages))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
