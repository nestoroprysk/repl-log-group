from threading import Lock
from time import sleep

from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

lock = Lock()
id_to_message_in_staging = {}
messages = []
message_ids = set()
next_id = 0


@app.route("/messages", methods=["POST"])
def post():
    global next_id

    print(request.json, file=sys.stderr)

    noreply = request.json.get("noreply")
    if noreply:
        return "failing by the noreply field", 500

    delay = request.json.get("delay")
    if delay:
        sleep(float(delay))

    id_ = request.json.get("id")

    msg = request.json.get("message")
    with lock:
        if id_ not in message_ids:
            # Adding an id for deduplication.
            message_ids.add(id_)

            # Adding the message to staging.
            id_to_message_in_staging[id_] = msg

            # Checking if there are messages that are ready to be delivered.
            while next_id in id_to_message_in_staging:
                messages.append(id_to_message_in_staging[next_id])
                del id_to_message_in_staging[next_id]
                next_id = next_id + 1

    return jsonify(msg)


@app.route('/messages', methods=['GET'])
def get():
    return jsonify(messages)


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/flush', methods=['POST'])
def flush():
    global next_id

    with lock:
        id_to_message_in_staging.clear()
        messages.clear()
        message_ids.clear()
        next_id = 0

    return 'ok'
