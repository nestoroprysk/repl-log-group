import os
import time
import logging
from threading import Lock
from collections import UserList

import requests

secondaries_number = int(os.getenv('SECONDARIES_NUMBER'))
heartbeats_number = int(os.getenv('HEARTBEATS_NUMBER'))
heartbeats_interval = float(os.getenv('HEARTBEATS_INTERVAL'))
client_ports = [os.getenv(f'SECONDARY_{i}_PORT') for i in range(1, secondaries_number + 1)]

logger = logging.getLogger(__name__)


class NodesStatus(UserList):
    def __init__(self, secondaries_number: int):
        """
        :param secondaries_number: number of secondary nodes
        """
        super().__init__()
        self.data = ['healthy'] * secondaries_number

    def update(self, index: int, status: str):
        """
        :param index: node index
        :param status: node health status
        """
        prev_status = self[index]

        if prev_status != 'healthy' and status == 'healthy':
            super().__setitem__(index, status)
            logger.warning(f'Node secondary-{index + 1} recovered')
        else:
            super().__setitem__(index, status)


def check_node_status(index: int, port: str) -> bool:
    """
    Sends GET request to check secondary node liveness
    :param index: node index
    :param port: secondary node port
    :return: bool; whether secondary node is alive or not
    """
    url = f'http://secondary-{index + 1}:{port}/ping'
    response = requests.get(url, timeout=1)

    if response.status_code == 200:
        return True
    else:
        return False


def check_health() -> dict:
    """
    Performs `heartbeats_number` liveness checks and sets nodes status
    :return statuses: secondary nodes statuses
    """
    statuses = {}

    for index, port in enumerate(client_ports):
        is_healthy = list()
        for _ in range(heartbeats_number):
            try:
                is_alive = check_node_status(index, port)
            except Exception:
                is_alive = False

            is_healthy.append(is_alive)
            status = _get_status(is_healthy)
            statuses[index] = status

        if statuses[index] == 'unhealthy':
            logger.warning(f'secondary-{index + 1} is unavailable')

    return statuses


def _get_status(is_healthy: list) -> str:
    """
    :param is_healthy: list of liveness check results for secondary node
    :return: node status
    """
    acknowledgements = sum(is_healthy)

    if acknowledgements == heartbeats_number:
        return 'healthy'
    elif 0 < acknowledgements < heartbeats_number:
        return 'suspected'
    else:
        return 'unhealthy'


def monitor_nodes_status(nodes_status: NodesStatus, lock: Lock):
    """
    Runs secondaries status monitoring
    :param nodes_status: list of secondary nodes statuses
    :param lock: threading.Lock object
    """
    while True:
        time.sleep(heartbeats_interval)

        with lock:
            statuses = check_health()

            for node_index, status in statuses.items():
                nodes_status.update(node_index, status)
