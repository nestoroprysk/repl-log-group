import os
import time
from threading import Lock


def monitor_quorum(nodes_status: list, has_quorum: dict, lock: Lock):
    """
    Runs secondaries quorum monitoring
    :param nodes_status: list of secondary nodes statuses
    :param has_quorum: whether system has quorum or not
    :param lock: threading.Lock object
    """
    quorum_interval = float(os.getenv('QUORUM_INTERVAL'))

    while True:
        time.sleep(quorum_interval)

        if _system_has_quorum(nodes_status, lock):
            has_quorum['value'] = True
        else:
            has_quorum['value'] = False


def _system_has_quorum(nodes_status: list, lock: Lock) -> bool:
    """
    Checks if system has quorum
    :param nodes_status: list of secondary nodes statuses
    :param lock: threading.Lock object
    :return has_quorum: whether system has quorum or not
    """
    nodes_number = len(nodes_status)
    quorum = nodes_number / 2 + 1

    with lock:
        alive_nodes = sum(1 if status == 'healthy' else 0 for status in nodes_status) + 1

    has_quorum = True if alive_nodes >= quorum else False

    return has_quorum
