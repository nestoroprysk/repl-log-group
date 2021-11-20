import threading
from typing import Optional


class CountDownLatch:
    def __init__(self, requests_count: Optional[int] = None, success_count: Optional[int] = None):
        self.requests_count = requests_count
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
