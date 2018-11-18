import uuid
import threading


class RequestEvent:
    def __init__(self, arrived_time, data):
        self.arrived_time = arrived_time
        self.data = data
        self.event = threading.Event()
        self.id = uuid.uuid1()

