import queue
import json
import requests
import time


class BatchService:
    def __init__(self, request_queue, response_data, time_limit_ms=3000, req_num_limit=100, retry_count=3):
        self.queue = request_queue
        self.response_data = response_data
        self.time_limit_ms = time_limit_ms
        self.req_num_limit = req_num_limit
        self.retry_count = retry_count  # add retry later

    def start(self):
        print("batch service started")
        while True:
            request_list = []
            first_request_arrive_time = -1
            timeout = self.time_limit_ms
            while len(request_list) < self.req_num_limit and timeout > 0:
                try:
                    req_event = self.queue.get(timeout=timeout / 1000.0)  # convert millisecond to second
                    #print("in batch_handler: get a new request")
                    if len(request_list) == 0:
                        first_request_arrive_time = req_event.arrived_time
                    timeout = self.time_limit_ms - (int(time.time()*1000.0) - first_request_arrive_time)
                    request_list.append(req_event)
                except queue.Empty:
                    print("time out, queue is Empty")
                    break
            if len(request_list) > 0:
                self.send_batch_request(request_list)  # can also use different threads to handle this

    def send_batch_request(self, request_list):
        batch_data = json.loads("[]")
        for req in request_list:
            batch_data.append(req.data)
            #print(batch_data)

        print("sending a batch request, size = ", len(request_list))
        # r = requests.post('http://httpbin.org/post', json=batch_data)
        # print("batch request returns:", r.status_code)
        # time.sleep(1)

        for req in request_list:
            self.response_data[req.id] = "OK"
            req.event.set()


