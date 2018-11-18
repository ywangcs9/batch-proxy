import threading
import queue
import time
from flask import Flask
from flask import request
from flask import abort
from request_event import RequestEvent
from batch_service import BatchService

app = Flask(__name__)

queue_put_timeout = 4  # second
queue_length = 10000
response_wait_timeout = 6  # second
batch_service_thread_num = 1  # default to use 1 thread
request_queue = queue.Queue(queue_length)
response_data = {}


@app.before_first_request
def start_batch_service():
    threads = []
    for i in range(batch_service_thread_num):
        print(i)
        batch_handler = BatchService(request_queue, response_data)
        thread = threading.Thread(target=batch_handler.start)
        threads.append(thread)
        threads[i].start()


@app.route('/', methods=['POST'])
def hello():
    print("get a request")
    json_body = request.get_json()
    req_arrived_time = int(time.time()*1000.0)
    req_event = RequestEvent(req_arrived_time, json_body)
    response_event = req_event.event
    try:
        request_queue.put(req_event, timeout=queue_put_timeout)
    except queue.Full as e:
        abort(503)

    if response_event.wait(response_wait_timeout):
        res_data = response_data.get(req_event.id)
        return res_data
    else:
        print("response timeout")
        abort(504)
        #or expose the single request interface to end user

#if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=5000, debug=True)
