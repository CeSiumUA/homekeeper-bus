from queue import Queue
from typing import Dict

class BrokerQueue:
    def __init__(self) -> None:
        self.__queue_dict : Dict[str, Queue] = dict()

    def __ensure_audience_exists(self, audience):
        if audience not in self.__queue_dict.keys():
            self.__queue_dict[audience] = Queue()

    def enqueue(self, audience, data: bytearray, lifetime=None):
        queue_message = {'lifetime': lifetime, 'data': data,}
        self.__ensure_audience_exists(audience=audience)

        self.__queue_dict[audience].put(queue_message, block=True)

    def dequeue(self, audience) -> bytearray:
        self.__ensure_audience_exists(audience=audience)
        stored_message = self.__queue_dict[audience].get(block=True)
        return stored_message['data']
    
    def audience_msg_count(self, audience):
        self.__ensure_audience_exists(audience=audience)
        queue = self.__queue_dict[audience]
        return queue.qsize()
