from multiprocessing.pool import ThreadPool
from broker_queue import BrokerQueue

class GlobalContainer:
    pool: ThreadPool = ThreadPool()
    broker_queue: BrokerQueue = BrokerQueue()