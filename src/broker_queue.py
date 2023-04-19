from queue import Queue

class BrokerQueue:
    def __init__(self) -> None:
        self.__queue = Queue()