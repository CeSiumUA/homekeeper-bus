from global_container import GlobalContainer
import bus_operation
import socket
import logging
import msgpack

class Client:
    def __init__(self, sock: socket.socket, ip_ep) -> None:
        self.__sock = sock
        self.ip = ip_ep[0]
        self.port = ip_ep[1]
        self.__enqueue_operation_id = None

    def __message_enqueue(self, message):
        enqueue_total_frames = message[bus_operation.BusOperationKeywords.FRAME_TOTAL_COUNT]
        enqueue_current_frame = message[bus_operation.BusOperationKeywords.FRAME]
        enqueue_operation_id = message[bus_operation.BusOperationKeywords.OPERATION_ID]

        if self.__enqueue_operation_id is None:
            self.__enqueue_operation_id = enqueue_operation_id
        elif self.__enqueue_operation_id != enqueue_operation_id:
            self.__enqueue_total_frames = 0
            self.__enqueue_current_frame = 0
            return
        



    def __process_data(self, data: bytes):
        message = msgpack.unpackb(data)
        operation_type = message[bus_operation.BusOperationKeywords.OPERATION_TYPE]

        if operation_type == bus_operation.BusOperationType.ENQUEUE:
            self.__message_enqueue(message=message)
        elif operation_type == bus_operation.BusOperationType.DEQUEUE:
            self.__message_dequeue(message=message)

    def __process_connection(self):
        try:
            while(True):
                received = self.__sock.recv(bus_operation.frame_bytes_count)
                logging.log("received {} data from client {}:{}".format(len(received), self.ip, self.port))
                self.__process_data(received)
        except:
            self.__sock.close()
            return

    def start_connection_processing(self):
        GlobalContainer.pool.apply_async(self.__process_connection)

    def close(self):
        self.__sock.close()