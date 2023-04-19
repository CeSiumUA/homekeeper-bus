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
        self.__enqueue_buffer = bytearray()

    def __message_enqueue(self, message):
        enqueue_total_frames = message[bus_operation.BusOperationKeywords.FRAME_TOTAL_COUNT]
        enqueue_current_frame = message[bus_operation.BusOperationKeywords.FRAME]
        enqueue_operation_id = message[bus_operation.BusOperationKeywords.OPERATION_ID]
    
        if self.__enqueue_operation_id != enqueue_operation_id:
            if self.__enqueue_total_frames != self.__enqueue_current_frame:
                #TODO add error response
                pass
            self.__enqueue_buffer.clear()
        
        self.__enqueue_total_frames = enqueue_total_frames
        self.__enqueue_current_frame = enqueue_current_frame
        self.__enqueue_operation_id = enqueue_operation_id
        self.__enqueue_buffer += message['data']

        if self.__enqueue_current_frame == self.__enqueue_total_frames:
            GlobalContainer.broker_queue.enqueue(message[bus_operation.BusOperationKeywords.AUDIENCE], self.__enqueue_buffer)

    def __message_dequeue(self, message):
        audience = message[bus_operation.BusOperationKeywords.AUDIENCE]

        stored_message = GlobalContainer.broker_queue.dequeue(audience=audience)

        data_len = len(stored_message)
        #implement batching to send response

        response_message = {}

        response_message[bus_operation.BusOperationKeywords.FRAME_TOTAL_COUNT] = 1
        response_message[bus_operation.BusOperationKeywords.FRAME] = 1
        response_message[bus_operation.BusOperationKeywords.OPERATION_ID] = ""
        response_message['data'] = stored_message

        return response_message

    def __process_data(self, data: bytes):
        message = msgpack.unpackb(data)
        operation_type = message[bus_operation.BusOperationKeywords.OPERATION_TYPE]

        if operation_type == bus_operation.BusOperationType.ENQUEUE:
            self.__message_enqueue(message=message)
        elif operation_type == bus_operation.BusOperationType.DEQUEUE:
            msg = self.__message_dequeue(message=message)
            msg = msgpack.packb(msg)
            self.__sock.send(msg)

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