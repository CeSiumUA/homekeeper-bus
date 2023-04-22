from global_container import GlobalContainer
import bus_operation
import socket
import logging
import msgpack
import time
import math
import uuid

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
        self.__enqueue_buffer += message[bus_operation.BusOperationKeywords.DATA]

        if self.__enqueue_current_frame == self.__enqueue_total_frames:
            GlobalContainer.broker_queue.enqueue(message[bus_operation.BusOperationKeywords.AUDIENCE], self.__enqueue_buffer, int(message[bus_operation.BusOperationKeywords.LIFETIME]) + int(time.time()))

    def __message_dequeue(self, message):
        audience = message[bus_operation.BusOperationKeywords.AUDIENCE]

        stored_message = GlobalContainer.broker_queue.dequeue(audience=audience)

        data_len = len(stored_message)

        bytes_to_send = bus_operation.frame_bytes_count - bus_operation.frame_header_length

        overall_frames = math.ceil(data_len / bytes_to_send)

        frame_counter = 1

        op_id = str(uuid.uuid4())

        while data_len > 0:
            skip = (frame_counter - 1) * bytes_to_send
            response_message = {}

            response_message[bus_operation.BusOperationKeywords.FRAME_TOTAL_COUNT] = overall_frames
            response_message[bus_operation.BusOperationKeywords.FRAME] = frame_counter
            response_message[bus_operation.BusOperationKeywords.OPERATION_ID] = op_id
            response_message[bus_operation.BusOperationKeywords.DATA] = stored_message[skip:(skip+bytes_to_send)]
            encoded_msg = msgpack.packb(response_message)
            self.__sock.sendall(encoded_msg)
            frame_counter += 1
            data_len -= bytes_to_send

    def __message_count(self, message):
        aud = message[bus_operation.BusOperationKeywords.AUDIENCE]
        size = GlobalContainer.broker_queue.audience_msg_count(audience=aud)

        data_message = {
            'audience_message_count': size
        }

        response_message = {}
        response_message[bus_operation.BusOperationKeywords.FRAME_TOTAL_COUNT] = 1
        response_message[bus_operation.BusOperationKeywords.FRAME] = 1
        response_message[bus_operation.BusOperationKeywords.OPERATION_ID] = str(uuid.uuid4())
        response_message[bus_operation.BusOperationKeywords.DATA] = msgpack.packb(data_message)
        encoded_msg = msgpack.packb(response_message)
        self.__sock.sendall(encoded_msg)


    def __process_data(self, data: bytes):
        message = msgpack.unpackb(data)
        operation_type = message[bus_operation.BusOperationKeywords.OPERATION_TYPE]

        if operation_type == bus_operation.BusOperationType.ENQUEUE:
            self.__message_enqueue(message=message)
        elif operation_type == bus_operation.BusOperationType.DEQUEUE:
            self.__message_dequeue(message=message)
        elif operation_type == bus_operation.BusOperationType.MSG_CNT:
            self.__message_count(message=message)

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