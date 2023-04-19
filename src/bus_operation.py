from enum import Enum

frame_bytes_count = 4096

class BusOperationType(Enum):
    ENQUEUE = 0
    DEQUEUE = 1
    MSG_CNT = 2
    STATUS  = 3

class BusOperationKeywords:
    OPERATION_TYPE = 'op_type'
    FRAME = 'frame_num'
    FRAME_TOTAL_COUNT = 'frame_cnt'
    OPERATION_ID = 'op_id'

class BusStatusType(Enum):
    WRONG_OPERATION_ID = 0