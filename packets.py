from enum import Enum
import logging
import pickle
import struct
import json
from typing import Optional


class PacketType(str, Enum):
    """Current packet types supported by failure detector"""
    PING = "000000"
    ACK = "000001"
    INTRODUCE = "000010"
    INTRODUCE_ACK = "000011"
    FETCH_INTRODUCER = "000100"
    FETCH_INTRODUCER_ACK = "000101"
    ELECTION = '000110'
    COORDINATE = '000111'
    COORDINATE_ACK = '001000'
    UPDATE_INTRODUCER = '001001'
    DOWNLOAD_FILE = '001010'
    DOWNLOAD_FILE_SUCCESS = '001011'
    DOWNLOAD_FILE_FAIL = '001100'
    DELETE_FILE = '001101'
    DELETE_FILE_ACK = '001110'
    DELETE_FILE_NAK = '001111'
    GET_FILE = '010000'
    GET_FILE_SUCCESS = '010001'
    GET_FILE_FAIL = '010010'
    PUT_REQUEST = '010011'
    LIST_FILE_REQUEST = '010100'
    LIST_FILE_REQUEST_ACK = '010101'
    GET_FILE_REQUEST = '010110'
    GET_FILE_REQUEST_ACK = '010111'
    PUT_REQUEST_ACK = '011000',
    PUT_REQUEST_SUCCESS = '011001'
    DELETE_FILE_REQUEST = '011010'
    DELETE_FILE_REQUEST_ACK = '011011'
    DELETE_FILE_REQUEST_SUCCESS = '011100'
    DELETE_FILE_REQUEST_FAIL = '011101'
    PUT_REQUEST_FAIL = '011110'
    REPLICATE_FILE = '011111'
    REPLICATE_FILE_SUCCESS = '100000'
    REPLICATE_FILE_FAIL = '100001'
    ALL_LOCAL_FILES = '100010'
    GET_FILE_NAMES_REQUEST = '100011'
    GET_FILE_NAMES_REQUEST_ACK = '100100'
    SUBMIT_JOB_REQUEST = '100101'
    SUBMIT_JOB_REQUEST_ACK = '100110'
    WORKER_TASK_REQUEST = '100111'
    WORKER_TASK_REQUEST_ACK = '101000'
    SUBMIT_JOB_REQUEST_SUCCESS = '101001'
    WORKER_KILL_TASK_REQUEST = '101010'
    WORKER_KILL_TASK_REQUEST_ACK = '101011'
    SUBMIT_JOB_RELAY = "101100"
    WORKER_TASK_ACK_RELAY = "101101"
    ALL_LOCAL_FILES_RELAY = "101110"
    SET_BATCH_SIZE = '101111'
    GET_C2_COMMAND = '110000'
    GET_C2_COMMAND_ACK = "110001"

class Packet:
    """Custom packet type for failure detector"""

    def __init__(self, sender: str, packetType: PacketType, data: dict):
        self.data = data
        self.type = packetType
        self.sender = sender

    def pack(self) -> bytes:
        """Returns the bytes for packet"""
        jsondata = json.dumps(self.data)
        return struct.pack(f"i{255}s{6}si{2048 * 16}s", len(self.sender), self.sender.encode('utf-8'), self.type.encode('utf-8'), len(jsondata), jsondata.encode())

        # pickled = pickle.dumps(self, pickle.HIGHEST_PROTOCOL)
        # return pickled

    @staticmethod
    def unpack(recvPacket: bytes):
        """Converts the bytes to Packet class"""
        try:
            unpacked_tuple: tuple[bytearray] = struct.unpack(
                f"i{255}s{6}si{2048 * 16}s", recvPacket)
            sender = unpacked_tuple[1][:unpacked_tuple[0]].decode('utf-8')
            packetType = unpacked_tuple[2].decode('utf-8')
            data = unpacked_tuple[4][:unpacked_tuple[3]].decode('utf-8')

            # print(sender, packetType, data)
            return Packet(sender, PacketType(packetType), json.loads(data))
        except Exception as e:
            logging.error(f"unknown bytes: {e}")
            return None
