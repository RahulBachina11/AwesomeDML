from datetime import datetime
import logging
import sys
import asyncio
from asyncio import Event, exceptions
from time import time
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn, Optional
from config import Config
from nodes import Node
from packets import Packet, PacketType
from protocol import AwesomeProtocol
# from membershipList import MemberShipList

class Worker:
    """Main worker class to handle all the failure detection and sends PINGs and ACKs to other nodes"""
    def __init__(self, io: AwesomeProtocol) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Node,
                                         WeakSet[Event]] = WeakKeyDictionary()
        self.config: Config = None
        # self.membership_list: Optional[MemberShipList] = None

    def initialize(self, config: Config) -> None:
        """Function to initialize all the required class for Worker"""
        self.config = config
        self.is_current_node_active = True

    def _add_waiting(self, node: Node, event: Event) -> None:
        """Function to keep track of all the unresponsive PINGs"""
        waiting = self._waiting.get(node)
        if waiting is None:
            self._waiting[node] = waiting = WeakSet()
        waiting.add(event)

    def _notify_waiting(self, node) -> None:
        """Notify the PINGs which are waiting for ACKs"""
        waiting = self._waiting.get(node)
        if waiting is not None:
            for event in waiting:
                event.set()

    async def _run_handler(self) -> NoReturn:
        """RUN the main loop which handles all the communication to external nodes"""
        
        while True:
            packedPacket, host, port = await self.io.recv()

            packet: Packet = Packet.unpack(packedPacket)
            if (not packet) or (not self.is_current_node_active):
                continue

            logging.debug(f'got data: {packet.data} from {packet.sender}')

            if packet.type == PacketType.FETCH_INTRODUCER:
                print(f'{datetime.now()}: received ping from {packet.sender}')
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.FETCH_INTRODUCER_ACK, {'introducer': self.config.introducer}).pack())
                print("sent ACK")
            elif packet.type == PacketType.UPDATE_INTRODUCER:
                self.config.introducer = packet.sender
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.FETCH_INTRODUCER_ACK, {'introducer': self.config.introducer}).pack())
                print("sent ACK")

    # async def _wait(self, node: Node, timeout: float) -> bool:
    #     """Function to wait for ACKs after PINGs"""
    #     event = Event()
    #     self._add_waiting(node, event)

    #     try:
    #         await asyncio.wait_for(event.wait(), timeout)
    #     except exceptions.TimeoutError:
    #         # print(f'{datetime.now()}: failed to recieve ACK from {node.unique_name}')
    #         self.total_ack_missed += 1
    #         if not self.waiting_for_introduction:
    #             if node in self.missed_acks_count:
    #                 self.missed_acks_count[node] += 1
    #             else:
    #                 self.missed_acks_count[node] = 1
                
    #             logging.error(f'failed to recieve ACK from {node.unique_name} for {self.missed_acks_count[node]} times')
    #             if self.missed_acks_count[node] > 3:
    #                 self.membership_list.update_node_status(node=node, status=0)
    #         else:
    #             logging.debug(f'failed to recieve ACK from Introducer')
    #     except Exception as e:
    #         self.total_ack_missed += 1
    #         # print(f'{datetime.now()}: Exception when waiting for ACK from {node.unique_name}: {e}')
    #         if not self.waiting_for_introduction:
    #             if node in self.missed_acks_count:
    #                 self.missed_acks_count[node] += 1
    #             else:
    #                 self.missed_acks_count[node] = 1

    #             logging.error(f'Exception when waiting for ACK from {node.unique_name} for {self.missed_acks_count[node]} times: {e}')
    #             if self.missed_acks_count[node] > 3:
    #                 self.membership_list.update_node_status(node=node, status=0)
    #         else:
    #             logging.debug(
    #                 f'Exception when waiting for ACK from introducer: {e}')

    #     return event.is_set()

    # async def introduce(self) -> None:
    #     """FUnction to ask introducer to introduce"""
    #     logging.debug(
    #         f'sending pings to introducer: {self.config.introducerNode.unique_name}')
    #     await self.io.send(self.config.introducerNode.host, self.config.introducerNode.port, Packet(self.config.node.unique_name, PacketType.INTRODUCE, self.membership_list.get()).pack())
    #     await self._wait(self.config.introducerNode, PING_TIMEOOUT)

    # async def check(self, node: Node) -> None:
    #     """Fucntion to send PING to a node"""
    #     logging.debug(f'pinging: {node.unique_name}')
    #     await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.PING, self.membership_list.get()).pack())
    #     await self._wait(node, PING_TIMEOOUT)

    # async def run_failure_detection(self) -> NoReturn:
    #     """Function to sends pings to subset of nodes in the RING"""
    #     while True:
    #         if self.is_current_node_active:
    #             if not self.waiting_for_introduction:
    #                 for node in self.membership_list.current_pinging_nodes:
    #                     self.total_pings_send += 1
    #                     asyncio.create_task(self.check(node))
    #             else:
    #                 self.total_pings_send += 1
    #                 asyncio.create_task(self.introduce())

    #         await asyncio.sleep(PING_DURATION)

    # async def check_user_input(self):
    #     """Function to ask for user input and handles"""
    #     loop = asyncio.get_event_loop()
    #     queue = asyncio.Queue()

    #     def response():
    #         loop.create_task(queue.put(sys.stdin.readline()))

    #     loop.add_reader(sys.stdin.fileno(), response)

    #     while True:

    #         print(f'choose one of the following options:')
    #         print('1. list the membership list.')
    #         print('2. list self id.')
    #         print('3. join the group.')
    #         print('4. leave the group.')
    #         if self.config.testing:
    #             print('5. print current bps.')
    #             print('6. current false positive rate.')

    #         option: Optional[str] = None
    #         while True:
    #             option = await queue.get()
    #             if option != '\n':
    #                 break

    #         if option.strip() == '1':
    #             self.membership_list.print()
    #         elif option.strip() == '2':
    #             print(self.config.node.unique_name)
    #         elif option.strip() == '3':
    #             self.is_current_node_active = True
    #             logging.info('started sending ACKs and PINGs')
    #         elif option.strip() == '4':
    #             self.is_current_node_active = False
    #             self.membership_list.memberShipListDict = dict()
    #             self.config.ping_nodes = GLOBAL_RING_TOPOLOGY[self.config.node]
    #             self.waiting_for_introduction = True
    #             self.io.time_of_first_byte = 0
    #             self.io.number_of_bytes_sent = 0
    #             # self.initialize(self.config)
    #             logging.info('stopped sending ACKs and PINGs')
    #         elif option.strip() == '5':
    #             if self.io.time_of_first_byte != 0:
    #                 logging.info(
    #                     f'BPS: {(self.io.number_of_bytes_sent)/(time() - self.io.time_of_first_byte)}')
    #             else:
    #                 logging.info(f'BPS: 0')
    #         elif option.strip() == '6':
    #             if self.membership_list.false_positives > self.membership_list.indirect_failures:
    #                 logging.info(
    #                     f'False positive rate: {(self.membership_list.false_positives - self.membership_list.indirect_failures)/self.total_pings_send}, pings sent: {self.total_pings_send}, indirect failures: {self.membership_list.indirect_failures}, false positives: {self.membership_list.false_positives}')
    #             else:
    #                 logging.info(
    #                     f'False positive rate: {(self.membership_list.indirect_failures - self.membership_list.false_positives)/self.total_pings_send}, pings sent: {self.total_pings_send}, indirect failures: {self.membership_list.indirect_failures}, false positives: {self.membership_list.false_positives}')
    #         else:
    #             print('invalid option.')

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            )
        raise RuntimeError()
