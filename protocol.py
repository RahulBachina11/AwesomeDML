import asyncio
from asyncio import DatagramProtocol, DatagramTransport, BaseTransport, Condition
from datetime import datetime
import logging
from time import time
from random import shuffle
from typing import Optional, cast, Tuple
from collections import deque

PACKET_DROP_PERCENTAGE = 3


class AwesomeProtocol(DatagramProtocol):
    """
    Implements class asyncio.DatagramProtocol to send and receive UDP packets
    """

    def __init__(self) -> None:
        super().__init__()
        self._queue_lock = Condition()
        self._queue = deque()
        self._transport = None
        self.number_of_bytes_sent = 0
        self.time_of_first_byte = 0
        self.random_flags = [0 for _ in range(
            PACKET_DROP_PERCENTAGE)] + [1 for _ in range(100 - PACKET_DROP_PERCENTAGE)]
        shuffle(self.random_flags)
        self.current_msg_sent_count = 0
        self.testing = False

    @property
    def transport(self) -> DatagramTransport:
        """The current asyncio.DatagramTransport object"""
        transport = self._transport
        assert transport is not None
        return transport

    def connection_made(self, transport: BaseTransport) -> None:
        """Called when the UDP socket is initialized."""
        self._transport = cast(DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Called when data is received on the UDP socket"""
        # parse data into a known structure
        # print(f"{datetime.now()}: received data: from {addr[0]}:{addr[1]}")
        asyncio.create_task(self._push((data, addr[0], addr[1])))

    def error_received(self, exc: Exception) -> None:
        """Called when a UDP send or receive operation fails"""
        logging.error('UDP operation failed')

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when the UDP socket is closed"""
        logging.error(f'UDP connection lost: {exc}')
        self._transport = None

    async def _push(self, data) -> None:
        """Push UDP packet data to queue"""
        async with self._queue_lock:
            self._queue.append(data)
            self._queue_lock.notify()

    async def recv(self):
        """Receive current available UDP data from queue"""
        async with self._queue_lock:
            await self._queue_lock.wait()
            return self._queue.popleft()

    async def send(self, host, port, data) -> None:
        """Sends current bytes to UDP socket"""
        if self.testing:
            if self.number_of_bytes_sent == 0:
                self.time_of_first_byte = time()
            self.number_of_bytes_sent += len(data)

            if self.random_flags[self.current_msg_sent_count % len(self.random_flags)]:
                self.transport.sendto(data, (host, port))

            self.current_msg_sent_count += 1
        else:
            self.transport.sendto(data, (host, port))
