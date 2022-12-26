import asyncio
from contextlib import AsyncExitStack, suppress
import signal
from typing import List, Optional
from transport import UdpTransport
import getopt
import sys
from worker import Worker
from config import Config
import logging


async def run(config: Config) -> None:
    """Function to initialize whole application and starts events loop"""
    loop = asyncio.get_running_loop()
    async with AsyncExitStack() as stack:
        stack.enter_context(suppress(asyncio.CancelledError))
        tranport = UdpTransport(config.node.host, config.node.port)
        worker: Worker = await stack.enter_async_context(tranport.enter())
        worker.initialize(config)
        task = asyncio.create_task(worker.run())
        loop.add_signal_handler(signal.SIGINT, task.cancel)
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        await task


def parse_cmdline_args(arguments: List[str]) -> Config:
    """Parse cmdline options"""
    # hostname: str = '127.0.0.1'
    hostname: str = 'fa22-cs425-6901.cs.illinois.edu'
    port: int = 8888
    conf: Optional[Config] = None
    introducer: str = None
    testing: bool = False

    try:
        opts, args = getopt.getopt(arguments, "h:p", [
            "hostname=", "port=", "help="])

        for opt, arg in opts:
            if opt == '--help':
                print(
                    'introduce.py -h <hostname> -p <port>')
                sys.exit()
            elif opt in ("-h", "--hostname"):
                hostname = arg
            elif opt in ("-p", "--port"):
                port = int(arg)

        conf = Config(hostname, port)

    except getopt.GetoptError:
        logging.error(
            'failure_detector.py -h <hostname> -p <port> -i <introducer_ip:port> -t')
        sys.exit(2)

    return conf


if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s: [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    conf = parse_cmdline_args(sys.argv[1:])

    asyncio.run(run(conf))
