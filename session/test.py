import logging
import asyncio
import socket
from session.protocol import Session
from typing import cast


def main():
    import os
    selector = int('SERVER' in os.environ)
    logging.basicConfig()
    rl = logging.getLogger()
    rl.setLevel(logging.DEBUG)
    asyncio.run((client, server)[selector]())


async def server():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        Session, local_addr=("127.0.0.1", 5000), family=socket.AF_INET)
    await asyncio.sleep(3600)


async def client():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        Session, remote_addr=("127.0.0.1", 5000), family=socket.AF_INET
    )

    protocol = cast(Session, protocol)
    endpoint = protocol.get_or_create_endpoint(("127.0.0.1", 5000))
    await endpoint.open_session()
    channel0 = await endpoint.open_channel("info.shenghaoyang.hello1")
    channel1 = await endpoint.open_channel("info.shenghaoyang.hello2")
    for i in range(10):
        channel1.send(f"hello {i} from {channel0.number}".encode("utf-8"))
        channel0.send(f"hello {i} from {channel1.number}".encode("utf-8"))
        await asyncio.sleep(1)

    await endpoint.close_session()

if __name__ == '__main__':
    main()
