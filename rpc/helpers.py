"""
helpers.py

Convenience functions for creating RPC servers and clients.
"""


import asyncio
import socket
from typing import Callable, cast
from rpc.proxy import Proxy
from rpc.skeleton import Skeleton
from rpc.protocol import AddressType, RPCClient, RPCServer


async def create_server(
    laddr: AddressType, skel_factory: Callable[[AddressType], Skeleton],
        disconnect_callback: Callable[[AddressType, Skeleton], None],
        family=socket.AF_INET
) -> RPCServer:
    """
    Create a RPC server.

    :param laddr: local address of the server.
    :param skel_factory: skeleton factory function.
    :param disconnect_callback: disconnection callback.
    :param family: server address family.

    :return: newly created RPC server.
    """
    loop = asyncio.get_running_loop()

    def pf():
        return RPCServer(skel_factory, disconnect_callback)

    t, s = await loop.create_datagram_endpoint(
        local_addr=laddr, family=family, protocol_factory=pf
    )
    s = cast(RPCServer, s)

    return s


async def create_and_connect_client(
    raddr: AddressType,
    proxy_factory: Callable[[RPCClient], Proxy],
    family=socket.AF_INET,
) -> tuple[RPCClient, Proxy]:
    """
    Create a new RPC client.

    :param raddr: address of the RPC server.
    :param proxy_factory: proxy factory function.
    :param family: client address family.

    :return: Proxy to RPC object.
    """

    def cf():
        return RPCClient(peer_adr=raddr)

    loop = asyncio.get_running_loop()
    t, c = await loop.create_datagram_endpoint(
        remote_addr=raddr, family=family, protocol_factory=cf
    )

    c = cast(RPCClient, c)
    await c.wait_connected()

    return c, proxy_factory(c)
