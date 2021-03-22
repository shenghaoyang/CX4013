"""
helpers.py

Convenience functions for creating RPC servers and clients.
"""


import asyncio
import socket
from typing import Callable, cast, Optional
from rpc.proxy import Proxy
from rpc.skeleton import Skeleton
from rpc.protocol import AddressType, RPCClient, RPCServer


async def create_server(
    laddr: AddressType,
    skel_factory: Callable[[AddressType], Skeleton],
    disconnect_callback: Callable[[AddressType, Skeleton], None],
    inactivity_timeout: int = 300,
    result_cache_timeout: int = 60,
    family=socket.AF_INET,
) -> RPCServer:
    """
    Create a RPC server.

    :param laddr: local address of the server.
    :param skel_factory: skeleton factory function.
    :param disconnect_callback: disconnection callback.
    :param inactivity_timeout: amount of time (in seconds) without receiving a request
        before a client is disconnected.
    :param result_cache_timeout: amount of time (in seconds) to cache the result of an
        RPC call made using at-most-once semantics.
    :param family: server address family.

    :return: newly created RPC server.
    """
    loop = asyncio.get_running_loop()

    def pf():
        return RPCServer(
            skel_factory, disconnect_callback, inactivity_timeout, result_cache_timeout
        )

    t, s = await loop.create_datagram_endpoint(
        local_addr=laddr,
        family=family,
        protocol_factory=pf,
    )
    s = cast(RPCServer, s)

    return s


async def create_and_connect_client(
    raddr: AddressType,
    proxy_factory: Callable[[RPCClient], Proxy],
    inactivity_timeout: int = 300,
    pre_receive: Callable[[bytes], Optional[bytes]] = None,
    pre_send: Callable[[bytes], Optional[bytes]] = None,
    family=socket.AF_INET,
) -> tuple[RPCClient, Proxy]:
    """
    Create a new RPC client.

    :param raddr: address of the RPC server.
    :param proxy_factory: proxy factory function.
    :param inactivity_timeout: amount of time (in seconds) without receiving a server reply
        before the client disconnects.
    :param pre_receive: pre-receive hook used to drop / alter received packets.
    :param pre_send: pre-send hook used to drop / alter sent packets.
    :param family: client address family.

    :return: Proxy to RPC object.
    """

    def cf():
        return RPCClient(
            peer_adr=raddr,
            inactivity_timeout=inactivity_timeout,
            pre_receive=pre_receive,
            pre_send=pre_send,
        )

    loop = asyncio.get_running_loop()
    _, c = await loop.create_datagram_endpoint(
        remote_addr=raddr, family=family, protocol_factory=cf
    )

    c = cast(RPCClient, c)
    try:
        await c.wait_connected()
    except asyncio.CancelledError:
        c.close()

    return c, proxy_factory(c)
