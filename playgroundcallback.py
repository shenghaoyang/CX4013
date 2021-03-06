#!/usr/bin/env python3


"""
Playground testing program.

The interface shouldn't change even though some parts of the RPC servers & clients don't
work too well right now (there will be resource leaks - working on that).

You may need to install poetry and use that to perform an initial install of all
dependencies.

Using ``poetry shell`` should drop you into a shell that has the correct environment
setup with installed dependdencies.

Python version used for testing was 3.9.2.

Run the server:

    ``SERVER=1 python3 ./playground.py``

Run the client:

    ``python3 ./playground.py``
"""


import logging
import asyncio
import os
import time
from aioconsole import aprint, ainput
from rpc.helpers import create_server, create_and_connect_client
from rpc.skeleton import generate_skeleton, Skeleton
from rpc.proxy import generate_proxy
from rpc.common import remotemethod, RemoteInterface
from rpc.packet import InvocationSemantics
from rpc.proxy import Proxy
from rpc.protocol import AddressType, RPCClient
from serialization.derived import String, create_union_type, create_struct_type
from serialization.numeric import i64, u8, u32


# Declare any types that you may want to use.
ErrorOri64 = create_union_type("ErrorOri64", (("i64", i64), ("error", String)))
Host = create_struct_type("Host", (("ipv4_address", String), ("port", u32)))
Void = create_struct_type("Void", ())


# Declare callback receiver remote interface.
class CallbackReceiver(RemoteInterface):
    @remotemethod
    async def on_add(self, v1: i64, v2: i64) -> Void:
        await aprint(f"callback: some client added {v1.value} + {v2.value}")
        return Void()


CRProxy = generate_proxy(CallbackReceiver)


# Declare remote interface.
# We use an implementation here because it doesn't really matter.
# You might want to use an interface if you want to hide the implementation details.
class ARemoteObject(RemoteInterface):
    def __init__(self):
        self._callback_targets: dict[AddressType, tuple[RPCClient, Proxy]] = {}

    @remotemethod
    async def reverse(self, s: String) -> String:
        return String("".join(reversed(s.value)))

    @remotemethod
    async def time(self) -> String:
        return String(time.ctime())

    @remotemethod
    async def add(self, v1: i64, v2: i64) -> ErrorOri64:
        # todo timeout this call
        for _, p in self._callback_targets.values():
            await p.on_add(v1, v2)

        try:
            return ErrorOri64("i64", i64(v1.value + v2.value))
        except OverflowError as e:
            # Return error on overflow.
            return ErrorOri64("error", String(str(e)))

    @remotemethod
    async def long_computation(self) -> Void:
        await asyncio.sleep(10)
        return Void()

    @remotemethod
    async def register_add_callback(self, host: Host, timeout: u32) -> ErrorOri64:
        if host["ipv4_address"].value in self._callback_targets:
            return ErrorOri64("i64", i64(0))

        addr = host["ipv4_address"].value, host["port"].value
        try:
            c, p = await asyncio.wait_for(
                create_and_connect_client(
                    (host["ipv4_address"].value, host["port"].value), CRProxy
                ),
                timeout.value,
            )
            self._callback_targets[addr] = c, p
            return ErrorOri64("i64", i64(0))
        except asyncio.CancelledError:
            return ErrorOri64("error", String("timeout"))

    def remove_cb_target(self, addr: AddressType):
        t = self._callback_targets.pop(addr, None)
        if t is None:
            return

        t[0].close()


async def server():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the remote object
    ro = ARemoteObject()
    # Generate the skeleton on the remote class
    skel = generate_skeleton(ARemoteObject)
    # Bind the skeleton to the actual object
    sm_skel = skel(ro)

    # Factory function that returns a skeleton to use for every new connection.
    # todo: could accept address of remote client
    # We return the same skeleton object everytime, so that every client operates
    # on the same object, but having a factory allows for new objects for every
    # client, etc.
    def skel_fac(addr: AddressType) -> Skeleton:
        logger.info(f"new connection from {addr}")
        return sm_skel

    # Function that accepts a skeleton and an address on disconnection.
    def disconnect_callback(addr: AddressType, skel: Skeleton):
        logger.info(f"client {addr} disconnected")
        ro.remove_cb_target(addr)

    # Create the server and wait for it to be up.
    s = await create_server(("127.0.0.1", 5000), skel_fac, disconnect_callback)
    # Sleep for one hour and serve the remote object.
    await asyncio.sleep(3600)


async def client():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the callback object
    cr = CallbackReceiver()
    # Generate the skeleton on the remote class
    skel = generate_skeleton(CallbackReceiver)
    # Bind the skeleton to the actual object
    cr_skel = skel(cr)

    # Factory function that returns a skeleton to use for every new connection.
    # todo: could accept address of remote client
    # We return the same skeleton object everytime, so that every client operates
    # on the same object, but having a factory allows for new objects for every
    # client, etc.
    def skel_fac(addr: AddressType) -> Skeleton:
        logger.info(f"CallbackReceiver: new connection from {addr}")
        return cr_skel

    # Function that accepts a skeleton and an address on disconnection.
    def disconnect_callback(addr: AddressType, skel: Skeleton):
        logger.info(f"CallbackReceiver: client {addr} disconnected")

    # Setup the callback server
    s = await create_server(("127.0.0.1", 5001), skel_fac, disconnect_callback)

    # Generate the proxy class for the remote object.
    ROProxy = generate_proxy(ARemoteObject)
    # Pass the proxy to the connection function.
    # When this coroutine completes, the client is connected.
    client, proxy = await create_and_connect_client(("127.0.0.1", 5000), ROProxy)

    # Set invocation semantics if required, defaults to at least once.
    # The interface for setting these would probably change because they
    # may collide with the names of remote methods.
    proxy.set_semantics(InvocationSemantics.AT_LEAST_ONCE)

    await aprint("client: connected")

    async def do_reverse():
        string = await ainput("Enter string to reverse:")
        rev = await proxy.reverse(String(string))
        await aprint("Reversed string:", rev.value)

    async def do_time():
        ts = await proxy.time()
        await aprint("Current time:", ts.value)

    async def do_add():
        try:
            inputs = [int(await ainput(f"Enter number {i}: ")) for i in range(2)]
            operands = tuple(map(i64, inputs))
            res = await proxy.add(*operands)
            if "error" in res:
                await aprint("Addition failed:", res["error"].value)
                return

            await aprint(" + ".join(map(str, inputs)), "=", res["i64"].value)

        except (ValueError, OverflowError):
            await aprint("Inputs are not numeric / out of range")

    async def do_long_rpc():
        await proxy.long_computation()

    async def do_register_callback():
        host = Host()
        host["ipv4_address"] = String("127.0.0.1")
        host["port"] = u32(5001)
        await proxy.register_add_callback(host, u32(5))

    async def do_exit():
        client.close()
        exit(1)

    labels = (
        "Reverse a string",
        "Get current time",
        "Add two numbers",
        "Perform long RPC (10s)",
        "Register callback on addition",
        "Exit",
    )
    handlers = (do_reverse, do_time, do_add, do_long_rpc, do_register_callback, do_exit)

    # Call functions on the remote.
    # Use aprint and await for printing data.
    while True:
        await aprint("\n".join(f"{i}: {s}" for i, s in enumerate(labels)))
        try:
            selection = int(await ainput(">>> "))
            await handlers[selection]()
        except ValueError:
            await aprint("Input is not a number")
        except IndexError:
            await aprint("Selection out of range")


def main():
    routine = (client, server)[int("SERVER" in os.environ)]
    asyncio.run(routine(), debug=True)


if __name__ == "__main__":
    main()
