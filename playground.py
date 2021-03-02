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


import asyncio
import os
import time
from aioconsole import aprint, ainput
from rpc.helpers import create_server, create_and_connect_client
from rpc.skeleton import generate_skeleton
from rpc.proxy import generate_proxy
from rpc.common import remotemethod, RemoteInterface
from rpc.packet import InvocationSemantics
from serialization.derived import String, create_union_type
from serialization.numeric import i64, u8


# Declare any types that you may want to use.
ErrorOri64 = create_union_type('ErrorOri64',
                               (('i64', i64),
                                ('error', String)))


# Declare remote interface.
# We use an implementation here because it doesn't really matter.
# You might want to use an interface if you want to hide the implementation details.
class ARemoteObject(RemoteInterface):
    @remotemethod
    async def reverse(self, s: String) -> String:
        return String("".join(reversed(s.value)))

    @remotemethod
    async def time(self) -> String:
        return String(time.ctime())

    @remotemethod
    async def add(self, v1: i64, v2: i64) -> ErrorOri64:
        try:
            return ErrorOri64('i64', i64(v1.value + v2.value))
        except OverflowError as e:
            # Return erorr on overflow.
            return ErrorOri64('error', String(str(e)))

    @remotemethod
    async def long_computation(self, key: u8) -> u8:
        await asyncio.sleep(10)
        return key


async def server():
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
    def skel_fac():
        return sm_skel

    # Create the server and wait for it to be up.
    s = await create_server(("127.0.0.1", 5000), skel_fac)
    # Sleep for one hour and serve the remote object.
    await asyncio.sleep(3600)


async def client():
    # Generate the proxy class for the remote object.
    Proxy = generate_proxy(ARemoteObject)
    # Pass the proxy to the connection function.
    # When this coroutine completes, the client is connected.
    proxy = await create_and_connect_client(("127.0.0.1", 5000), Proxy)

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
            if 'error' in res:
                await aprint("Addition failed:", res['error'].value)
                return

            await aprint(' + '.join(map(str, inputs)), '=', res['i64'].value)

        except (ValueError, OverflowError):
            await aprint("Inputs are not numeric / out of range")

    handlers = [do_reverse, do_time, do_add]

    # Call functions on the remote.
    # Use aprint and await for printing data.
    while True:
        await aprint("Select an option:\n0: Reverse a string\n1: Get current time\n2: Add two numbers")
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
