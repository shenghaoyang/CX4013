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
from rpc.protocol import AddressType
from serialization.derived import String, create_union_type
from serialization.numeric import i64, u8
from prompt_toolkit import prompt
from typing import Union,TypeVar

async def client():
    # Generate the proxy class for the remote object.
    Proxy = generate_proxy(ARemoteObject)
    # Pass the proxy to the connection function.
    # When this coroutine completes, the client is connected.
    client, proxy = await create_and_connect_client(("127.0.0.1", 5000), Proxy)

    # Set invocation semantics if required, defaults to at least once.
    # The interface for setting these would probably change because they
    # may collide with the names of remote methods.
    proxy.set_semantics(InvocationSemantics.AT_LEAST_ONCE)

    await aprint("client: connected")
    
    T = TypeVar('T')

    async def inputdate(date: T)-> Union[str, None]:
        if date:
         string = await ainput(prompt("Enter Date:"))
         date = await proxy.date_format(String(string))
         await aprint("Date Input:", date.value)
         return date.value
         await facility()
         

    async def facility(facilityinput: T)-> Union[int, None]:
         if facilityinput:
           input = await int(ainput(prompt("Select facility : 1 - meeting rooms , 2 - lecture theatres 3- study room :")))
           facilityinput = await proxy.int(int(int))
           await aprint("Choosen Facilty\n".join(f"{i}: {facilityinput.value}" for i, facilityinput.value in 3))
           return facilityinput.value

    async def facility1(facilityinput1:T)-> Union[int, None]:
        if facilityinput1: 
           input = await int(ainput(prompt("Select facility : 1 - meeting rooms , 2 - lecture theatres 3- study room :")))
           facilityinput1 = await proxy.int(int(int))
           await aprint("Choosen Facilty\n".join(f"{i}: {facilityinput1.value}" for i, facilityinput1.value in 3))
           return facilityinput1.value
           await time
         

           
    
    async def time(time:T)-> Union[str, None]:
       if time:    
        string = await ainput(prmopt("Enter time to book:"))
        time = await proxy.time(String(string))
        await aprint("Entered Time:", time.value)
        return time.value

    async def Bookingid(bookingidinput:T)-> Union[str, None]:
        if bookingidinput:
         input = await int(ainput(prompt("Enter Your Booking ID")))
         bookingidinput = await proxy.int(int(int))
         await aprint("BookingId Entered:", bookingidinput.value)
         return bookingidinput.value
         await time()

    #async def Idempotent():
    
    #async def non-Idempotent():


        
    async def do_exit():
        client.close()
        exit(1)

    labels = (
        "1: Query the availability of a facility (Select days and facility)",
        "2: Booking a facility (for a period of time)",
        "3: Manage Booking (change booking)",
        "4: Check Availability (monitor using callback)"
        "5: Idempotent example"
        "6: non-Idempotent example"
        "Exit",
    )
    handlers = (inputdate,facility1,time,Bookingid,Idempotent,non-Idempotent,do_exit)

    

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

        

"""

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
        await proxy.long_computation(u8())

    async def do_exit():
        client.close()
        exit(1)

    labels = (
        "Reverse a string",
        "Get current time",
        "Add two numbers",
        "Perform long RPC (10s)",
        "Exit",
    )
    handlers = (do_reverse, do_time, do_add, do_long_rpc, do_exit)

    

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
            """