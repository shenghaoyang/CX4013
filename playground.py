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


To run without poetry:
Run the server: 
    ``set SERVER=1``
    ``python3 ./playground.py``
Run the client:
    ``python3 ./playground.py``
"""


import logging
import asyncio
import os
import time
from aioconsole import aprint, ainput
from rpc.helpers import create_server, create_and_connect_client
from rpc.protocol import AddressType
from rpc.skeleton import generate_skeleton, Skeleton
from rpc.proxy import generate_proxy
from rpc.common import remotemethod, RemoteInterface
from rpc.packet import InvocationSemantics
from serialization.derived import String, create_union_type, create_array_type, create_struct_type
from serialization.numeric import i64, u8
#from tinydb import TinyDB, Query

Arrayu8 = create_array_type('u8', u8)

arr = Arrayu8()
arr.append(u8(1))
arr.append(u8(2))

#at client
len(arr)

arr[0].value #to find the integer

# Declare any types that you may want to use.
ErrorOri64 = create_union_type(
    "ErrorOri64", (("i64", i64), ("error", String), ("array", Arrayu8))
)


# Declare remote interface.
# We use an implementation here because it doesn't really matter.
# You might want to use an interface if you want to hide the implementation details.
class Table:
    def __init__(self):
        self.wholeTable = [[() for i in range (48)] for j in range(7)]
        #self.availTable = [[[0 for i in range(48)] for j in range(7)] for k in range(3)]

    def updateTable(self, k, j, i, value):
        self.wholeTable[i][j].append(value)

    def searchTable(self, nameStr, j):
        #for j in range(7)
        searched = list(self.wholeTable[j][:])
        return searched

    def booking(self, nameSlot, daySlot, startSlot, endSlot):
        # check if available
        for i in range(startSlot, endSlot):
            if self.wholeTable[daySlot][i] == 1: #name not settled
                raise Exception("Conflict booking")

        for i in range(startSlot, endSlot):
            self.wholeTable[daySlot][i] = 1 #name not settled

        bookingID = BookingList.setBookingList(nameSlot, daySlot, startSlot, endSlot)

        return bookingID


# class BookingList:
#     def __init__(self):
#         # list: (bookingID, name, startslot, endslot)
#         self.bookingID
#         self.bookingList = []

#     def setBookingList(name, daySlot, startSlot, endSlot):

#         return bookingID

#     def modifyBookingList(bookingID, name, daySlot, newStartSlot, newEndSlot):
#         return True


class Msc:
    #static method wont be expecting the first arg to be a instance
    @staticmethod
    def name2Num(name): 
        try:
            return {
                "Meeting Room": 0,
                "Lecture Room": 1,
                "Tutorial Room": 2
            }[name]
        except KeyError:
            raise ValueError[name]

    @staticmethod  
    def day2Num(day):
        try:
            return {
                "Monday": 0,
                "Tuesday": 1,
                "Wednesday": 2,
                "Thursday": 3,
                "Friday": 4,
                "Saturday": 5,
                "Sunday": 6
            }[day]
        except KeyError:
            raise ValueError[day]
       
    def time2slot(hour, min):
        # 0 0000-0030
        # 1 0030-0100
        # 47 2330-0000

        slot = hour * 2
        if min == 30:
            slot = slot + 1
        return slot

        if slot > 48:
            raise Exception("Not Valid Slot")


class ARemoteObject(RemoteInterface):
    def __init__(self, table:Table):
        self._table = table

    @remotemethod
    async def reverse(self, s: String) -> String:
        return String("".join(reversed(s.value)))

    @remotemethod
    async def time(self) -> String:
        return String(time.ctime())

    @remotemethod
    async def add(self, v1: i64, v2: i64) -> ErrorOri64:
        try:
            return ErrorOri64("i64", i64(v1.value + v2.value))
        except OverflowError as e:
            # Return erorr on overflow.
            return ErrorOri64("error", String(str(e)))

    @remotemethod
    async def long_computation(self, key: u8) -> u8:
        await asyncio.sleep(10)
        return key

    @remotemethod
    async def avail(self, nameStr: String, day: String) -> Arrayu8:
        try:
            #nameNum = Msc.name2Num(name)
            #print("nameNum: ", nameNum)
            dayNum = Msc.day2Num(day)
            print("dayNum: ", dayNum)
            searched = Table.searchTable(nameStr, dayNum)
            return searched
        except ValueError as e:
            print("Error at input", e)

    @remotemethod
    async def book(
        self,
        name: String,
        day: String,
        startHour: i64,
        startMin: i64,
        endHour: i64,
        endMin: i64,
    ) -> i64:
        try:
            if startHour > 23 or endHour > 23:
                raise Exception("Invalid Hour")
            else:
                intStartHour = int(startHour)
                intEndHour = int(endHour)

            if startMin > 59 or endMin > 59:
                raise Exception("Invalid Minute")
            else:
                intStartMin = int(startMin)
                intEndMin = int(endMin)

            if intStartMin > 0 and intStartMin < 30:
                intStartMin = 0
            elif intStartMin > 29 and intStartMin < 60:
                intStartMin = 30
            if intEndMin > 0 and intEndMin < 31:
                intEndMin = 30
            elif (intEndMin > 30 and intEndMin < 60) or intEndMin == 0:
                intEndMin = 0

            startSlot = Msc.time2slot(intStartHour, intStartMin)
            endSlot = Msc.time2slot(intEndHour, intEndMin)

            daySlot = Msc.day2Num(day)
            nameSlot = Msc.name2Num(name)

            confirmID = Table.booking(daySlot, nameSlot, startSlot, endSlot)
            return confirmID
        except ValueError as e:
            print("Error at input", e)

    @remotemethod
    async def reschedule(self, confirmID: i64, offsetHour: i64, offsetMin: i64) -> i64:
        try:

            return 1
        except ValueError as e:
            print("Error at input", e)

    @remotemethod
    async def monitoring(
        self,
        name: String,
        day: String,
        startHour: i64,
        startMin: i64,
        endHour: i64,
        endMin: i64,
    ) -> i64:

        return 1


async def server():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the remote object
    ro = ARemoteObject(Table())
    # Generate the skeleton on the remote class
    skel = generate_skeleton(ARemoteObject)
    # Bind the skeleton to the actual object
    sm_skel = skel(ro)

    schedule = Table()
    #print(schedule)

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

    # Create the server and wait for it to be up.
    s = await create_server(("127.0.0.1", 5000), skel_fac, disconnect_callback)
    # Sleep for one hour and serve the remote object.
    await asyncio.sleep(3600)


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

    async def do_availability():
        # facility include Meeting Rooms, Lecture Theatres, Tutorial Rooms
        await aprint(
            "Select the facility: \n1. Meeting Rooms \n2. Lecture Theatres\n3. Tutorial Rooms\n"
        )
        facility = int(await ainput("Enter Selection: "))
        facility = facility - int(1)
        if facility == 0:
            facilityName = "Meeting Room"
        elif facility == 1:
            facilityName = "Lecture Theatres"
        elif facility == 2:
            facilityName = "Tutorial Rooms"
        await aprint(facility)
        await aprint("How many days?")

        # if 1 day... if many days
        day = "Monday"

        check = await proxy.avail(String(facilityName), String(day))
        print("list of free slots: ", check)

        # calendar = TinyDB('calendar.json')
        # calendar.insert({'name': 'Meeting Room', 'day': 'Monday', '0000-0030': 0, '0030-0100': 0, '0100-0130': 0, '0130-0200': 0})
        # await aprint(calendar.all)
        # setCalendar = Query()
        # calender.search(setCalendar.name == 'Meeting Room')
        # setCalendar = Query()
        # calendar.update({'0030-0100': 1}, setCalendar.name =='Meeting Room')

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
        "Check Availability",
        "Exit",
    )
    handlers = (do_reverse, do_time, do_add, do_long_rpc, do_availability, do_exit)

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
