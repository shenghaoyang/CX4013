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


import asyncio
import os
import time
from aioconsole import aprint, ainput
from rpc.helpers import create_server, create_and_connect_client
from rpc.skeleton import generate_skeleton
from rpc.proxy import generate_proxy
from rpc.common import remotemethod, RemoteInterface
from rpc.packet import InvocationSemantics
from serialization.derived import String, create_union_type, Array
from serialization.numeric import i64
from tinydb.tinydb import TinyDB, Query


# Declare any types that you may want to use.
ErrorOri64 = create_union_type('ErrorOri64',
                               (('i64', i64),
                                ('error', String),
                                ('array', Array)))


# Declare remote interface.
# We use an implementation here because it doesn't really matter.
# You might want to use an interface if you want to hide the implementation details.
class Table():
    def __init__(self):
        self.availTable = [[[0 for i in range(48)] for j in range(7)] for k in range(3)]

    def updateTable(self, k, j, i, value):
        self.availTable[i][j][k] = value

    def searchTable(self, k, j):
        searched = list(self.availTable[k][j][:])
        return searched

    def booking(self, nameSlot, daySlot, startSlot, endSlot):
        #check if available
        for i in range(startSlot, endSlot):
            if self.availTable[nameSlot][daySlot][i] == 1:
                raise Exception("Conflict booking")

        for i in range(startSlot, endSlot):
            self.availTable[nameSlot][daySlot][i] = 1

        bookingID = BookingList.setBookingList(nameSlot, daySlot, startSlot, endSlot)

        return bookingID

class BookingList():
    def __init__(self):
        #list: (bookingID, name, startslot, endslot)
        self.bookingID
        self.bookingList = []    
    
    def setBookingList(name, daySlot, startSlot, endSlot):

        return bookingID

    def modifyBookingList(bookingID, name, daySlot, newStartSlot, newEndSlot):
        return True

class Msc():
    def name2Num(name):
        if name == "Meeting Room":
            return 0
        elif name == "Lecture Room":
            return 1
        elif name == "Tutorial Room":
            return 2
        else:
            raise Exception("Invalid Facility")

    def day2Num(day):
        if day == "Monday":
            return 0
        elif day == "Tuesday":
            return 1
        elif day == "Wednesday":
            return 2
        elif day == "Thursday":
            return 3
        elif day == "Friday":
            return 4
        elif day == "Saturday":
            return 5
        elif day == "Sunday":
            return 6
        else:
           raise Exception("Invalid day")

    def time2slot(hour, min):
        # 0 0000-0030
        # 1 0030-0100
        # 47 2330-0000
        
        slot = hour * 2
        if(min == 30):
            slot = slot + 1
        return slot

        if slot > 48:
            raise Exception("Not Valid Slot")


        

class ARemoteObject(RemoteInterface):
    @remotemethod
    def reverse(self, s: String) -> String:
        return String("".join(reversed(s.value)))

    @remotemethod
    def time(self) -> String:
        return String(time.ctime())

    @remotemethod
    def add(self, v1: i64, v2: i64) -> ErrorOri64:
        try:
            return ErrorOri64('i64', i64(v1.value + v2.value))
        except OverflowError as e:
            # Return erorr on overflow.
            return ErrorOri64('error', String(str(e)))

    @remotemethod
    def avail(self, name: String, day: String) -> Array:
        try:
            nameNum = Msc.name2Num(name)
            dayNum = Msc.day2Num(day)
            searched = Table.searchTable(nameNum, dayNum)
            return searched
        except ValueError as e:
            print("Error at input", e)

    @remotemethod
    def book(self, name: String, day: String, startHour: i64, startMin: i64, endHour: i64, endMin: i64) -> i64:
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
            elif ((intEndMin > 30 and intEndMin < 60) or intEndMin == 0):
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
    def reschedule(self, confirmID: i64, offsetHour: i64, offsetMin:i64) -> i64:
        try:
            
            return 1
        except ValueError as e:
            print("Error at input", e)

    @remotemethod
    def monitoring(self, name: String, day: String, startHour: i64, startMin: i64, endHour: i64, endMin: i64) -> i64:
        
        return 1



async def server():
    # Create the remote object
    ro = ARemoteObject()
    # Generate the skeleton on the remote class
    skel = generate_skeleton(ARemoteObject)
    # Bind the skeleton to the actual object
    sm_skel = skel(ro)

    schedule = Table()
    
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

    async def do_availability():
        #facility include Meeting Rooms, Lecture Theatres, Tutorial Rooms
        await aprint("Select the facility: \n1. Meeting Rooms \n2. Lecture Theatres\n3. Tutorial Rooms\n")
        facility = int(await ainput("Enter Selection: "))
        facility = facility-int(1)
        if facility == 0:
            facilityName = "Meeting Room"
        elif facility == 1:
            facilityName = "Lecture Theatres"
        elif facility == 2:
            facilityName = "Tutorial Rooms"
        await aprint(facility)
        await aprint("How many days?")
        
        #if 1 day... if many days  
        day = "Monday"

        check = await proxy.avail(String(facilityName), String(day))
        print("list of free slots: ",check)

        
        #calendar = TinyDB('calendar.json')
        #calendar.insert({'name': 'Meeting Room', 'day': 'Monday', '0000-0030': 0, '0030-0100': 0, '0100-0130': 0, '0130-0200': 0})
        #await aprint(calendar.all)
        #setCalendar = Query()
        #calender.search(setCalendar.name == 'Meeting Room')
        #setCalendar = Query()
        #calendar.update({'0030-0100': 1}, setCalendar.name =='Meeting Room')
        
        
        

    handlers = [do_reverse, do_time, do_add, do_availability]

    # Call functions on the remote.
    # Use aprint and await for printing data.
    while True:
        await aprint("Select an option:\n0: Reverse a string\n1: Get current time\n2: Add two numbers\n 3: Check Availability")
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
