import asyncio
import Server.table as table
import Server.misc as misc

from serialization.derived import String, create_union_type, create_array_type, create_struct_type
from serialization.numeric import i64, u8
Arrayu8 = create_array_type('u8', u8)

def __init__(self, table:table.Table):
    self.thisTable = table()
    
# Declare callback receiver 
# class CallbackReceiver():
#     async def on_booking(nameStr : String) -> Void:
#         await aprint(f"callback: some client made a booking on ", nameStr)
#         return Void()
#     async def on_changeBooking(nameStr: String )-> Void:
#         await print(f"")

async def server():
    print("hello")
    wholeTable = table.Table()
    #print(wholeTable.availTable)

async def avail(nameStr: String, day: String, thisTable:table.Table) -> Arrayu8:
    try:
        #nameNum = Misc.name2Num(nameStr)
        dayNum = misc.Misc.day2Num(day)
        #print(dayNum)
        #thisTable.getTable()
        #thisTable.updateTable(dayNum, 4, nameStr)
        #strTime = misc.Misc.slot2time(31)
        #print(strTime)
        #thisTable.updateTable(dayNum, 4, "Tutorial Room")
        #print(table.Table.getTable())
        searched = thisTable.searchTable(nameStr, dayNum)
        #client can do this to print out the available slot
        # for i in range(len(searched)):
        #     if searched[i] == ():
        #         strTime = misc.Misc.slot2time(i)
        #         print(strTime)
        return searched
    except ValueError as e:
        print("Error at input", e)

async def booking(nameStr: String, day: String, startHour: u8, startMin: u8, endHour: u8, endMin: u8, thisTable: table.Table) -> u8:
    try:
        startSlot = misc.Misc.time2slot(startHour,startMin)
        endSlot = misc.Misc.time2slot(endHour,endMin)
        dayNum = misc.Misc.day2Num(day)
        
        for i in range(startSlot,endSlot):
            thisTable.updateTable(dayNum,i,nameStr)

        #searched = thisTable.searchTable(nameStr, dayNum)
        nameNum = misc.Misc.name2Num(str(nameStr))
        if startSlot < 10:
            startStr = "0"+ str(startSlot)
        else:
            startStr = str(startSlot)
        if endSlot < 10:
            endStr = "0" +str(endSlot)
        else:
            endStr = str(endSlot)
        confirmID = int(str(nameNum)+str(dayNum)+startStr+endStr)
        thisTable.bookingList.append(confirmID)
        await callback(nameStr, thisTable)
        return confirmID
    except ValueError as e:
        print("Error at input", e)

#async def deleteBooking

async def changeBooking(bookingID: u8, offset: i64, thisTable: table.Table) -> i64:
    x=0
    for i in range(len(thisTable.bookingList)):
        if thisTable.bookingList[i] == bookingID:
            x=1 
    if x == 0:
        raise Exception("Not valid booking ID")

    oriBookingID = bookingID
    endSlot = bookingID % 100
    bookingID = (bookingID - endSlot)/100
    startSlot = int(bookingID % 100)
    bookingID = (bookingID - startSlot)/100
    dayNum = int(bookingID % 10)
    bookingID = (bookingID - dayNum)/10
    nameNum = int(bookingID)
    nameStr = misc.Misc.num2Name(nameNum)
    #print(endSlot, startSlot, dayNum, nameNum)
    #Assume offset is already negative or positive
    newStartSlot = startSlot + offset
    newEndSlot = endSlot + offset
    if (newStartSlot < 0) or (newEndSlot >48):
        raise Exception("Invalid range of booking, must be between 0000H to 2350H")
    if offset < 0:
        for i in range(newStartSlot,startSlot):
            thisTable.updateTable(dayNum,i,nameStr)
        for i in range(newEndSlot,endSlot):
            thisTable.updateTableRemove(dayNum,i,nameStr)
    else:
        if endSlot < newStartSlot:
            endSlot = newStartSlot
        for i in range(startSlot, newStartSlot):
            print(dayNum,i,nameStr)
            thisTable.updateTableRemove(dayNum,i,nameStr)
        for i in range(endSlot,newEndSlot):
            thisTable.updateTable(dayNum,i,nameStr)
    #searched = thisTable.searchTable(nameStr, dayNum)
    #print(searched)
    if newStartSlot < 10:
        startStr = "0"+ str(newStartSlot)
    else:
        startStr = str(newStartSlot)
    if newEndSlot < 10:
        endStr = "0" +str(newEndSlot)
    else:
        endStr = str(newEndSlot)

    confirmID = int(str(nameNum)+str(dayNum)+startStr+endStr)
    for i in range(len(thisTable.bookingList)):
        if thisTable.bookingList[i] == oriBookingID:
            thisTable.bookingList[i] = confirmID 

    await callback(nameStr, thisTable)
    return confirmID

async def registerCallback(nameStr: String, monitorInterval: u8, ipadd: String, port: u8, thisTable: table.Table) -> i64:
    entry = (str(nameStr), monitorInterval, str(ipadd), port)
    #print(entry)
    thisTable.callbackList.append(tuple(entry))
    return 1

async def callback(nameStr, thisTable: table.Table):
    if len(thisTable.callbackList) > 0:
        for i in thisTable.callbackList:
            #print(i[0])
            if nameStr == i[0]:
                 print("Callback to", i[2], i[3], "regarding", nameStr)
    

async def main():
    await server()
    thisTable = table.Table()
    #bookingList = []
    freeTable = await avail("Meeting Room", "Tuesday", thisTable)
    print("Free Table:", freeTable)
    book = await booking("Reading Room", "Monday", 1,30,19,00,thisTable)
    print("Booking ID:", book)
    book = await booking("Meeting Room", "Tuesday", 9,30,10,30,thisTable)
    print("Booking ID:", book)
    book = await booking("Tutorial Room", "Tuesday", 9,30,10,30,thisTable)
    #book = await booking("Meeting Room", "Tuesday", 23,00,23,30,thisTable)
    #book = await booking("Study Room", "Monday", 0,00,0,30,thisTable)
    print("Booking ID:", book)
    #freeTable = await avail("Meeting Room", "Monday", thisTable)
    #print("Free Table:", freeTable)
    #changed = await changeBooking(111921, 26, thisTable) #assume advance is negative and postponse is positive
    #print("New booking ID: ", changed)
    #print(thisTable.bookingList)
    await registerCallback("Meeting Room", 10, "127.0.0.1", "5000", thisTable)
    await registerCallback("Reading Room", 15, "127.0.0.1", "5003", thisTable)
    print(thisTable.callbackList)
    book = await booking("Meeting Room", "Tuesday", 8,30,9,30,thisTable)
    print("Booking ID:", book)
    changed = await changeBooking(311921, -10, thisTable)
    print("New Booking ID: ", changed)
    freeTable = await avail("Meeting Room", "Tuesday", thisTable)
    print("Free Table:", freeTable)



asyncio.run(main())
