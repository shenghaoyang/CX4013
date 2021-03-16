import asyncio
import Server.table as table
import Server.misc as misc

from serialization.derived import String, create_union_type, create_array_type, create_struct_type
from serialization.numeric import i64, u8
Arrayu8 = create_array_type('u8', u8)

def __init__(self, table:table.Table):
    self.thisTable = table()
    

async def server():
    print("hello")
    wholeTable = table.Table()
    #print(wholeTable.availTable)

async def avail(nameStr: String, day: String, thisTable:table.Table) -> Arrayu8:
    try:
        #nameNum = Misc.name2Num(nameStr)
        dayNum = misc.Misc.day2Num(day)
        print(dayNum)
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
        return confirmID
    except ValueError as e:
        print("Error at input", e)

#async def deleteBooking

async def changeBooking(bookingID: u8, offset: i64, thisTable: table.Table) -> i64:
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
    
    if offset < 0:
        for i in range(newStartSlot,startSlot):
            thisTable.updateTable(dayNum,i,nameStr)
        for i in range(newEndSlot,endSlot):
            thisTable.updateTableRemove(dayNum,i,nameStr)
    else:
        if endSlot < newStartSlot:
            endSlot = newStartSlot
        for i in range(startSlot, newStartSlot):
            thisTable.updateTableRemove(dayNum,i,nameStr)
        for i in range(endSlot,newEndSlot):
            thisTable.updateTable(dayNum,i,nameStr)
    searched = thisTable.searchTable(nameStr, dayNum)
    print(searched)
    return 1

async def main():
    await server()
    thisTable = table.Table()
    #freeTable = await avail("Meeting Room", "Monday", thisTable)
    #print("Free Table:", freeTable)
    book = await booking("Reading Room", "Monday", 1,30,19,00,thisTable)
    book = await booking("Meeting Room", "Tuesday", 9,30,10,30,thisTable)
    book = await booking("Tutorial Room", "Tuesday", 9,30,10,30,thisTable)
    #book = await booking("Study Room", "Monday", 0,00,0,30,thisTable)
    print("Booking ID:", book)
    #freeTable = await avail("Meeting Room", "Monday", thisTable)
    #print("Free Table:", freeTable)
    changed = await changeBooking(111921, 8, thisTable) #assume advance is negative and postponse is positive
    print("Booking Change: ", changed)


asyncio.run(main())
